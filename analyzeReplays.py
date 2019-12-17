#!/usr/bin/python3.7

# Script Analyze WoT Blitz replays

import sys, argparse, json, os, concurrent, inspect, aiohttp, asyncio, aiofiles, aioconsole, re, logging, time, xmltodict, collections
import motor.motor_asyncio, configparser, ssl
from datetime import datetime, timedelta, date
import blitzutils as bu
from blitzutils import WG
from blitzutils import WoTinspector

logging.getLogger("asyncio").setLevel(logging.DEBUG)

WG_appID  		= '81381d3f45fa4aa75b78a7198eb216ad'
FILE_CONFIG 	= 'blitzstats.ini'
DB_C_TANK_STATS = 'WG_TankStats'

wg = None
wi = None
REPLAY_N = 0
REPLAY_I = 0
STAT_TANK_BATTLE_MIN = 100

replay_summary_flds = [
	'battle_result',
	'battle_type',
	'map_name',
	'battle_duration',
	'protagonist',
	'title'
]

replay_details_flds = [
	'damage_made',
	'damage_assisted_track',
	'damage_assisted',
	'damage_received',
	'damage_blocked',
	'enemies_damaged',
	'enemies_destroyed',
	'hitpoints_left',
	'time_alive',
	'distance_travelled',
	'base_capture_points',
	'base_defend_points',
	'wp_points_earned',
	'wp_points_stolen',
	'hits_received',
	'hits_bounced',
	'hits_splash',
	'hits_pen',
	'shots_made',
	'shots_hit',
	'shots_pen',
	'shots_splash',
	'enemies_spotted'
	]

result_ratios = {
	'KDR'				: [ 'enemies_destroyed', 'destroyed' ],
	'DR'				: [ 'damage_made', 'damage_received' ],
	'hit_rate'			: [ 'shots_hit', 'shots_made' ],
	'pen_rate'			: [ 'shots_pen', 'shots_hit' ],
	'dmg_block_rate' 	: [ 'damage_blocked', 'damage_received' ]
}

result_categories = {
	'total'				: [ 'TOTAL', 'total' ],
	'battle_result'		: [ 'Result', [ 'Loss', 'Win', 'Draw']],
	'battle_type'		: [ 'Battle Type',['Encounter', 'Supremacy']],
	'tank_tier'			: [ 'Tank Tier', 'number' ],
	'top_tier'			: [ 'Tier', ['Bottom tier', 'Top tier']],
	'tank_name'			: [ 'Tank', 'string' ],
	'map_name'			: [ 'Map', 'string' ],
	'battle_i'			: [ 'Battle #', 'number']
	}

result_categories_default = [
	'total',
	'battle_result',
	'tank_tier'
]

result_cat_header_frmt = '{:_<17s}'
result_cat_frmt = '{:>17s}'


## Syntax: Check how the replay JSON files look. The algorithm is counting/recording fields
result_fields = {
	'battles'			: [ 'Battles', 'Number of battles', 8, '{:^8.0f}' ],
	'win'				: [ 'WR', 'Win rate', 				6, '{:6.1%}' ],
	'damage_made'		: [ 'DPB', 'Average Damage', 		5, '{:5.0f}' ],
	'DR'				: [ 'DR', 'Damage Ratio', 			5, '{:5.1f}' ],
	'KDR'				: [ 'KDR', 'Kills / Death', 		4, '{:4.1f}' ],
	'enemies_spotted'	: [ 'Spot', 'Enemies spotted per battle', 		4, '{:4.1f}' ],
	'hit_rate'			: [ 'Hit rate', 'Shots hit / all shots made', 	8, '{:8.1%}' ],
	'pen_rate'			: [ 'Pen rate', 'Shots pen / shots hit', 		8, '{:8.1%}' ],
	'survived'			: [ 'Surv%', 'Survival rate', 					6, '{:6.1%}' ],
	'time_alive%'		: [ 'T alive%', 'Percentage of time being alive in a battle', 8, '{:8.0%}' ], 
	'top_tier'			: [ 'Top tier', 'Share of games as top tier', 					8, '{:8.0%}' ],
	'allies_wins'		: [ 'Allies WR', 'Average WR of allies at the tier of their tank', 9, '{:9.2%}' ],
	'enemies_wins'		: [ 'Enemies WR', 'Average WR of enemies at the tier of their tank', 10, '{:10.2%}' ],
	'allies_battles'	: [ 'Allies Btls', 'Average number battles of the allies', 		11, '{:11.0f}' ],
	'enemies_battles'	: [ 'Enemies Btls', 'Average number battles of the enemies', 	12, '{:12.0f}' ]	
}

## Syntax: key == stat field in https://api.wotblitz.eu/wotb/tanks/stats/  (all.KEY)
## Value array [ 'Stat Title', [ 0, data buckets ....], scaling_factor_for_bucket_values, 'print_format' ]
histogram_fields = {
	'wins'				: [ 'Win rate', [0, .35, .40, .45, .5, .55, .60, .65, 1], 100, '{:.0f}%' ],
	'damage_dealt'		: [ 'Avg. Dmg.', [0, .25e3, .5e3, 1e3, 1.5e3, 2e3,2.5e3,3e3,100e3], 1, '{:.0f}' ],
	'battles'			: [ 'Battles', 	[0, 1e3, 3e3, 5e3, 10e3, 15e3, 25e3, 50e3, 5e7], .001, '{:.0f}k']	# battles is a mandatory stat to include
	}


def defaultvalueZero():
    return 0

def defaultvalueBtlRec():
	return BattleRecord()


class PlayerHistogram():
	def __init__(self, field: str, name: str, fields : list, factor: float, format: str ):
		self.field 		= field
		self.name 		= name
		self.fields 	= fields
		self.catFactor 	= factor
		self.catFormat 	= format
		self.ncat 		= len(self.fields) - 1
		self.allies 	= [0] * self.ncat
		self.enemies 	= [0] * self.ncat
		self.total 		= [0] * self.ncat
	
	def recordAlly(self, stat: float):
		for cat in range(0, self.ncat):
			if self.fields[cat] <= stat <= self.fields[cat+1]:
				self.allies[cat] += 1
				self.total[cat]  += 1
				return True
		return False
	
	def recordEnemy(self, stat: float):
		for cat in range(0, self.ncat):
			if self.fields[cat] <= stat <= self.fields[cat+1]:
				self.enemies[cat] += 1
				self.total[cat]  += 1
				return True
		return False

	def getCatname(self, cat: int):
		if cat < self.ncat - 1:
			return self.formatCat(cat) + ' - ' + self.formatCat(cat+1)
		else:
			return self.formatCat(cat) + ' -'

	def formatCat(self, cat: int):
		val = self.fields[cat]
		return self.catFormat.format(float(val * self.catFactor))

	def print(self):
		N_enemies = 0
		N_allies = 0
		for cat in range(0, self.ncat):
			N_enemies += self.enemies[cat]
			N_allies += self.allies[cat]
		N_total = N_allies + N_enemies

		print("\n{:12s} | {:13s} | {:13s} | {:13s}".format(self.name, "Allies", "Enemies", "TOTAL"))
		for cat in range(0, self.ncat):
			print("{:12s} | {:5d} ({:4.1f}%) | {:5d} ({:4.1f}%) | {:5d} ({:4.1f}%)".format(self.getCatname(cat), self.allies[cat], self.allies[cat]/N_allies*100, self.enemies[cat], self.enemies[cat]/N_enemies*100, self.allies[cat] + self.enemies[cat], (self.allies[cat] + self.enemies[cat])/N_total*100 ))
		return None


class BattleRecord():
	def __init__(self):
		try:
			self.battles = 0
			self.ratios_ready = False
			self.results_ready = False
			self.results = collections.defaultdict(defaultvalueZero)
			self.avg_fields = set(result_fields.keys())
			self.avg_fields.remove('battles')
			self.avg_fields.difference_update(result_ratios.keys())
			self.ratio_fields = set()
			for ratio in result_ratios.keys():
				self.ratio_fields.add(result_ratios[ratio][0])
				self.ratio_fields.add(result_ratios[ratio][1])

		except KeyError as err:
			bu.error('Key ' + str(err) + ' not found') 

	def recordResult(self, result : dict) -> bool:
		try:
			for field in self.avg_fields | self.ratio_fields:
				if result[field] != None:
					self.results[field] += result[field]
			self.battles += 1
			return True
		except KeyError as err:
			bu.error('Key ' + str(err) + ' not found') 
		return False

	def calcRatios(self) -> bool:
		if self.ratios_ready:
			return True
		try:
			ratio_result_flds = set(result_ratios.keys()) & set(result_fields.keys())
			
			for field in ratio_result_flds:
				if self.results[result_ratios[field][1]] != 0:
					self.results[field] = self.results[result_ratios[field][0]] / self.results[result_ratios[field][1]]
				else:
					self.results[field] = float('Inf')
			self.ratios_ready = True
			return True
		except KeyError as err:
			bu.error('Key ' + str(err) + ' not found')
		return False

	def calcResults(self):
		if self.results_ready == True: 
			return True
		if not self.ratios_ready:
			self.calcRatios()
		try:
			for field in self.avg_fields:
				self.results[field] = self.results[field] / max(self.battles,1)
			self.results['battles'] = self.battles
			self.results_ready = True
			return True
		except KeyError as err:
			bu.error('Key ' + str(err) + ' not found') 
		except Exception as err:
			bu.error(str(err)) 
		return False

	def getResults(self):
		if not self.results_ready:
			self.calcResults()
		results = []
		try:
			for field in result_fields.keys():
				results.append( result_fields[field][3].format(self.results[field]) )
			return results
		except KeyError as err:
			bu.error('Key ' + str(err) + ' not found') 
		except Exception as err:
			bu.error(str(err)) 
		return None
	
	def printResults(self):
		print(' : '.join(self.getResults()))


class BattleRecordCategory():
	def __init__(self, cat_name : str):
		self.category_name = cat_name
		self.category = collections.defaultdict(defaultvalueBtlRec)
		if result_categories[self.category_name][1] == 'string':
			self.type = 'string'
		elif result_categories[self.category_name][1] == 'number':
			self.type = 'number'
		elif result_categories[self.category_name][1] == 'total':
			self.type = 'total'
		else:
			self.type = 'category'
	
	def getSubCategories(self):
		return self.category.keys()

	def recordResult(self, result: dict):
		try:
			if self.type == 'total':
				cat = 'Total'
			elif self.type == 'number':
				cat = str(result[self.category_name])
			elif self.type == 'string':
				cat = result[self.category_name]
			else:
				cat = result_categories[self.category_name][1][result[self.category_name]]
			self.category[cat].recordResult(result)
			return True
		except KeyError as err:
			bu.error('Key ' + str(err) + ' not found') 
		except Exception as err:
			bu.error(str(err)) 
		return False
	
	def getHeaders(self):
		try:
			headers = [  result_cat_header_frmt.format(result_categories[self.category_name][0]) ]
			for field in result_fields.keys():
				print_format = '{:^' + str(result_fields[field][2]) + 's}'
				headers.append(print_format.format(result_fields[field][0]))
			return headers
		except KeyError as err:
			bu.error('Key ' + str(err) + ' not found') 
		except Exception as err:
			bu.error(str(err)) 
		return None

	def printResults(self):
		try:
			print('   '.join(self.getHeaders()))
			for row in self.getResults():
				print(' : '.join(row))
		except KeyError as err:
			bu.error('Key ' + str(err) + ' not found') 
		except Exception as err:
			bu.error(str(err)) 
		return None

	def getResults(self):
		try:
			results = []
			# results.append(self.getHeaders())			
			if self.type == 'number':
				for cat in sorted( [ int(s) for s in self.category.keys() ] ):
					cat = str(cat) 
					row = [ result_cat_frmt.format(cat) ]
					row.extend(self.category[cat].getResults())
					results.append(row)
			else:
				for cat in sorted(self.category.keys() , key=str.casefold):
					row = [ result_cat_frmt.format(cat) ]
					row.extend(self.category[cat].getResults())
					results.append(row)
			return results
		except KeyError as err:
			bu.error('Key ' + str(err) + ' not found') 
		return None

## main() -------------------------------------------------------------

async def main(argv):
	global wg, wi
	# set the directory for the script
	os.chdir(os.path.dirname(sys.argv[0]))

	## Read config
	config = configparser.ConfigParser()
	config.read(FILE_CONFIG)
	configDB 	= config['DATABASE']
	DB_SERVER 	= configDB.get('db_server', 'localhost')
	DB_PORT 	= configDB.getint('db_port', 27017)
	DB_SSL		= configDB.getboolean('db_ssl', False)
	DB_CERT_REQ = configDB.getint('db_ssl_req', ssl.CERT_NONE)
	DB_AUTH 	= configDB.get('db_auth', 'admin')
	DB_NAME 	= configDB.get('db_name', 'BlitzStats')
	DB_USER		= configDB.get('db_user', 'mongouser')
	DB_PASSWD 	= configDB.get('db_password', "PASSWORD")
	DB_CERT 	= configDB.get('db_ssl_cert_file', None)
	DB_CA 		= configDB.get('db_ssl_ca_file', None)

	bu.debug('DB_SERVER: ' + DB_SERVER)
	bu.debug('DB_PORT: ' + str(DB_PORT))
	bu.debug('DB_SSL: ' + "True" if DB_SSL else "False")
	bu.debug('DB_AUTH: ' + DB_AUTH)
	bu.debug('DB_NAME: ' + DB_NAME)

	TASK_N = 7

	parser = argparse.ArgumentParser(description='ANalyze Blitz replay JSONs from WoTinspector.com')
	parser.add_argument('--output', default='plain', choices=['json', 'plain', 'db'], help='Select output mode: JSON, plain text or database')
	parser.add_argument('-id', dest='accountID', type=int, default=None, help='WG account_id to analyze')
	parser.add_argument('-a', '--account', dest='account', type=str, default=None, help='WG account nameto analyze. Format: ACCOUNT_NAME@SERVER')
	parser.add_argument('-x', '--extended', action='store_true', default=False, help='Print Extended stats')
	parser.add_argument('-s', '--stats', action='store_true', default=False, help='Print player stats (WR/battles)')
	parser.add_argument('-u', '--url', dest= 'url', action='store_true', default=False, help='Print replay URLs')
	parser.add_argument('--tankfile', type=str, default='tanks.json', help='JSON file to read Tankopedia from. Default is "tanks.json"')
	parser.add_argument('--mapfile', type=str, default='maps.json', help='JSON file to read Blitz map names from. Default is "maps.json"')
	parser.add_argument('-o','--outfile', type=str, default='-', metavar="OUTPUT", help='File to write results. Default STDOUT')
	parser.add_argument('--db', action='store_true', default=False, help='Use DB - You are unlikely to have it')
	parser.add_argument('-d', '--debug', action='store_true', default=False, help='Debug mode')
	parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Verbose mode')
	parser.add_argument('files', metavar='FILE1 [FILE2 ...]', type=str, nargs='+', help='Files to read. Use \'-\' for STDIN"')
	
	args = parser.parse_args()
	bu.setVerbose(args.verbose)
	bu.setDebug(args.debug)
	wg = WG(WG_appID, args.tankfile, args.mapfile)
	wi = WoTinspector()
	bu.setWaiter(100)

	if args.account != None:
		args.accountID = await wg.getAccountID(args.account)
		bu.debug('WG  account_id: ' + str(args.accountID))

	#### Connect to MongoDB (TBD)
	client = None
	db = None
	if args.db:
		try:
			client = motor.motor_asyncio.AsyncIOMotorClient(DB_SERVER,DB_PORT, authSource=DB_AUTH, username=DB_USER, password=DB_PASSWD, ssl=DB_SSL, ssl_cert_reqs=DB_CERT_REQ, ssl_certfile=DB_CERT, tlsCAFile=DB_CA)
			db = client[DB_NAME]
			bu.debug('Database connection initiated')
		except Exception as err: 
			bu.error("Could no initiate DB connection: Disabling DB" + str(err)) 
			args.db = False
			pass
	else:
		bu.debug('No DB in use')

	try:
		replayQ  = asyncio.Queue()			
		reader_tasks = []
		# Make replay Queue
		scanner_task = asyncio.create_task(mkReplayQ(replayQ, args.files))
		bu.debug('Replay scanner started')
		# Start tasks to process the Queue
		for i in range(TASK_N):
			reader_tasks.append(asyncio.create_task(replayReader(replayQ, i, args)))
			bu.debug('ReplayReader ' + str(i) + ' started')

		bu.debug('Waiting for the replay scanner to finish')
		await asyncio.wait([scanner_task])
		
		bu.debug('Scanner finished. Waiting for replay readers to finish the queue')
		await replayQ.join()
		await asyncio.sleep(0.1)
		bu.debug('Replays read. Cancelling Readers and analyzing results')
		for task in reader_tasks:
			task.cancel()
			await asyncio.sleep(0.1)	
		results = []
		players = set()
		for res in await asyncio.gather(*reader_tasks):
			results.extend(res[0])
			players.update(res[1])

		## Remove battle_time from the player:tank data in case DB is not in use
		if db == None:
			players_tmp = set()
			for player in players:
				#bu.debug('playerID: ' + player + ' ==> ' + prunePlayerStatID(player))
				players_tmp.add(prunePlayerStatID(player))
			#bu.debug(str(len(players_tmp)))
			players = players_tmp

		player_stats = await processPlayerStats(players, TASK_N, db)
		bu.verbose('')
		bu.debug('PLAYER STATS:\n' + str(player_stats))
		teamresults = calcTeamStats(results, player_stats)
		processBattleStats(teamresults, args)	
		if args.stats: 
			processPlayerDist(results, player_stats)
	except Exception as err:
		bu.error(str(type(err)) + ' : ' +  str(err))
	finally:
		## Need to close the aiohttp.session since Python destructors do not support async methods... 
		await wg.close()
		await wi.close()
	return None

def processPlayerDist(result_list: list, player_stats: dict):
	
	hist_stats = dict()
	
	for field in histogram_fields.keys():
		hist_stats[field] = PlayerHistogram(field, histogram_fields[field][0], histogram_fields[field][1], histogram_fields[field][2], histogram_fields[field][3] )

	for result in result_list:
		for ally in result['allies']:
			for stat_field in hist_stats.keys():
				hist_stats[stat_field].recordAlly(player_stats[ally][stat_field])
		for enemy in result['enemies']:
			for stat_field in hist_stats.keys():
				hist_stats[stat_field].recordEnemy(player_stats[enemy][stat_field])
	
	for stat_field in hist_stats.keys():
		hist_stats[stat_field].print()
			
	return None


def processBattleStats(results: dict, args : argparse.Namespace):

	url 	= args.url
	urls 	= collections.OrderedDict()
	categories = {}
	
	cats = result_categories_default
	if args.extended:
		cats = result_categories.keys()
	
	for cat in cats:
		categories[cat] = BattleRecordCategory(cat)

	max_title_len = 0
	for result in results:
		for cat in cats:
			categories[cat].recordResult(result)
		if url:
			max_title_len = max(max_title_len, len(result['title']))
			urls[result['title']] = result['url']

	for cat in cats:
		for sub_cat in categories[cat].getSubCategories():
			categories[cat].category[sub_cat].calcResults()
		print('')
		categories[cat].printResults()
	if url:
		print('\nURLs to Battle replays:\n')
		for title in urls.keys():
			print(('{:' + str(3 + max_title_len) + '}').format(title) + ' : ' + urls[title])

	return None

async def processPlayerStats(players, N_workers: int, db : motor.motor_asyncio.AsyncIOMotorDatabase) -> dict:
	## Create queue of player-tank pairs to find stats for
	statsQ = asyncio.Queue()
	bu.debug('Create player stats queue: ' + str(len(players)) + ' players')
	for player in players:
		#bu.debug(player)
		await statsQ.put(player)
	# Process player WR / battle stats
	stats_tasks = []
	bu.debug('Starting player stats workers')
	for i in range(N_workers):
		bu.debug("Starting worker " + str(i))
		stats_tasks.append(asyncio.create_task(statWorker(statsQ, i, db)))
	## let the workers to process	
	bu.debug('Waiting stats workers to finish')
	await statsQ.join()
	bu.debug('Cancelling stats workers')
	for task in stats_tasks: 
		task.cancel()	
	player_stats = {}
	bu.debug('Gathering stats worker outputs')
	for stats in await asyncio.gather(*stats_tasks):
		player_stats = {**player_stats, **stats}
	bu.debug('Returning player_stats')
	return player_stats

def calcTeamStats(result_list: list, player_stats  : dict) -> list:
	return_list = []
	stat_types = player_stats[list(player_stats.keys())[0]].keys()
	bu.debug('stat_types: ' + ','.join(stat_types))
	for result in result_list:
		try:
			n_allies = collections.defaultdict(defaultvalueZero)
			allies_stats = collections.defaultdict(defaultvalueZero)
			bu.debug('Processing Allies')
			for ally in result['allies']:
				# Player itself is not in 'allies': see readReplayJSON()
				if ally not in player_stats: continue
				for stat in stat_types:
					if player_stats[ally][stat] != None:
						allies_stats[stat] += player_stats[ally][stat]
						n_allies[stat] += 1
			bu.debug('Processing Allies\' avg stats')
			for stat in stat_types:
				if  n_allies[stat] > 0:
					result['allies_' + str(stat)] = allies_stats[stat] / n_allies[stat]
			
			bu.debug('Processing Enemies')
			n_enemies = collections.defaultdict(defaultvalueZero)
			enemies_stats = collections.defaultdict(defaultvalueZero)
			for enemy in result['enemies']:
				if enemy not in player_stats: continue
				for stat in stat_types:
					if player_stats[enemy][stat] != None:
						enemies_stats[stat] += player_stats[enemy][stat]
						n_enemies[stat] += 1
			for stat in stat_types:
				if  n_enemies[stat] > 0:
					result['enemies_' + str(stat)] = enemies_stats[stat] / n_enemies[stat]

			return_list.append(result)
		except KeyError as err:
			bu.error('Key :' + str(err) + ' not found')
		except Exception as err:
			bu.error(err)
	
	return return_list

async def statWorker(queue : asyncio.Queue, workerID: int, db : motor.motor_asyncio.AsyncIOMotorDatabase) -> list:
	"""Worker thread to find stats for player / tank pairs"""
	# global wg
	stats = {}
	bu.debug("workedID: " + str(workerID) + ' started')
	try:
		while True:
			item = await queue.get()
			try:
				bu.printWaiter()
				bu.debug(str(item))
				## Add time stamp here
				stat 		= item.split(':')	
				account_id 	= int(stat[0])
				tier 		= int(stat[1])
				
				if len(stat) == 3:
					battletime 	= int(stat[2]) 
				else:
					battletime = None

				bu.debug('account_id: ' + str(account_id) + ' tank_tier: ' + str(tier), workerID)

				stats[item] = {}
				stats[item] = await getDBplayerStats(db, account_id, tier, battletime)
				bu.debug('getDBplayerStats returned: ' + str(stats[item]), workerID)
				if stats[item] == None:
					stats[item] = await getWGplayerStats(account_id, tier)
					stats[prunePlayerStatID(item)] = stats[item]
				if stats[item] == None:
					del stats[item]
				
			except KeyError as err:
				bu.error('Key :' + str(err) + ' not found', workerID)
			except Exception as err:
				bu.error('Type: ' + str(type(err)) + ' : ' + str(err), workerID)
			queue.task_done()

	except (asyncio.CancelledError, concurrent.futures._base.CancelledError):
		bu.debug('Stats queue is empty', workerID)
	except Exception as err:
		bu.error(str(err))
	return stats
	

async def getWGplayerStats(account_id: int, tier: int) -> dict:
	"""Get player stats from WG. Returns WR per tier of tank_id"""
	try:
		tier_tanks = wg.getTanksByTier(tier)

		# 'battles' must always be there
		hist_stats = [ 'all.' + x for x in histogram_fields.keys() ]

		playerStats = await wg.getPlayerTanksStats(account_id, tier_tanks ,hist_stats)
		bu.debug('account_id: ' + str(account_id) + ' ' + str(playerStats))

		return await statsHelper(playerStats)

	except KeyError as err:
		bu.error('account_id: ' + str(account_id) + ' Key :' + str(err) + ' not found')
	except Exception as err:
		bu.error('Type: ' + str(type(err)) + ' : ' + str(err))
	return None
	

async def getDBplayerStats(db : motor.motor_asyncio.AsyncIOMotorDatabase, account_id : int, tier: int, battletime: int) -> dict:
	"""Get player stats from MongoDB (you are very unlikely to have it, unless you have set it up)"""
	if db == None:
		return None
	try:
		dbc = db[DB_C_TANK_STATS]
		tier_tanks = wg.getTanksByTier(tier)
		#bu.debug('Tank_ids: ' + ','.join(map(str, tier_tanks)))
		
		if battletime == None:
			pipeline = 	[ { '$match': { '$and': [ { 'account_id': account_id }, { 'tank_id' : {'$in': tier_tanks} } ]}}, 
					{ '$sort': { 'last_battle_time': -1 }}, 
					{ '$group': { '_id': '$tank_id', 'doc': { '$first': '$$ROOT' }}}, 
					{ '$replaceRoot': { 'newRoot': '$doc' }}, 
					{ '$project': { '_id': 0 }} ]
		else:
			pipeline = 	[ { '$match': { '$and': [ { 'account_id': account_id }, { 'last_battle_time': { '$gte': battletime }}, { 'tank_id' : {'$in': tier_tanks} } ]}}, 
					{ '$sort': { 'last_battle_time': 1 }}, 
					{ '$group': { '_id': '$tank_id', 'doc': { '$first': '$$ROOT' }}}, 
					{ '$replaceRoot': { 'newRoot': '$doc' }}, 
					{ '$project': { '_id': 0 }} ]

		# cursor = dbc.aggregate(pipeline, allowDiskUse=True)
		cursor = dbc.aggregate(pipeline)

		return await statsHelper(await cursor.to_list(1000)) 
				
	except Exception as err:
		bu.error('account_id: ' + str(account_id) + ' Error :' + str(err))
	return None


async def statsHelper(stat_list: list):
	"""Helpher func got getDBplayerStats() and getWGplayerStats()"""
	try:
		if stat_list == None: 
			return None
		stats = {}
		hist_fields = histogram_fields.keys()
		if 'battles' not in hist_fields:
			bu.error('\'battles\' must be defined in \'histogram_fields\'')
			sys.exit(1)
		for field in hist_fields:
			stats[field] = 0
		
		for tankStat in stat_list: 
			tankStat = tankStat['all']
			for field in hist_fields:
				stats[field] += max(0, tankStat[field])

		if stats['battles'] == 0:
			return None

		for field in hist_fields:
			if field == 'battles': continue
			stats[field] = stats[field] / stats['battles']
		return stats

	except Exception as err:
		bu.error(str(type(err)) + ' : ' +  str(err))
	return None


async def mkReplayQ(queue : asyncio.Queue, files : list):
	"""Create queue of replays to post"""
	p_replayfile = re.compile('.*\\.wotbreplay\\.json$')
	if files[0] == '-':
		bu.debug('reading replay file list from STDIN')
		stdin, _ = await aioconsole.get_standard_streams()
		while True:
			line = (await stdin.readline()).decode('utf-8').rstrip()
			if not line: 
				break
			else:
				if (p_replayfile.match(line) != None):
					await queue.put(await mkReaderQitem(line))
	else:
		for fn in files:
			# bu.debug('Filename: ' + fn)
			if fn.endswith('"'):
				fn = fn[:-1] 
			if os.path.isfile(fn) and (p_replayfile.match(fn) != None):
				await queue.put(await mkReaderQitem(fn))
				bu.debug('File added to queue: ' + fn)
			elif os.path.isdir(fn):
				with os.scandir(fn) as dirEntry:
					for entry in dirEntry:
						if entry.is_file() and (p_replayfile.match(entry.name) != None): 
							bu.debug(entry.name)
							await queue.put(await mkReaderQitem(entry.path))
							bu.debug('File added to queue: ' + entry.path)
	bu.debug('Finished: ' + str(queue.qsize())  + ' replays to process') 
	return None

async def mkReaderQitem(filename : str) -> list:
	"""Make an item to replay queue"""
	global REPLAY_N
	REPLAY_N +=1
	return [filename, REPLAY_N]

async def replayReader(queue: asyncio.Queue, readerID: int, args : argparse.Namespace):
	"""Async Worker to process the replay queue"""
	#global SKIPPED_N
	# account_id = args.accountID
	results = []
	playerstanks = set()
	try:
		while True:
			item = await queue.get()
			filename = item[0]
			replayID = item[1]

			try:
				replay_json = await bu.readJSON(filename, wi.chkJSONreplay)
				if replay_json == None:
					bu.verbose('Replay[' + str(replayID) + ']: ' + filename + ' is invalid. Skipping.' )
					#SKIPPED_N += 1
					queue.task_done()
					continue
						
				## Read the replay JSON
				bu.debug('[' + str(readerID) + '] reading replay: ' + filename)
				result = await readReplayJSON(replay_json, args)
				bu.printWaiter()
				if result == None:
					bu.error('[' + str(readerID) + '] Error reading replay: ' + filename)
					queue.task_done()
					continue
				
				# if (account_id != None):						
				playerstanks.update(set(result['allies']))
				playerstanks.update(set(result['enemies']))	
				
				results.append(result)
				bu.debug('Marking task ' + str(replayID) + ' done')
						
			except Exception as err:
				bu.error(str(err))
			queue.task_done()
	except (asyncio.CancelledError, concurrent.futures.CancelledError):		
		bu.debug('[' + str(readerID) + ']: ' + str(len(results)) + ' replays, ' + str(len(playerstanks)) + ' player/tanks')
		return results, playerstanks
	return None

async def readReplayJSON(replay_json: dict, args : argparse.Namespace) -> dict:
	"""Parse replay JSON dict"""
	global REPLAY_I
	account_id = args.accountID
	url = args.url
	db = args.db
	result = {}
	try:
		if account_id == None:
			account_id = replay_json['data']['summary']['protagonist'] 
		elif replay_json['data']['summary']['protagonist'] != account_id:
			if account_id in replay_json['data']['summary']['enemies']:
				# switch the teams...
				if replay_json['data']['summary']['battle_result'] != 2:
					if replay_json['data']['summary']['battle_result'] == 0:
						replay_json['data']['summary']['battle_result'] = 1
					else: 
						replay_json['data']['summary']['battle_result'] = 0
				tmp = replay_json['data']['summary']['enemies']
				replay_json['data']['summary']['enemies'] = replay_json['data']['summary']['allies']
				replay_json['data']['summary']['allies'] = tmp
			elif account_id not in replay_json['data']['summary']['allies']:
				# account_id looked for but not found in teams
				bu.debug('Replay ' + replay_json['data']['summary']['title'] + ' does not have account_id ' + str(account_id) + '. Skipping.')
				return None

		if url: 
			result['url'] = replay_json['data']['view_url']
		#bu.debug('1')
		for key in replay_summary_flds:
			result[key] = replay_json['data']['summary'][key]
		result['battle_start_timestamp'] = int(replay_json['data']['summary']['battle_start_timestamp'])
		#bu.debug('2')
		result['allies'] = set()
		result['enemies'] = set()
	
		btl_duration = 0
		btl_tier = 0
		for player in replay_json['data']['summary']['details']:
			#bu.debug('3')
			btl_duration = max(btl_duration, player['time_alive'])
			player_tank_tier = wg.getTankData(player['vehicle_descr'], 'tier')
			btl_tier = max(btl_tier, player_tank_tier)
			#bu.debug('4')
			if player['dbid'] == account_id:
				# player itself is not put in results['allies']
				tmp = {}
				tmp['account_id'] 	= account_id
				tmp['tank_id'] 		= player['vehicle_descr']
				tmp['tank_tier'] 	= player_tank_tier
				tmp['tank_name']	= wg.getTankData(tmp['tank_id'], 'name')
				tmp['squad_index'] 	= player['squad_index']

				for key in replay_details_flds:
					tmp[key] = player[key]
				if player['hitpoints_left'] == 0:
					tmp['survived'] = 0
					tmp['destroyed'] = 1
				else:
					tmp['survived'] 	= 1
					tmp['destroyed'] 	= 0
				
				for key in tmp.keys():
					result[key] = tmp[key]				

			else:
				tmp_account_id 	= player['dbid']
				tmp_tank_id 	= player['vehicle_descr']
				tmp_tank_tier 	= wg.getTankTier(tmp_tank_id)				
				tmp_battletime	= int(replay_json['data']['summary']['battle_start_timestamp'])
				
				if player['dbid'] in replay_json['data']['summary']['allies']: 
					result['allies'].add(getPlayerStatID(tmp_account_id, tmp_tank_tier, tmp_battletime, db))
				else:
					result['enemies'].add(getPlayerStatID(tmp_account_id, tmp_tank_tier, tmp_battletime, db))

		# platoon buddy removed from stats 			
		if result['squad_index'] != None:
			for ally in replay_json['data']['summary']['allies']:
				if (ally['dbid'] != account_id) and (ally['squad_index'] == result['squad_index']):
					# platoon buddy found 
					tmp_account_id 	= str(ally['dbid'])
					tmp_tank_id 	= ally['vehicle_descr']
					tmp_tank_tier 	= str(wg.getTankTier(tmp_tank_id))
					tmp_battletime	= str(int(replay_json['data']['summary']['battle_start_timestamp']))
					# platoon buddy removed from stats 
					result['allies'].remove(getPlayerStatID(tmp_account_id, tmp_tank_tier, tmp_battletime, db))
		
		#bu.debug('6')
		result['time_alive%'] = result['time_alive'] / btl_duration  
		result['battle_tier'] = btl_tier
		result['top_tier'] = 1 if (result['tank_tier'] == btl_tier) else 0
		result['win'] = 1 if result['battle_result'] == 1 else 0
		REPLAY_I += 1
		result['battle_i'] = REPLAY_I
				
		bu.debug(str(result))
		return result
	except KeyError as err:
		bu.error('Key :' + str(err) + ' not found')
	except Exception as err:
		bu.error(str(type(err)) + ' : ' +  str(err))
	return None

def prunePlayerStatID(id: str) -> str: 
	stat 		= id.split(':')	
	#bu.debug('Stat: ' + id + ' ==> ' +  ':'.join([stat[0], stat[1]]))
	return ':'.join([stat[0], stat[1]])

def getPlayerStatID(account_id: int, tier: int, battletime: int, db = False) -> str:
	if db: 
		battle_date = date.fromtimestamp(battletime) 
		battle_date = battle_date - timedelta(days=battle_date.weekday())
		#bu.debug('account_id: ' + str(account_id) +  ' tier: ' + str(tier) + ' battle_time: ' + str(int(time.mktime(battle_date.timetuple()))))
		return ':'.join( [ str(account_id), str(tier), str(int(time.mktime(battle_date.timetuple()))) ] )  
	else:
		return ':'.join([ str(account_id), str(tier)])

### main()
if __name__ == "__main__":
   asyncio.run(main(sys.argv[1:]), debug=False)
