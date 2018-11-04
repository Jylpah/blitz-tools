#!/usr/bin/python3.7

# Script Analyze WoT Blitz replays

import sys, argparse, json, os, inspect, aiohttp, asyncio, aiofiles, aioconsole, re, logging, time, xmltodict, collections
import blitzutils as bu
from blitzutils import WG


logging.getLogger("asyncio").setLevel(logging.DEBUG)

WG_appID  = '81381d3f45fa4aa75b78a7198eb216ad'
wg = None
REPLAY_N = 0
STAT_TANK_BATTLE_MIN = 100

replay_summary_flds = [
	'battle_result',
	'battle_type',
	'map_name',
	'battle_duration',
	'battle_start_timestamp',
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
	'total'				: [ 'TOTAL', 'Total' ],
	'battle_result'		: [ 'Result', [ 'Loss', 'Win', 'Draw']],
	'battle_type'		: [ 'Battle Type',['Encounter', 'Supremacy']],
	'battle_tier'		: [ 'Battle Tier', 'value' ],
	'top_tier'			: [ 'Tier', ['Bottom tier', 'Top tier']],
	'tank_name'			: [ 'Tank', 'value' ],
	'map_name'			: [ 'Map', 'value' ]
	}

result_cat_header_frmt = '{:_<17s}'
result_cat_frmt = '{:>17s}'

result_fields = {
	'battles'			: [ 'Battles', 'Number of battles', 8, '{:^8.0f}' ],
	'win'				: [ 'WR', 'Win rate', 			6, '{:6.1%}' ],
	'damage_made'		: [ 'DPB', 'Average Damage', 	5, '{:5.0f}' ],
	'DR'				: [ 'DR', 'Damage Ratio', 		5, '{:5.1f}' ],
	'KDR'				: [ 'KDR', 'Kills / Death', 	4, '{:4.1f}' ],
	'enemies_spotted'	: [ 'Spot', 'Enemies spotted per battle', 		4, '{:4.1f}' ],
	'hit_rate'			: [ 'Hit rate', 'Shots hit / all shots made', 	8, '{:8.1%}' ],
	'pen_rate'			: [ 'Pen rate', 'Shots pen / shots hit', 		8, '{:8.1%}' ],
	'survived'			: [ 'Surv%', 'Survival rate', 						6, '{:6.1%}' ],
	'time_alive%'		: [ 'T alive%', 'Percentage of time being alive in a battle', 8, '{:8.0%}' ], 
	'top_tier'			: [ 'Top tier', 'Share of games as top tier', 	8, '{:8.0%}' ],
	'allies_win_rate'	: [ 'Allies WR', 'Average WR of allies: WR in the tank played counted if more than ' + str(STAT_TANK_BATTLE_MIN) + ' in the tank', 9, '{:9.2%}' ],
	'enemies_win_rate'	: [ 'Enemies WR', 'Average WR of enemies: WR in the tank played counted if more than ' + str(STAT_TANK_BATTLE_MIN) + ' in the tank', 10, '{:10.2%}' ],
	'allies_battles'	: [ 'Allies Btls', 'Average number battles of the allies', 		11, '{:11.0f}' ],
	'enemies_battles'	: [ 'Enemies Btls', 'Average number battles of the enemies', 	12, '{:12.0f}' ]
	
}

battle_stats_fields = {
	'WR'			: 'Win rate',
	'Battles'		: 'Battles'
	}

def defaultvalueZero():
    return 0

def defaultvalueBtlRec():
	return BattleRecord()

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
				self.results[field] = self.results[field] / self.battles
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
	
	def getSubCategories(self):
		return self.category.keys()

	def recordResult(self, result: dict):
		try:
			if self.category_name == 'total':
				cat = 'Total'
			elif result_categories[self.category_name][1] == 'value':
				cat = str(result[self.category_name])
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
			for cat in self.category.keys():
				row = [ result_cat_frmt.format(cat)  ]
				row.extend(self.category[cat].getResults())
				results.append(row)
			return results
		except KeyError as err:
			bu.error('Key ' + str(err) + ' not found') 
		return None

## main() -------------------------------------------------------------

async def main(argv):
	global wg
	
	TASK_N = 7
	WG_ID = 2008897346
	parser = argparse.ArgumentParser(description='ANalyze Blitz replay JSONs from WoTinspector.com')
	parser.add_argument('--output', default='plain', choices=['json', 'plain', 'db'], help='Select output mode: JSON, plain text or database')
	parser.add_argument('-id', dest='accountID', type=int, default=WG_ID, help='WG account_id to analyze')
	parser.add_argument('-a', '--account', dest='account', type=str, default=None, help='WG account nameto analyze. Format: ACCOUNT_NAME@SERVER')
	parser.add_argument('-u', '--url', dest= 'url', action='store_true', default=False, help='Print replay URLs')
	parser.add_argument('--tankfile', type=str, default='tanks.json', help='JSON file to read Tankopedia from. Default is "tanks.json"')
	parser.add_argument('--mapfile', type=str, default='maps.json', help='JSON file to read Blitz map names from. Default is "maps.json"')
	parser.add_argument('-o','--outfile', type=str, default='-', metavar="OUTPUT", help='File to write results. Default STDOUT')
	parser.add_argument('-d', '--debug', action='store_true', default=False, help='Debug mode')
	parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Verbose mode')
	parser.add_argument('files', metavar='FILE1 [FILE2 ...]', type=str, nargs='+', help='Files to read. Use \'-\' for STDIN')
	
	args = parser.parse_args()
	bu.setVerbose(args.verbose)
	bu.setDebug(args.debug)
	wg = WG(WG_appID, args.tankfile, args.mapfile)

	if args.account != None:
		args.accountID = await wg.getAccountID(args.account)
		bu.debug('WG  account_id: ' + str(args.accountID))

	try:
		replayQ  = asyncio.Queue()	

		#resultsQ = asyncio.Queue()
		
		reader_tasks = []
		# Make replay Queue
		scanner_task = asyncio.create_task(mkReplayQ(replayQ, args.files))

		# Start tasks to process the Queue
		for i in range(TASK_N):
			reader_tasks.append(asyncio.create_task(replayReader(replayQ, i, args)))
			bu.debug('Task ' + str(i) + ' started')

		bu.debug('Waiting for the replay scanner to finish')
		await asyncio.wait([scanner_task])
		bu.debug('Scanner finished. Waiting for replay readers to finish the queue')
		await replayQ.join()
		bu.debug('Replays read. Cancelling Readers and analyzing results')
		for task in reader_tasks:
			task.cancel()	
		results = []
		playerstanks = set()
		for res in await asyncio.gather(*reader_tasks):
			results.extend(res[0])
			playerstanks.update(res[1])

		player_stats = await processTankStats(playerstanks, TASK_N)
		bu.verbose('')
		results = calcTeamStats(results, player_stats)
		processStats(results, args)	

	finally:
		## Need to close the aiohttp.session since Python destructors do not support async methods... 
		await wg.session.close()
	return None

def processStats(results: dict, args : argparse.Namespace):

	url = args.url
	urls = collections.OrderedDict()
	categories = {}
	for cat in result_categories.keys():
		categories[cat] = BattleRecordCategory(cat)

	max_title_len = 0
	for result in results:
		for cat in result_categories.keys():
			categories[cat].recordResult(result)
		if url:
			max_title_len = max(max_title_len, len(result['title']))
			urls[result['title']] = result['url']

	for cat in result_categories.keys():
		for sub_cat in categories[cat].getSubCategories():
			categories[cat].category[sub_cat].calcResults()
		print('')
		categories[cat].printResults()
	if url:
		print('\nURLs to Battle replays:\n')
		for title in urls.keys():
			print(('{:' + str(3 + max_title_len) + '}').format(title) + ' : ' + urls[title])

	return None

async def processTankStats(playerstanks, N_workers: int) -> dict:
	## Create queue of player-tank pairs to find stats for
	statsQ = asyncio.Queue()
	bu.debug('Create player/tank stats queue')
	for playertank in playerstanks:
		await statsQ.put(playertank)
	# Process player WR / battle stats
	stats_tasks = []
	bu.debug('Starting player/tank stats queue workers')
	for i in range(N_workers):
		stats_tasks.append(asyncio.create_task(statWorker(statsQ, i)))
	## let the workers to process	
	await statsQ.join()
	for task in stats_tasks: 
		task.cancel()	
	player_stats = {}
	for stats in await asyncio.gather(*stats_tasks):
		player_stats = {**player_stats, **stats}
	
	return player_stats

def calcTeamStats(result_list: list, player_stats  : dict) -> list:
	return_list = []
	stat_types = player_stats[list(player_stats.keys())[0]].keys()
	for result in result_list:
		try:
			n_allies = collections.defaultdict(defaultvalueZero)
			allies_stats = collections.defaultdict(defaultvalueZero)
			for ally in result['allies']:
				for stat in stat_types:
					if player_stats[ally][stat] != None:
						allies_stats[stat] += player_stats[ally][stat]
						n_allies[stat] += 1
			
			for stat in stat_types:
				result['allies_' + str(stat)] = allies_stats[stat] / n_allies[stat]

			n_enemies = collections.defaultdict(defaultvalueZero)
			enemies_stats = collections.defaultdict(defaultvalueZero)
			for enemy in result['enemies']:
				for stat in stat_types:
					if player_stats[enemy][stat] != None:
						enemies_stats[stat] += player_stats[enemy][stat]
						n_enemies[stat] += 1
			for stat in stat_types:
				result['enemies_' + str(stat)] = enemies_stats[stat] / n_enemies[stat]

			return_list.append(result)
		except KeyError as err:
			bu.error('Key :' + str(err) + ' not found')
		except Exception as err:
			bu.error(err)
	
	return return_list

async def statWorker(queue : asyncio.Queue, workerID: int) -> list:
	"""Worker thread to find stats for player / tank pairs"""
	# global wg
	stats = {}

	try:
		while True:
			# item = await queue.get_nowait()
			item = await queue.get()
			try:
				bu.printWaiter()
				acc, tank = item.split(':')			
				account_id = int(acc)
				tank_id = int(tank)
				# bu.debug('[' +str(workerID) + '] AccountID: ' + acc + ' TankID: '  + tank)
				playerTankStat = await wg.getPlayerTankStats(account_id, tank_id, ['all.battles', 'all.wins'])
				# bu.debug('[' +str(workerID) + '] ' + str(playerTankStat))
				playerStat = await wg.getPlayerStats(account_id,  ['statistics.all.battles', 'statistics.all.wins'])
				# bu.debug('[' +str(workerID) + '] ' + str(playerStat))
			
				stats[item] = {}
				if (playerTankStat == None) or (playerStat == None):
					stats[item]['win_rate'] = None
					stats[item]['battles'] 	= None
				else:
					playerTankStat = playerTankStat['all']
					playerStat = playerStat['statistics']['all']

					battles = playerStat['battles']
					stats[item]['battles'] = battles

					battles_in_tank = playerTankStat['battles']
					if battles_in_tank >= STAT_TANK_BATTLE_MIN:
						stats[item]['win_rate'] =  min(playerTankStat['wins'] / battles_in_tank,1)  # To cope with broken stats in WG DB
					else:
						stats[item]['win_rate'] = min(playerStat['wins'] / battles, 1) # To cope with broken stats in WG DB
					bu.debug('[' +str(workerID) + '] Player[' + str(account_id) + '], Tank[' + str(tank_id) + '] : WR : ' + str(stats[item]['win_rate']) + ' Battles: ' + str(battles))
			except KeyError as err:
				bu.error('[' +str(workerID) + '] Key :' + str(err) + ' not found')
			except Exception as err:
				bu.error('[' +str(workerID) + '] ' + str(err))
			queue.task_done()

	except asyncio.CancelledError:
		bu.debug('Stats queue[' + str(workerID) + '] is empty')
	except Exception as err:
		bu.error(str(err))
	return stats
	
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
			if os.path.isfile(fn) and (p_replayfile.match(fn) != None):
				await queue.put(await mkReaderQitem(fn))
			elif os.path.isdir(fn):
				with os.scandir(fn) as dirEntry:
					for entry in dirEntry:
						if entry.is_file() and (p_replayfile.match(entry.name) != None): 
							bu.debug(entry.name)
							await queue.put(await mkReaderQitem(entry.path))
			bu.debug('File added to queue: ' + fn)
	bu.debug('Finished')
	return None

async def mkReaderQitem(filename : str) -> list:
	"""Make an item to replay queue"""
	global REPLAY_N
	REPLAY_N +=1
	return [filename, REPLAY_N]

async def replayReader(queue: asyncio.Queue, readerID: int, args : argparse.Namespace):
	"""Async Worker to process the replay queue"""
	#global SKIPPED_N
	account_id = args.accountID
	results = []
	playerstanks = set()
	try:
		while True:
			item = await queue.get()
			filename = item[0]
			replayID = item[1]

			try:
				if os.path.exists(filename) and os.path.isfile(filename):
					async with aiofiles.open(filename) as fp:
						replay_json = json.loads(await fp.read())
						if replay_json['status'] != 'ok':
							bu.verbose('Replay[' + str(replayID) + ']: ' + filename + ' is invalid. Skipping.' )
							#SKIPPED_N += 1
							queue.task_done()
							continue
						
						## Read the replay JSON
						bu.debug('[' + str(readerID) + '] reading replay: ' + filename)
						result = await readReplayJSON(replay_json, args)
						if result == None:
							bu.error('[' + str(readerID) + '] Error reading replay: ' + filename)
							queue.task_done()
							continue
						
						if (account_id != None):						
							playerstanks.update(set(result['allies']))
							playerstanks.update(set(result['enemies']))	
						
						results.append(result)
						bu.debug('Marking task ' + str(replayID) + ' done')
						
			except Exception as err:
				bu.error(str(err))
			queue.task_done()
	except asyncio.CancelledError:		
		return results, playerstanks
	return None

async def readReplayJSON(replay_json: dict, args : argparse.Namespace) -> dict:
	"""Parse replay JSON dict"""
	account_id = args.accountID
	url = args.url
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

		for key in replay_summary_flds:
			result[key] = replay_json['data']['summary'][key]

		result['allies'] = set()
		result['enemies'] = set()

		btl_duration = 0
		btl_tier = 0
		for player in replay_json['data']['summary']['details']:
			btl_duration = max(btl_duration, player['time_alive'])
			player_tank_tier = wg.getTankData(player['vehicle_descr'], 'tier')
			btl_tier = max(btl_tier, player_tank_tier)
			if player['dbid'] == account_id:
				tmp = {}
				for key in replay_details_flds:
					tmp[key] = player[key]
				if player['hitpoints_left'] == 0:
					tmp['survived'] = 0
					tmp['destroyed'] = 1
				else:
					tmp['survived'] 	= 1
					tmp['destroyed'] 	= 0
				tmp['account_id'] 	= account_id
				tmp['tank_id'] 		= player['vehicle_descr']
				tmp['tank_tier'] 	= player_tank_tier
				tmp['tank_name']	= wg.getTankData(tmp['tank_id'], 'name')
				
				for key in tmp.keys():
					result[key] = tmp[key]				

			elif player['dbid'] in replay_json['data']['summary']['allies']: 
				result['allies'].add(':'.join([ str(player['dbid']), str(player['vehicle_descr'])]))
			else:
				result['enemies'].add(':'.join([ str(player['dbid']), str(player['vehicle_descr'])]))
		result['time_alive%'] = result['time_alive'] / btl_duration  
		result['battle_tier'] = btl_tier
		result['top_tier'] = 1 if (result['tank_tier'] == btl_tier) else 0
		result['win'] = 1 if result['battle_result'] == 1 else 0
				
		bu.debug(str(result))
		return result
	except KeyError as err:
		bu.error('Key :' + str(err) + ' not found')
	except Exception as err:
		bu.error(err)
	return None

### main()
if __name__ == "__main__":
   asyncio.run(main(sys.argv[1:]), debug=True)
   #asyncio.run(main(sys.argv[1:]))
