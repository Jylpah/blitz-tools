#!/usr/bin/env python3.8

import json, argparse, inspect, sys, os, io, base64, aiohttp, urllib, asyncio, aiofiles, aioconsole
import logging, re, concurrent.futures, configparser, motor.motor_asyncio, ssl, zipfile
import blitzutils as bu
from blitzutils import WG
from blitzutils import WoTinspector

logging.getLogger("asyncio").setLevel(logging.DEBUG)

FILE_CONFIG 	= 'blitzstats.ini'
# DB_C_REPLAYS   	= 'Replays'

DEBUG 	= False
VERBOSE = False
SLEEP 		= 2
MAX_RETRIES = 3
REPLAY_N 	= 0
SKIPPED_N 	= 0
ERROR_N 	= 0
WIurl='https://wotinspector.com/api/replay/upload?'
WG_appID  = '81381d3f45fa4aa75b78a7198eb216ad'
wg = None
wi = None

async def main(argv):
	global wg, wi
	current_dir = os.getcwd()
	# set the directory for the script
	os.chdir(os.path.dirname(sys.argv[0]))
	
	# options defaults
	OPT_WORKERS_N  	= 5
	WG_ID 			= None
	WG_ACCOUNT 		= None 	# format: nick@server, where server is either 'eu', 'ru', 'na', 'asia' or 'china'. 
	  					# China is not supported since WG API stats are not available there
	WG_RATE_LIMIT 	= 10

	try:
		## Read config
		if os.path.isfile(FILE_CONFIG):
			bu.debug('Reading config file: ' + FILE_CONFIG)
			config = configparser.ConfigParser()
			config.read(FILE_CONFIG)
			try:
				if 'OPTIONS' in config.sections():
					configOptions 	= config['OPTIONS']
					OPT_WORKERS_N = configOptions.getint('opt_uploader_workers', OPT_WORKERS_N)
			except (KeyError, configparser.NoSectionError) as err:
				bu.error(exception=err)
				
			try:
				if 'WG' in config.sections():
					configWG 		= config['WG']
					# WG account id of the uploader: 
					# # Find it here: https://developers.wargaming.net/reference/all/wotb/account/list/
					WG_ID			= configWG.getint('wg_id', WG_ID)
					WG_ACCOUNT		= configWG.get('wg_account', WG_ACCOUNT)
					## WG API Rules limit 10 request / sec. Higher rate of requests will return errors ==> extra delay
					WG_RATE_LIMIT	= configWG.getint('wg_rate_limit', WG_RATE_LIMIT)
			except (KeyError, configparser.NoSectionError) as err:
				bu.error(exception=err)
		
	except Exception as err:
		bu.error(exception=err)

	parser = argparse.ArgumentParser(description='Post replays(s) to WoTinspector.com and retrieve replay data as JSON')
	parser.add_argument('-id', dest='accountID', type=int, default=WG_ID, help='WG account_id')
	parser.add_argument('-a', '--account', dest='account', type=str, default=WG_ACCOUNT, help='Uploader\'s WG account name. Format: ACCOUNT_NAME@SERVER')
	parser.add_argument('-t','--title', type=str, default=None, help='Title for replays. Use "NN" for continous numbering. Default is automatic naming')
	parser.add_argument('-p', '--private', dest="private", action='store_true', default=False, help='Set replays private on WoTinspector.com')
	parser.add_argument('--tankopedia', type=str, default='tanks.json', help='JSON file to read Tankopedia from. Default: "tanks.json"')
	parser.add_argument('--mapfile', type=str, default='maps.json', help='JSON file to read Blitz map names from. Default: "maps.json"')
	parser.add_argument('-d', '--debug', action='store_true', default=False, help='Debug mode')
	parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Verbose mode')
	parser.add_argument('-s', '--silent', action='store_true', default=False, help='Silent mode')
	parser.add_argument('files', metavar='FILE1 [FILE2 ...]', type=str, nargs='+', help='Files to read. Use \'-\' for STDIN"')
	args = parser.parse_args()

	bu.set_verbose(args.verbose)
	bu.set_log_level(args.silent, args.verbose, args.debug)

	wg = WG(WG_appID, args.tankopedia, args.mapfile)
	wi = WoTinspector(rate_limit=WG_RATE_LIMIT)

	if args.account != None:
		args.accountID = await wg.get_account_id(args.account)
		bu.debug('WG  account_id: ' + str(args.accountID))

	if args.accountID == None: 
		args.accountID = 0

	try:
		queue  = asyncio.Queue()	
		# rebase file arguments due to moving the working directory to the script location
		args.files = bu.rebase_file_args(current_dir, args.files)

		tasks = []
		# Make replay Queue
		tasks.append(asyncio.create_task(mkReplayQ(queue, args.files, args.title)))
		# Start tasks to process the Queue
		for i in range(OPT_WORKERS_N):
			tasks.append(asyncio.create_task(replayWorker(queue, i, args.accountID, args.private)))
			bu.debug('Task ' + str(i) + ' started')
		
		bu.debug('Waiting for the replay scanner to finish')
		await asyncio.wait([tasks[0]])
		bu.debug('Scanner finished. Waiting for workers to finish queue')
		await queue.join()
		bu.debug('Cancelling workers')
		for task in tasks:
			task.cancel()
		bu.debug('Waiting for workers to cancel')
		await asyncio.gather(*tasks, return_exceptions=True)
		bu.verbose_std(str(REPLAY_N) + ' replays: ' + str(REPLAY_N - SKIPPED_N - ERROR_N) + ' uploaded, ' + str(SKIPPED_N) + ' skipped, ' + str(ERROR_N) + ' errors')
				
	except KeyboardInterrupt:
		print('Ctrl-c pressed ...')
		sys.exit(1)
	finally:
		## Need to close the aiohttp.session since Python destructors do not support async methods... 
		await wg.session.close()
		await wi.close()

	return None


async def mkReplayQ(queue : asyncio.Queue, files : list, title : str):
	"""Create queue of replays to post"""
	p_replayfile = re.compile('.*\\.wotbreplay$')
	if files[0] == '-':
		bu.debug('reading replay file list from STDIN')
		stdin, _ = await aioconsole.get_standard_streams()
		while True:
			line = (await stdin.readline()).decode('utf-8').rstrip()
			if not line: 
				break
			else:
				if (p_replayfile.match(line) != None):
					await queue.put(await mkQueueItem(line, title))
	else:
		for fn in files:
			if fn.endswith('"'):
				fn = fn[:-1]  
			if os.path.isfile(fn) and (p_replayfile.match(fn) != None):
				await queue.put(await mkQueueItem(fn, title))
				bu.debug('File added to queue: ' + fn)
			elif os.path.isdir(fn):
				with os.scandir(fn) as dirEntry:
					for entry in dirEntry:
						try:
							bu.debug('Found: ' + entry.name)
							if entry.is_file() and (p_replayfile.match(entry.name) != None): 
								bu.debug(entry.name)
								await queue.put(await mkQueueItem(entry.path, title))
								bu.debug('File added to queue: ' + entry.path)
						except Exception as err:
							bu.error(str(err))
			else:
				bu.error('File not found: ' + fn)
			
	bu.debug('Finished')
	return None


async def mkQueueItem(filename : str, title : str) -> list:
	"""Make an item to replay queue"""
	global REPLAY_N
	REPLAY_N +=1
	return [filename, REPLAY_N, getTitle(filename, title, REPLAY_N)]


async def replayWorker(queue: asyncio.Queue, workerID: int, account_id: int, priv = False):
	"""Async Worker to process the replay queue"""
	global SKIPPED_N
	global ERROR_N

	while True:
		item = await queue.get()
		filename = item[0]
		N = item[1]
		title = item[2]

		replay_json_fn = filename +  '.json'
		msg_str = 'Replay[' + str(N) + ']: '

		#bu.debug(msg_str + replay_json_fn)
		try:
			if os.path.isfile(replay_json_fn):
				async with aiofiles.open(replay_json_fn) as fp:
					replay_json = json.loads(await fp.read())
					#if replay_json['status'] == 'ok':
					if wi.chk_JSON_replay(replay_json):
						bu.verbose_std(msg_str + title + ' has already been posted. Skipping.' )
					else:
						os.remove(replay_json_fn)
						bu.debug(msg_str + "Replay JSON not valid: Deleting " + replay_json_fn, id=workerID)
					SKIPPED_N += 1						
					queue.task_done()						
					continue

		except asyncio.CancelledError as err:
			raise err
		except Exception as err:
			bu.error(msg_str + 'Unexpected error: ' + str(type(err)) + ' : '+ str(err))
		try:
			#bu.debug('Opening file [' + str(N) +']: ' + filename)
			async with aiofiles.open(filename,'rb') as fp:
				filename = os.path.basename(filename)
				bu.debug(msg_str + 'File:  ' + filename)
				json_resp = await wi.post_replay(await fp.read(), filename, account_id, title, priv, N)
				if json_resp != None:
					if (await bu.save_JSON(replay_json_fn,json_resp)):
						if wi.chk_JSON_replay(json_resp):
							if not bu.debug(msg_str + 'Replay saved OK: ' + filename):
								bu.verbose_std(msg_str + title + ' posted')
						else:
							bu.warning(msg_str +'Replay file is not valid: ' + filename)
							ERROR_N += 1
					else:
						bu.error(msg_str + 'Error saving replay: ' + filename)
						ERROR_N += 1
				else:
					bu.error(msg_str + 'Replay file is not valid: ' + filename)
					ERROR_N += 1	
		except Exception as err:
			bu.error(msg_str + 'Unexpected Exception: ' + str(type(err)) + ' : ' + str(err) )
		bu.debug(msg_str + 'Marking task done')
		queue.task_done()	

	return None


def getTitle_old(replayfile: str, title: str, i : int) -> str:
	global wg

	if title == None:
		try:
			filename = os.path.basename(replayfile)	
			bu.debug(filename)
			map_usrStrs = wg.get_map_user_strs()
			tank_userStrs = wg.get_tank_user_strs()
			
			#p = re.compile('\\d{8}_\\d{4}_(.+)_(' + '|'.join(map_usrStrs) + ')(?:-\\d)?\\.wotbreplay$')
			# update 6.2 changed the file name format. Bug fixed 2019-09-09 Jylpah
			p = re.compile('\\d{8}_\\d{4}_.*?(' + '|'.join(tank_userStrs) + ')_(' + '|'.join(map_usrStrs) + ')(?:-\\d)?\\.wotbreplay$')
			
			m = p.match(filename)
			if (m != None):
				if wg.tanks != None:
					tank = m.group(1)
					if tank in wg.tanks['userStr']:
						tank = wg.tanks['userStr'][tank]
					else:
						bu.error('Tank code: "' + tank + '" not found from Tankopedia (tanks.json)')
				else:
					tank = m.group(1)
				blitz_map = m.group(2)
				if blitz_map in wg.maps:
					blitz_map = wg.maps[blitz_map]
				else:
					bu.error('Mapcode: "' + blitz_map + '" not found from map database (maps.json)')
				title = tank + ' @ ' + blitz_map
			else:
				title = re.sub('\\.wotbreplay$', '', filename)
		except Exception as err:
			bu.error(err)
	else:
		title.replace('NN', str(i))	
	return title 


def getTitle(replayfile: str, title: str, i : int) -> str:
	global wg
	if title == None:
		try:
			filename = os.path.basename(replayfile)	
			bu.debug(filename)
			player = None
			tank = None
			map_name = None

			with zipfile.ZipFile(replayfile, 'r') as archive:
				# bu.debug('Replay file: ' + replayfile + ' opened')
				with io.TextIOWrapper(archive.open('meta.json')) as meta:
					# bu.debug('Replay file\'s metadata: opened')
					try:
						metadata_json = json.load(meta)
						#player = metadata_json['playerName']
						tank = wg.get_tank_name(metadata_json['playerVehicleName'])
						map_name = wg.get_map_name(metadata_json['mapName'])
						bu.debug('Tank: ' + tank +  ' Map: ' + map_name)
					except Exception as err:
						bu.error(exception = err)
			if (tank != None) and  (map_name != None):
				title = tank + ' @ ' + map_name
			else:
				title = re.sub('\\.wotbreplay$', '', filename)
		except Exception as err:
			bu.error(err)
	else:
		title.replace('NN', str(i))	
	bu.debug('Returning: '  + title)
	return title 

### main() -------------------------------------------
if __name__ == "__main__":
	# To avoid 'Event loop is closed' RuntimeError due to compatibility issue with aiohttp
	if sys.platform.startswith("win") and sys.version_info >= (3, 8):
		try:
			from asyncio import WindowsSelectorEventLoopPolicy
		except ImportError:
			pass
		else:
			if not isinstance(asyncio.get_event_loop_policy(), WindowsSelectorEventLoopPolicy):
				asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())
	asyncio.run(main(sys.argv[1:]), debug=False)
