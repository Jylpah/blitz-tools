#!/usr/bin/python3.7

import json, argparse, inspect, sys, os, base64, aiohttp, urllib, asyncio, aiofiles, aioconsole
import logging, re, concurrent.futures
import blitzutils as bu
from blitzutils import WG
from blitzutils import WoTinspector

logging.getLogger("asyncio").setLevel(logging.DEBUG)

# WG account id of the uploader: 
# # Find it here: https://developers.wargaming.net/reference/all/wotb/account/list/
WG_ID = 0

DEBUG = False
VERBOSE = False
SLEEP = 2
MAX_RETRIES = 3
REPLAY_N = 0
SKIPPED_N = 0
WIurl='https://wotinspector.com/api/replay/upload?'
WG_appID  = '81381d3f45fa4aa75b78a7198eb216ad'
wg = None
wi = None


async def main(argv):
	global wg, wi

	parser = argparse.ArgumentParser(description='Post replays(s) to WoTinspector.com and retrieve battle data')
	parser.add_argument('--output', default='single', choices=['file', 'files', 'db'] , help='Select output mode: single/multiple files or database')
	parser.add_argument('-id', dest='accountID', type=int, default=WG_ID, help='WG account_id')
	parser.add_argument('-a', '--account', dest='account', type=str, default=None, help='Uploader\'s WG account name. Format: ACCOUNT_NAME@SERVER')
	parser.add_argument('-t','--title', type=str, default=None, help='Title for replays. Use NN for continous numbering. Default is filename-based numbering')
	parser.add_argument('-p', '--private', dest="private", action='store_true', default=False, help='Set replays private on WoTinspector.com')
	parser.add_argument('--tasks', dest='N_tasks', type=int, default=10, help='Number of worker threads')
	parser.add_argument('--tankopedia', type=str, default='tanks.json', help='JSON file to read Tankopedia from. Default: "tanks.json"')
	parser.add_argument('--mapfile', type=str, default='maps.json', help='JSON file to read Blitz map names from. Default: "maps.json"')
	parser.add_argument('-d', '--debug', action='store_true', default=False, help='Debug mode')
	parser.add_argument('-v', '--verbose', action='store_true', default=True, help='Verbose mode')
	parser.add_argument('-s', '--silent', action='store_true', default=False, help='Silent mode')	
	parser.add_argument('files', metavar='FILE1 [FILE2 ...]', type=str, nargs='+', help='Files to read. Use \'-\' for STDIN"')
	args = parser.parse_args()

	bu.setVerbose(args.verbose)
	bu.setDebug(args.debug)
	if args.silent:
		bu.setVerbose(False)    	

	wg = WG(WG_appID, args.tankopedia, args.mapfile)
	wi = WoTinspector()

	if args.account != None:
		args.accountID = await wg.getAccountID(args.account)
		bu.debug('WG  account_id: ' + str(args.accountID))

	if args.accountID == None: 
		args.accountID = 0

	try:
		queue  = asyncio.Queue()	
		
		tasks = []
		# Make replay Queue
		tasks.append(asyncio.create_task(mkReplayQ(queue, args.files, args.title)))
		# Start tasks to process the Queue
		for i in range(args.N_tasks):
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
		bu.verbose(str(REPLAY_N) + ' replays: ' + str(REPLAY_N - SKIPPED_N) + ' uploaded, ' + str(SKIPPED_N) + ' skipped')
				
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

	while True:
		item = await queue.get()
		filename = item[0]
		N = item[1]
		title = item[2]

		replay_json_fn = filename +  '.json'
		msg_str = 'Replay[' + str(N) + ']: '

		#bu.debug(msg_str + replay_json_fn)
		try:
			#if os.path.exists(replay_json_fn) and os.path.isfile(replay_json_fn):
			if os.path.isfile(replay_json_fn):
				async with aiofiles.open(replay_json_fn) as fp:
					replay_json = json.loads(await fp.read())
					#if replay_json['status'] == 'ok':
					if wi.chkJSONreplay(replay_json):
						bu.verbose(msg_str + title + ' has already been posted. Skipping.' )
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
				json_resp = await wi.postReplay(await fp.read(), filename, account_id, title, priv, N)
				if json_resp != None:
					if (await bu.saveJSON(replay_json_fn,json_resp)):
						bu.debug(msg_str + 'Replay saved OK: ' + filename )
					else:
						bu.error(msg_str + 'Error saving replay: ' + filename)
		except Exception as err:
			bu.error(msg_str + 'Unexpected Exception: ' + str(type(err)) + ' : ' + str(err) )
		bu.debug(msg_str + 'Marking task done')
		queue.task_done()	

	return None

def getTitle(replayfile: str, title: str, i : int) -> str:
	global wg

	if title == None:
		try:
			filename = os.path.basename(replayfile)	
			bu.debug(filename)
			map_usrStrs = wg.getMapUserStrs()
			p = re.compile('\\d{8}_\\d{4}_(.+)_(' + '|'.join(map_usrStrs) + ')(?:-\\d)?\\.wotbreplay$')
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


### main() -------------------------------------------
if __name__ == "__main__":
   asyncio.run(main(sys.argv[1:]), debug=False)