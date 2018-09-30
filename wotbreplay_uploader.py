#!/usr/bin/python3.7

import json, argparse, inspect, sys, os, base64, aiohttp, urllib, asyncio, aiofiles, aioconsole, logging, re

logging.getLogger("asyncio").setLevel(logging.DEBUG)

# WG account id of the uploader: 
# # Find it here: https://developers.wargaming.net/reference/all/wotb/account/list/
WG_ID = USE_YOUR_OWN_WG_ID

DEBUG = False
VERBOSE = False
MAX_RETRIES = 3
SLEEP = 2

TANKS = None
MAPS ={
	'amigosville' 	: 'Falls Creek',
	'canal'			: 'Canal',
	'canyon' 		: 'Canyon',
	'desert_train' 	: 'Desert Sands',
	'erlenberg'		: 'Middleburg',
	'fort'			: 'Fort Despair',
	'grossberg'		: 'Dynasty\'s Pearl',
	'faust'			: 'Faust',
	'himmelsdorf' 	: 'Himmelsdorf',
	'italy'			: 'Vineyeards',
	'karelia'		: 'Rockfield',
	'milbase' 		: 'Yamato Harbor',
	'mountain'		: 'Black Goldville',
	'port'			: 'Port Bay',
	'medvedkovo' 	: 'Dead Rail',
	'pliego'		: 'Castilla',
	'rock'			: 'Mayan Ruins',
	'rudniki'		: 'Mines',
	'savanna'		: 'Oasis Palms',
	'skit'			: 'Naval Frontier'
	}

REPLAY_N = 0

#WIurl='https://wotinspector.com/api/replay/upload?&details=full&uploaded_by='
WIurl='https://wotinspector.com/api/replay/upload?'
# additional option: 'private=1' to keep the replay way from public listing at WoTinspector.com/replays 


async def main(argv):
	global VERBOSE, DEBUG, TANKS

	parser = argparse.ArgumentParser(description='Post replays(s) to WoTinspector.com and retrieve battle data')
	parser.add_argument('--output', default='single', choices=['file', 'files', 'db'] , help='Select output mode: single/multiple files or database')
	parser.add_argument('-id', dest='accountID', type=int, default=WG_ID, help='WG account_id')
	parser.add_argument('-t','--title', type=str, default=None, help='Title for replays. Use NN for continous numbering. Default is filename-based numbering')
	parser.add_argument('-p', '--private', dest="private", action='store_true', default=False, help='Set replays private on WoTinspector.com')
	parser.add_argument('--tasks', dest='N_tasks', type=int, default=10, help='Number of worker threads')
	parser.add_argument('--tankopedia', type=str, default=None, help='Open Tankopedia')
	parser.add_argument('-d', '--debug', action='store_true', default=False, help='Debug mode')
	parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Verbose mode')
	parser.add_argument('files', metavar='FILE1 [FILE2 ...]', type=str, nargs='+', help='Files to read. Use \'-\' for STDIN"')
	args = parser.parse_args()

	print(args)
	VERBOSE = args.verbose
	DEBUG = args.debug
	if DEBUG: VERBOSE = True

	if args.tankopedia != None:
		with open(args.tankopedia, 'r', encoding='utf8') as f:
			TANKS = json.load(f)

	try:
		queue  = asyncio.Queue(maxsize=10)	
		
		tasks = []
		tasks.append(asyncio.create_task(mkReplayQ(queue, args.files, args.title)))
		# mkReplayQ(queue, args.files, args.title)
		for i in range(args.N_tasks):
			tasks.append(asyncio.create_task(replayWorker(queue, i, args.accountID, args.private)))
			debug('Task ' + str(i) + ' started')
		debug('Waiting for the replay scanner to finish')
		await asyncio.wait([tasks[0]])
		debug('Scanner finished. Waiting for workers to finish queue')
		await queue.join()
		debug('Cancelling workers')
		for task in tasks:
			task.cancel()
		debug('Waiting for workers to cancel')
		await asyncio.gather(*tasks, return_exceptions=True)
		verbose(str(REPLAY_N) + ' replays uploaded')
		# tanklist = []
		# for tanklist_tmp in await asyncio.gather(*tasks):
		# 	tanklist.extend(tanklist_tmp)
			
		# with open(args.file, 'w+') as outfile:
		# 	tankopedia = {}
		
	except KeyboardInterrupt:
		print('Ctrl-c pressed ...')
		sys.exit(1)

	return None


async def mkReplayQ(queue : asyncio.Queue, files : list, title : str):
	"""Create queue of replays to post"""
	p_replayfile = re.compile('.*\\.wotbreplay$')
	if files[0] == '-':
		debug('reading replay file list from STDIN')
		stdin, _ = await aioconsole.get_standard_streams()
		#stdin = sys.stdin
		while True:
			line = stdin.readline()
			if not line: 
				break
			else:
				if (p_replayfile.match(line) != None):
					await queue.put(mkQueueItem(line, title))
	else:
		# debug('Reading files from the command line: ')
		# debug(', '.join(files))
		for fn in files:
			# debug(fn)
			if os.path.isfile(fn) and (p_replayfile.match(fn) != None):
				await queue.put(mkQueueItem(fn, title))
			elif os.path.isdir(fn):
				with os.scandir(fn) as dirEntry:
					for entry in dirEntry:
						if entry.is_file() and (p_replayfile.match(entry.name) != None): 
							debug(entry.name)
							await queue.put(mkQueueItem(entry.path, title))
			debug('File added to queue: ' + fn)
	debug('Finished')
	return None


def mkQueueItem(filename : str, title : str) -> list:
	"""Make an item to replay queue"""
	global REPLAY_N
	REPLAY_N +=1
	return [filename, REPLAY_N, getTitle(filename, title, REPLAY_N)]


async def replayWorker(queue: asyncio.Queue, workerID: int, account_id: int, priv = False):
	async with aiohttp.ClientSession() as session:
		while True:
			item = await queue.get()
			filename = item[0]
			# debug(filename)
			N = item[1]
			title = item[2]
			# debug('Opening file [' + str(N) +']: ' + filename)
			async with aiofiles.open(filename,'rb') as fp:
				# debug('File opened: ' + filename)
				filename = os.path.basename(filename)
				params = { 
					'title'			: title, 
					'private' 		: (1 if priv else 0),
					'uploaded_by'	: account_id,
					'details'		: 'full'
					}
				url = WIurl + urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
				headers ={'Content-type':  'application/x-www-form-urlencoded'}
				# debug('Reading the payload')
				payload = { 'file' : (filename, base64.b64encode(await fp.read())) }
				# debug('Payload read')
				# debug('URL: ' + url)
				for retry in range(MAX_RETRIES):
					# verbose('Worker ' + str(workerID) + ': Posting replay[' + str(N) +']:  ' + title + ' Try #: ' + str(retry+ 1) + '/' + str(MAX_RETRIES) )
					verbose('Posting replay[' + str(N) +']:  ' + title + ' Try #: ' + str(retry+ 1) + '/' + str(MAX_RETRIES) )
					try:
						async with session.post(url, headers=headers, data=payload) as resp:
							debug('Replay #' + str(N) + ' HTTP response: '+ str(resp.status))
							if resp.status == 200:								
								debug('HTTP POST 200 = Success. Reading response data')
								json_resp = await resp.json()
								if json_resp['status'] == 'error':
									error(json_resp['error']['message'] + ' : Replay[' + str(N) + ']' + title)
								else:
									debug('Response data read')
									verbose('Replay[' + str(N) + ']: ' + title + ' posted')
								await storeReplayJSON(filename,json_resp)												
								break
							else:
								debug('Replay[' + str(N)+ ']: Got HTTP/' + str(resp.status) + ' Retrying... ' + str(retry))
								await asyncio.sleep(SLEEP)								
					except Exception as err:
						error(str(err))
						await asyncio.sleep(SLEEP)	
			debug('Marking task ' + str(N) + ' done')
			queue.task_done()	

	return None


async def storeReplayJSON(filename: str, json_data : dict):
	filename = filename + '.json'
	try: 
		async with aiofiles.open(filename, 'w', encoding='utf8') as outfile:
			await outfile.write(json.dumps(json_data, ensure_ascii=False, indent=4))
			return True
	except Exception as err:
		error(str(err))
		return False
	return False


def getTitle(replayfile: str, title: str, i : int) -> str:
	
	if title == None:
		try:
			filename = os.path.basename(replayfile)	
			# debug(filename)
			#p = re.compile('(\\d{4})(\\d{2})(\\d{2})_(\\d{2})(\\d{2})_(.+)_(.+?)(?:-\\d)\\.wotbreplay')
			p = re.compile('\\d{8}_\\d{4}_(.+)_(' + '|'.join(MAPS.keys()) + ')(?:-\\d)?\\.wotbreplay$')
			m = p.match(filename)
			if (m != None):
				# debug('Match')
				if TANKS != None:
					tank = TANKS['userStr'][m.group(1)]
					# debug('Tank from Tankopedia:' + tank)
				else:
					tank = m.group(1)
					# debug(tank)
				blitz_map = MAPS[m.group(2)]
				title = tank + ' @ ' + blitz_map
			else:
				# debug('No match')
				title = re.sub('\\.wotbreplay$', '', filename)
		except Exception as err:
			debug(err)
	else:
		title.replace('NN', str(i))
	
	# debug(title)
	return title 



# with open(filename,'r') as f:
# 	payload = {'file' : (filename, base64.b64encode(f.read())) }
# 	r = requests.post(url, data=payload)
# 	print(r.json())



def verbose(msg = ""):
    """Print a message"""
    if VERBOSE:
        print(msg)
    return None


def debug(msg = ""):
    """print a conditional debug message"""
    if DEBUG: 
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        caller = calframe[1][3]
        print('DEBUG: ' + caller + '(): ' + msg)
    return None


def error(msg = ""):
    """Print an error message"""
    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe, 2)
    caller = calframe[1][3]
    print('ERROR: ' + caller + '(): ' + msg)
    
    return None

### main()
if __name__ == "__main__":
   asyncio.run(main(sys.argv[1:]))
