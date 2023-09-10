#!/usr/bin/env python3
## PYTHON VERSION MUST BE 3.7 OR HIGHER

import sys, os, json, time,  base64, urllib, inspect, hashlib, re, string, random
import asyncio, aiofiles, aiohttp, aiosqlite, lxml
from pathlib import Path
from bs4 import BeautifulSoup
from progress.bar import IncrementalBar
from progress.counter import Counter
from decimal import Decimal
from datetime import datetime

MAX_RETRIES = 3
SLEEP = 1.5

LOG_LEVELS = {'silent': 0, 'normal': 1, 'verbose': 2, 'debug': 3}
SILENT = 0
NORMAL = 1
VERBOSE = 2
DEBUG = 3
_log_level = NORMAL
LOG = False
LOGGER = None

MODE_TANK_STATS         = 'tank_stats'
MODE_PLAYER_STATS       = 'player_stats'
MODE_PLAYER_ACHIEVEMENTS= 'player_achievements'
MODE_TANKOPEDIA			= 'tankopedia'

# Progress display
_progress_N = 100
_progress_i = 0
_progress_id = None
_progress_obj = None

UMASK = os.umask(0)
os.umask(UMASK)

## -----------------------------------------------------------
#### Class ThrottledClientSession(aiohttp.ClientSession)
#
#  Rate-limited async http client session
#
#  Inherits aiohttp.ClientSession 
## -----------------------------------------------------------

from typing import Optional, Union
import aiohttp
import asyncio
import time
import logging
import re

class ThrottledClientSession(aiohttp.ClientSession):
	"""Rate-throttled client session class inherited from aiohttp.ClientSession)""" 

	def __init__(self, rate_limit: float = 0, filters: list[str] = list() , 
				limit_filtered: bool = False, re_filter: bool = False, *args,**kwargs) -> None: 
		assert isinstance(rate_limit, (int, float)),   "rate_limit has to be float"
		assert isinstance(filters, list),       "filters has to be list"
		assert isinstance(limit_filtered, bool),"limit_filtered has to be bool"
		assert isinstance(re_filter, bool),     "re_filter has to be bool"

		super().__init__(*args,**kwargs)
		
		self.rate_limit     : float
		self._fillerTask    : Optional[asyncio.Task]    = None
		self._queue         : Optional[asyncio.Queue]   = None
		self._start_time    : float = time.time()
		self._count         : int = 0
		self._limit_filtered: bool = limit_filtered
		self._re_filter     : bool = re_filter
		self._filters       : list[Union[str, re.Pattern]] = list()

		if re_filter:
			for filter in filters:
				self._filters.append(re.compile(filter))
		else:
			for filter in filters:
				self._filters.append(filter)
		self.set_rate_limit(rate_limit)


	def _get_sleep(self) -> float:        
		if self.rate_limit > 0:
			return 1/self.rate_limit
		return 0


	def get_rate(self) -> float:
		"""Return rate of requests"""
		return self._count / (time.time() - self._start_time)


	def get_stats(self) -> dict[str, float]:
		"""Get session statistics"""
		res = {'rate' : self.get_rate(), 'rate_limit': self.rate_limit, 'count' : self._count }
		return res
		

	def get_stats_str(self) -> str:
		"""Print session statistics"""
		return f"rate limit: {str(self.rate_limit if self.rate_limit is not None else '-')} \
				rate:   {0:.1f}.format(self.get_rate()) requests: {str(self._count)}"


	def reset_counters(self) -> dict[str, float]:
		"""Reset rate counters and return current results"""
		res = self.get_stats()
		self._start_time = time.time()
		self._count = 0
		return res


	def set_rate_limit(self, rate_limit: float = 0) -> float:
		assert rate_limit is not None, "rate_limit must not be None" 
		assert isinstance(rate_limit, (int,float)) and rate_limit >= 0, "rate_limit has to be type of 'float' >= 0"
		
		self.rate_limit = rate_limit
		if rate_limit > 0:
			self._queue     = asyncio.Queue(int(rate_limit)+1) 
			if self._fillerTask is not None: 
				self._fillerTask.cancel()  
			self._fillerTask = asyncio.create_task(self._filler())
		return self.rate_limit
		

	async def close(self) -> None:
		"""Close rate-limiter's "bucket filler" task"""
		# DEBUG 
		logging.debug(self.get_stats_str())
		try:
			if self._fillerTask is not None:
				self._fillerTask.cancel()
				await asyncio.wait_for(self._fillerTask, timeout=0.5)
		except asyncio.TimeoutError as err:
			logging.error(str(err))
		await super().close()

	
	async def _filler(self) -> None:
		"""Filler task to fill the leaky bucket algo"""
		try:
			if self._queue is None:
				return None            
			logging.debug('SLEEP: ' + str(self._get_sleep()))
			updated_at = time.monotonic()
			extra_increment : float = 0
			for i in range(0, self._queue.maxsize):
				await self._queue.put(i)
			while True:
				if not self._queue.full():
					now = time.monotonic()
					increment = self.rate_limit * (now - updated_at)
					items_2_add = int(min(self._queue.maxsize - self._queue.qsize(), int(increment + extra_increment)))
					extra_increment = (increment + extra_increment) % 1
					for i in range(0,items_2_add):
						self._queue.put_nowait(i)
					updated_at = now
				await asyncio.sleep(self._get_sleep())
		except asyncio.CancelledError:
			logging.debug('Cancelled')
		except Exception as err:
			logging.error(str(err))
		return None


	async def _request(self, *args,**kwargs) -> aiohttp.ClientResponse:
		"""Throttled _request()"""
		if self._queue is not None and self.is_limited(*args):  
			await self._queue.get()
			self._queue.task_done()
		self._count += 1
		return await super()._request(*args,**kwargs)


	def is_limited(self, *args: str) -> bool:
		"""Check wether the rate limit should be applied"""
		try:
			url: str = args[1]
			for filter in self._filters:
				if isinstance(filter, re.Pattern) and filter.match(url) is not None:
					return self._limit_filtered
				elif isinstance(filter, str) and url.startswith(filter):
					return self._limit_filtered
					
			return not self._limit_filtered
		except Exception as err:
			logging.error(str(err))
		return True    

# -----------------------------------------------------------
# Class AsyncLogger()
# -----------------------------------------------------------

class AsyncLogger():
	"""Async file logger"""

	def __init__(self) -> None:
		self._queue = asyncio.Queue()
		self._task = None
		self._file = None

	async def open(self, logfn: str = None):
		"""Set logging to file"""
		if logfn is None:
			logfn = 'LOG_' + _randomword(6) + '.log'
		try:
			self._file = await aiofiles.open(logfn, mode='a')
			self._task = asyncio.create_task(self.logger())
			return True
		except Exception as err:
			error('Error opening file: ' + logfn, err)
			self._file = None
		return False

	async def logger(self):
		"""Async file logger"""
		if self._file is None:
			error('No log file defined')
			return False
		while True:
			try:
				msg = await self._queue.get()
				await self._file.write(msg + '\n')
				self._queue.task_done()
			except asyncio.CancelledError as err:
				return None
			except Exception as err:
				error(exception=err)

	def log(self, msg: str = ''):
		self._queue.put_nowait(msg)

	async def close(self):
		try:
			# empty queue & close
			await self._queue.join()
			self._task.cancel()
			self._file.close()
		except Exception as err:
			error('Error closing log file', err)
		return None

# -----------------------------------------------------------
# Utils
# -----------------------------------------------------------


def set_debug(debug: bool):
	global _log_level
	_log_level = DEBUG


def set_verbose(verbose: bool):
	global _log_level
	_log_level = VERBOSE


def set_silent(silent: bool):
	global _log_level
	_log_level = SILENT


def set_log_level_normal():
	global _log_level
	_log_level = NORMAL


def is_debug() -> bool:
	return _log_level == DEBUG


def is_verbose() -> bool:
	return _log_level == VERBOSE


def is_normal() -> bool:
	return _log_level == NORMAL


def is_silent() -> bool:
	return _log_level == SILENT


def set_log_level(silent: bool, verbose: bool, debug: bool):
	global _log_level
	_log_level = NORMAL
	if silent:  _log_level = SILENT
	if verbose: _log_level = VERBOSE
	if debug:   _log_level = DEBUG


def get_log_level() -> int:
	return _log_level


def get_log_level_str() -> str:
	for log_level in LOG_LEVELS:
		if _log_level == LOG_LEVELS[log_level]:
			return log_level
	error('Unknown log level: ' + str(_log_level))


async def set_file_logging(logfn=None, add_timestamp=False):
	"""Set logging to file"""
	global LOG, LOGGER
	LOG = True
	if logfn is None:
		logfn = 'LOG_' + _randomword(6)
	else:
		dateTimeObj = datetime.now()
		timestampStr = dateTimeObj.strftime("%y%m%d%H%M%S")
		logfn = logfn + '_' + timestampStr
	logfn = logfn + '.log'
	try:
		LOGGER = AsyncLogger()
		await LOGGER.open(logfn)
	except Exception as err:
		error('Error starting logger: ' + logfn, err)
		LOG = False
		LOGGER = None
	return LOG


async def close_file_logging():
	global LOG, LOGGER
	LOG = False
	await LOGGER.close()
	LOGGER = None


def _randomword(length):
   letters = string.ascii_lowercase
   return ''.join(random.choice(letters) for i in range(length))


def verbose(msg="", id=None) -> bool:
	"""Print a message"""
	return _print_log_msg('', msg, exception=None, id=id, print_msg=(_log_level >= VERBOSE))


def verbose_std(msg="", id=None) -> bool:
	"""Print a message"""
	return _print_log_msg('', msg, exception=None, id=id, print_msg=(_log_level >= NORMAL))


def warning(msg="", id=None, force: bool = False) -> bool:
	"""Print a warning message"""
	return _print_log_msg('', 'Warning: ' + msg, None, id, print_msg=(force or (_log_level >= NORMAL)))


def debug(msg="", id=None, exception=None, force: bool = False) -> bool:
	"""print a conditional debug message"""
	if (_log_level >= DEBUG) or force:
		return _print_log_msg('DEBUG', msg, exception, id)
	return False


def error(msg="", exception=None, id=None) -> bool:
	"""Print an error message"""
	return _print_log_msg('ERROR', msg, exception, id)


def log(msg="", id=None, exception=None) -> bool:
	"""print a conditional debug message"""
	return _print_log_msg('LOG', msg=msg, exception=exception, id=id, print_msg=(_log_level >= DEBUG))


## Copy with pride: https://stackoverflow.com/questions/2203424/python-how-to-retrieve-class-information-from-a-frame-object
def get_class_from_frame(fr):
	args, _, _, value_dict = inspect.getargvalues(fr)
	# we check the first parameter for the frame function is
	# named 'self'
	if len(args) and args[0] == 'self':
		# in that case, 'self' will be referenced in value_dict
		instance = value_dict.get('self', None)
		if instance:
			# return its class
			return getattr(instance, '__class__', None)
	# return None otherwise
	return None


def _print_log_msg(prefix: str = 'LOG', msg:str = '', 
					exception: Exception | None = None, 
					id: int|None = None, print_msg : bool = True):
	# Use empty prefix to determine standard verbose messages
	if not (print_msg or LOG):
		return False
	retval = False
	if prefix != '':
		curframe = inspect.currentframe()
		calframe = inspect.getouterframes(curframe)
		class_name = get_class_from_frame(curframe)
		caller = calframe[2].function
		if class_name is not None:
			prefix = f'{prefix}: {class_name}.{caller}'
		else:
			prefix = prefix + ': ' + caller
		prefix = prefix + '(): '
	
	if id is not None:
		prefix = f'{prefix} [ {id}]: '

	exception_msg = ''
	if (exception is not None) and isinstance(exception, Exception):
		exception_msg = f' : Exception: {type(exception)} : {exception}'

	msg = f'{prefix}{msg}{exception_msg}'
	if print_msg: 
		print(msg)
		retval = True
	if _log_msg(msg):
		retval = True
	return retval


def _log_msg(msg :str=''):
	if LOG and (LOGGER is not None):
		LOGGER.log(msg)        
		return True
	return False


def set_progress_step(n: int):
	"""Set the frequency of the progress dots. The bigger 'n', the fewer dots"""
	global _progress_N, _progress_i 
	if n > 0:
		_progress_N = n
		_progress_i = 0
	return


def get_progress_step():
	"""Get the frequency of the progress dots. The bigger 'n', the fewer dots"""
	return _progress_N


def set_progress_bar(heading: str, max_value: int, step: int = None, slow: bool = False, id: str = None):
	global _progress_obj, _progress_N, _progress_i, _progress_id
	_progress_id = id
	if step is None:
		_progress_N = int(max_value / 1000) if (max_value > 1000) else 2
	else:
		_progress_N = step
	if _progress_obj is not None:
		finish_progress_bar()
	if slow:
		_progress_obj = SlowBar(heading, max=max_value)
	else:
		_progress_obj = IncrementalBar(heading, max=max_value, suffix='%(index)d/%(max)d %(percent)d%%')
	_progress_i = 0

	_log_msg(heading + str(max_value))
	return


def set_counter(heading: str, step = None):
	global _progress_obj, _progress_i
	_progress_i = 0
	if _progress_obj is not None:
		finish_progress_bar()
	_progress_obj = Counter(heading + ': ')
	if step is not None:
		set_progress_step(step)
	return 


def print_progress(force = False, id : str = None) -> bool:
	"""Print progress bar/dots. Returns True if the dot is being printed."""
	global _progress_i
	
	_progress_i +=  1 
	if ((_progress_i % _progress_N) == 0):
		if (_log_level > SILENT) and ( force or (_log_level < DEBUG ) ):
			if (_progress_obj is not None):
				if (_progress_id == id):
					_progress_obj.next(_progress_N)
					return True
				else:
					return False
			else:
				print('.', end='', flush=True)
				return True
	return False    


def finish_progress_bar():
	"""Finish and close progress bar object"""
	global _progress_obj

	# print_nl = True
	if _progress_obj is not None:
		# if isinstance(_progress_obj, Counter):
		#     print_nl = False
		_progress_obj.finish()
		# if print_nl:
		print_new_line()
	_progress_obj = None
	return None


def wait(sec : int):
	for i in range(0, sec): 
	   i=i   ## to get rid of the warning... 
	   time.sleep(1)
	   print_progress(True)
	print('', flush=True)  


def print_new_line(force = False):
	if (_log_level > SILENT) and ( force or (_log_level < DEBUG ) ):
		print('', flush=True)
		_log_msg('')


def NOW() -> int:
	return int(time.time())


def get_date_str(timestamp:int = NOW(), date_format = '%Y%m%d_%H%M%S'):
	"""Return YYYYMMDD_HHmm date string"""
	try:
		ts = datetime.fromtimestamp(timestamp)
		return ts.strftime(date_format)
	except Exception as err:
		error(exception=err)
	return None


def rebase_file_args(current_dir : str, files: list[str]) -> list[str]:
	"""Rebase file command line params after moving working dir to the script's dir""" 
	assert isinstance(files, list), "files has to be a list"

	if (files[0] == '-') or (files[0] == 'db:'):
		return [ files[0] ] 
	else:
		return [ os.path.join(current_dir, fn) for fn in files ]


async def read_int_list(filename: str) -> list[int]:
	"""Read file to a list and return list of integers in the input file"""
	
	input_list = []
	try:
		async with aiofiles.open(filename) as fp:
			async for line in fp:
				try:
					input_list.append(int(line))
				except (ValueError, TypeError) as err:
					pass
	except Exception as err:
		error('Unexpected error when reading file: ' + filename, err)
	return input_list


async def save_JSON(filename: str, json_data: dict, sort_keys = False, pretty = True) -> bool:
	"""Save JSON data into file"""
	try:
		dirname = os.path.dirname(filename)
		if (dirname != '') and not os.path.isdir(dirname):
			os.makedirs(dirname, 0o770-UMASK)
		async with aiofiles.open(filename,'w', encoding="utf8") as outfile:
			if pretty:
				await outfile.write(json.dumps(json_data, ensure_ascii=False, indent=4, sort_keys=sort_keys))
			else:
				await outfile.write(json.dumps(json_data, ensure_ascii=False, sort_keys=sort_keys))
			return True
	except Exception as err:
		error('Error saving JSON', err)
	return False


async def open_JSON(filename: str, chk_JSON_func = None) -> dict:
	try:
		async with aiofiles.open(filename) as fp:
			json_data = json.loads(await fp.read())
			if (chk_JSON_func is None):
				debug("JSON file content not checked: " + filename)
				return json_data                
			elif chk_JSON_func(json_data):
				debug("JSON File is valid: " + filename)
				return json_data
			else:
				debug('JSON File has invalid content: ' + filename)
	except Exception as err:
		error('Unexpected error when reading file: ' + filename, err)
	return None


async def get_url_JSON(session: aiohttp.ClientSession, url: str, chk_JSON_func = None, max_tries = MAX_RETRIES) -> dict:
		"""Retrieve (GET) an URL and return JSON object"""
		if session is None:
			error('Session must be initialized first')
			sys.exit(1)
		if url is None:
			return None
		
		# To avoid excessive use of servers            
		for retry in range(1,max_tries+1):
			try:
				async with session.get(url) as resp:
					if resp.status == 200:
						debug('HTTP request OK')
						json_resp = await resp.json()       
						if (chk_JSON_func is None) or chk_JSON_func(json_resp):
							# debug("Received valid JSON: " + str(json_resp))
							return json_resp
						# Sometimes WG API returns JSON error even a retry gives valid JSON
					elif resp.status == 407:
						json_resp_err = await resp.json()
						error('WG API returned 407: ' + json_resp_err['error']['message'])
					if retry == max_tries:                        
						break
					debug('Retrying URL [' + str(retry) + '/' +  str(max_tries) + ']: ' + url )
				await asyncio.sleep(SLEEP)    

			except aiohttp.ClientError as err:
				debug("Could not retrieve URL: " + url, exception=err)
			except asyncio.CancelledError as err:
				debug('Queue gets cancelled while still working.', exception=err)        
			except Exception as err:
				debug('Unexpected Exception', exception=err)
		debug("Could not retrieve URL: " + url)
		return None


def bld_dict_hierarcy(d : dict, key : str, value) -> dict:
	"""Build hierarcical dict based on multi-level key separated with  """
	try:
		key_hier = key.split('.')
		sub_key = key_hier.pop(0)
		if len(key_hier) == 0:
			d[sub_key] = value
		elif sub_key not in d:   
			d[sub_key] = bld_dict_hierarcy({}, '.'.join(key_hier), value)
		else:
			d[sub_key] = bld_dict_hierarcy(d[sub_key], '.'.join(key_hier), value)

		return d    
	except KeyError as err:
		error('Key not found', err)
	except Exception as err:
		error('Unexpected Exception', err)
	return None

def  get_JSON_keypath(keypath: str, key: str):
	if keypath is None:
		return key
	else:
		return '.'.join([keypath, key])

def get_JSON_value(json, key : str | None = None, keys : list | None = None, 
					keypath : str | None = None):
	if (keys is None):
		if key is not None:
			keys = key.split('.')
		else: 
			return json
	if len(keys) == 0:
		return json
	key = keys.pop(0)
	if type(json) == dict:
		if key in json:
			return get_JSON_value(json[key], keys=keys, keypath=get_JSON_keypath(keypath, key))
		else:
			raise KeyError('Key: '+ get_JSON_keypath(keypath, key) + ' not found')
	if type(json) == list:
		p = re.compile(r'^\[(\d+)\]$')
		m = p.match(key)
		if len(m.groups()) != 1:
			raise KeyError('Invalid key given: ' + get_JSON_keypath(keypath, key))
		ndx = m.group(1)
		try:
			return get_JSON_value(json[ndx], keys=keys, keypath=get_JSON_keypath(keypath, key))
		except IndexError:
			raise KeyError('JSON array index out of range: ' + get_JSON_keypath(keypath, key))
	raise KeyError('Key not found: ' + get_JSON_keypath(keypath, keys[0]))


def sort_dict(d: dict, number: bool = False) -> dict:
	"""Sort a dict by keys"""
	if number:
		return dict(sorted(d.items(), key=lambda item: int(item[0])))
	else:
		return dict(sorted(d.items(), key=lambda item: str(item[0])))


# -----------------------------------------------------------
# Class SlowBar 
# -----------------------------------------------------------

class SlowBar(IncrementalBar):
	suffix = '%(index)d/%(max)d %(percent)d%% ETA %(remaining_hours).0f h %(remaining_mins).0f mins'
	@property
	def remaining_hours(self):
		return self.eta // 3600

	@property
	def remaining_mins(self):
		return (self.eta - (self.eta // 3600)*3600) // 60
 

# -----------------------------------------------------------
# Class StatsNotFound 
# -----------------------------------------------------------

class StatsNotFound(Exception):
	pass


# -----------------------------------------------------------
# Class WG 
# -----------------------------------------------------------

class WG:

	URL_WG_CLAN_INFO         = 'clans/info/?application_id='
	# URL_WG_PLAYER_TANK_LIST   = 'tanks/stats/?fields=tank_id%2Clast_battle_time&application_id='
	# URL_WG_PLAYER_TANK_LIST   = 'tanks/stats/?fields=account_id%2Ctank_id%2Clast_battle_time%2Cbattle_life_time%2Call&application_id='
	URL_WG_PLAYER_TANK_STATS  = 'tanks/stats/?application_id='
	URL_WG_ACCOUNT_ID        = 'account/list/?fields=account_id%2Cnickname&application_id='
	URL_WG_PLAYER_STATS      = 'account/info/?application_id='
	URL_WG_PLAYER_ACHIEVEMENTS = 'account/achievements/?application_id='
	CACHE_DB_FILE           = '.blitzutils_cache.sqlite3' 
	CACHE_GRACE_TIME        =  30*24*3600  # 30 days cache

	# sql_create_player_stats_tbl = """CREATE TABLE IF NOT EXISTS player_stats (
	#                             account_id INTEGER NOT NULL,
	#                             date INTEGER NOT NULL,
	#                             stat TEXT,
	#                             value FLOAT
	#                             ); """
	
	# sql_create_player_tank_stats_tbl = """CREATE TABLE IF NOT EXISTS player_tank_stats (
	#                         account_id INTEGER NOT NULL,
	#                         tank_id INTEGER DEFAULT NULL,
	#                         date INTEGER NOT NULL,
	#                         stat TEXT,
	#                         value FLOAT
	#                         ); """
	
	# sql_select_player_stats = """SELECT value FROM player_stats ORDERBY date ASC
	#                                 WHERE account_id = {} AND stat = {} 
	#                                 AND date >= {} LIMIT 1;"""
		  
	# TANK_STATS
	SQL_TANK_STATS_TBL          = 'tank_stats'

	SQL_TANK_STATS_CREATE_TBL   = 'CREATE TABLE IF NOT EXISTS ' + SQL_TANK_STATS_TBL + \
									""" ( account_id INTEGER NOT NULL, 
									tank_id INTEGER NOT NULL, 
									update_time INTEGER NOT NULL, 
									stats TEXT, 
									PRIMARY KEY (account_id, tank_id) )"""

	SQL_TANK_STATS_COUNT        = 'SELECT COUNT(*) FROM ' + SQL_TANK_STATS_TBL

	SQL_TANK_STATS_UPDATE       = 'REPLACE INTO ' + SQL_TANK_STATS_TBL + '(account_id, tank_id, update_time, stats) VALUES(?,?,?,?)'

	# PLAYER_STATS
	SQL_PLAYER_STATS_TBL        = 'player_stats'

	SQL_PLAYER_STATS_CREATE_TBL = 'CREATE TABLE IF NOT EXISTS ' + SQL_PLAYER_STATS_TBL + \
									""" ( account_id INTEGER PRIMARY KEY, 
									update_time INTEGER NOT NULL, 
									stats TEXT)"""

	SQL_PLAYER_STATS_COUNT       = 'SELECT COUNT(*) FROM ' + SQL_PLAYER_STATS_TBL

	SQL_PLAYER_STATS_UPDATE     = 'REPLACE INTO ' + SQL_PLAYER_STATS_TBL + '(account_id, update_time, stats) VALUES(?,?,?)'

	SQL_PLAYER_STATS_CACHED     = 'SELECT * FROM ' +  SQL_PLAYER_STATS_TBL + ' WHERE account_id = ? AND update_time > ?'

	# PLAYER_ACHIEVEMENTS
	SQL_PLAYER_ACHIEVEMENTS_TBL  = 'player_achievements'

	SQL_PLAYER_ACHIEVEMENTS_CREATE_TBL = 'CREATE TABLE IF NOT EXISTS ' + SQL_PLAYER_ACHIEVEMENTS_TBL + \
									""" ( account_id INTEGER PRIMARY KEY, 
									update_time INTEGER NOT NULL, 
									stats TEXT)"""

	SQL_PLAYER_ACHIEVEMENTS_COUNT      = 'SELECT COUNT(*) FROM ' + SQL_PLAYER_ACHIEVEMENTS_TBL
	
	SQL_PLAYER_ACHIEVEMENTS_CACHED     = 'SELECT * FROM ' +  SQL_PLAYER_ACHIEVEMENTS_TBL + ' WHERE account_id = ? AND update_time > ?'

	SQL_PLAYER_ACHIEVEMENTS_UPDATE     = 'REPLACE INTO ' + SQL_PLAYER_ACHIEVEMENTS_TBL + '(account_id, update_time, stats) VALUES(?,?,?)'

	
	SQL_TABLES                  = [ SQL_PLAYER_STATS_TBL, SQL_TANK_STATS_TBL, SQL_PLAYER_ACHIEVEMENTS_TBL ]

	SQL_CHECK_TABLE_EXITS       = """SELECT name FROM sqlite_master WHERE type='table' AND name=?"""

	SQL_PRUNE_CACHE             = """DELETE from {} WHERE update_time < {}""" 

# Default data. Please use the latest maps.json

	maps = {
		"Random": "Random map",
		"amigosville": "Falls Creek",
		"asia": "Lost Temple",
		"canal": "Canal",
		"canyon": "Canyon",
		"desert_train": "Desert Sands",
		"erlenberg": "Middleburg",
		"faust": "Faust",
		"fort": "Macragge",
		"grossberg": "Dynasty's Pearl",
		"himmelsdorf": "Himmelsdorf",
		"italy": "Vineyards",
		"karelia": "Rockfield",
		"karieri": "Copperfield",
		"lake": "Mirage",
		"lumber": "Alpenstadt",
		"malinovka": "Winter Malinovka",
		"medvedkovo": "Dead Rail",
		"milbase": "Yamato Harbor",
		"mountain": "Black Goldville",
		"north": "North",
		"ordeal": "Trial by Fire",
		"pliego": "Castilla",
		"port": "Port Bay",
		"rock": "Mayan Ruins",
		"rudniki": "Mines",
		"savanna": "Oasis Palms",
		"skit": "Naval Frontier",
		"test": "World of Ducks",
		"tutorial": "Proving Grounds"
	}

	tanks = None
	tanks_by_tier = None

	NATION = [ 'ussr', 'germany', 'usa', 'china', 'france', 'uk', 'japan', 'other', 'european']
	NATION_STR = [ 'Soviet', 'Germany', 'USA', 'China', 'France', 'UK', 'Japan', 'Other', 'European']    
	NATION_ID = {
		'ussr'      : 0,
		'germany'   : 1, 
		'usa'       : 2, 
		'china'     : 3,
		'france'    : 4,
		'uk'        : 5,
		'japan'     : 6,
		'other'     : 7,
		'european'  : 8
	}

	TANK_TYPE       = [ 'lightTank', 'mediumTank', 'heavyTank', 'AT-SPG' ]
	TANK_TYPE_STR   = [ 'Light Tank', 'Medium Tank', 'Heavy Tank', 'Tank Destroyer' ]
	
	TANK_TYPE_ID = {
		'lightTank'     : 0,
		'mediumTank'    : 1,
		'heavyTank'     : 2,
		'AT-SPG'        : 3
		}

	URL_WG_SERVER = {
		'eu'    : 'https://api.wotblitz.eu/wotb/',
		'ru'    : 'https://api.wotblitz.ru/wotb/',
		'na'    : 'https://api.wotblitz.com/wotb/',
		'asia'  : 'https://api.wotblitz.asia/wotb/',
		'china' : None
		}

	ACCOUNT_ID_SERVER= {
		'ru'    : range(0, int(5e8)),
		'eu'    : range(int(5e8), int(10e8)),
		'na'    : range(int(1e9),int(2e9)),
		'asia'  : range(int(2e9),int(31e8)),
		'china' : range(int(31e8),int(4e9))
		}

	def __init__(self, WG_app_id : str = None, tankopedia_fn : str =  None, maps_fn : str = None, 
				stats_cache: bool = False, rate_limit: int = 10, global_rate_limit = True):
		
		self.WG_app_id = WG_app_id
		self.load_tanks(tankopedia_fn)
		WG.tanks = self.tanks
		self.global_rate_limit = global_rate_limit
		
		if (maps_fn is not None):
			if os.path.exists(maps_fn) and os.path.isfile(maps_fn):
				try:
					with open(maps_fn, 'rt', encoding='utf8') as f:
						WG.maps = json.loads(f.read())
				except Exception as err:
					error('Could not read maps file: ' + maps_fn + '\n' + str(err))  
			else:
				verbose('Could not find maps file: ' + maps_fn)    
		if self.WG_app_id is not None:
			headers = {'Accept-Encoding': 'gzip, deflate'} 	
			if self.global_rate_limit:
				self.session = ThrottledClientSession(rate_limit=rate_limit, headers=headers)
			else:
				self.session = dict()
				for server in list(self.URL_WG_SERVER)[:4]:    # China (5th) server is unknown, thus excluded
					self.session[server] = ThrottledClientSession(rate_limit=rate_limit, headers=headers)
			debug('WG aiohttp session initiated')            
		else:
			self.session = None
			debug('WG aiohttp session NOT initiated')
		
		# cache
		self.cache = None
		self.statsQ = None
		self.stat_saver_task = None
		if stats_cache:
			try:
				self.statsQ = asyncio.Queue()
				self.stat_saver_task = asyncio.create_task(self.stat_saver())
			except Exception as err:
				error(exception=err)
				sys.exit(1)
	

	async def close(self):
		# close stats queue 
		try:
			if self.statsQ is not None:
				debug('WG.close(): Waiting for statsQ to finish')
				await self.statsQ.join()
				debug('WG.close(): statsQ finished')
				self.stat_saver_task.cancel()
				debug('statsCacheTask cancelled')
				await self.stat_saver_task 
				
			# close cacheDB
			if self.cache is not None:
				# prune old cache records
				await self.cleanup_cache()   
				await self.cache.commit()
				await self.cache.close()
			
			if self.session is not None:
				if self.global_rate_limit:
					await self.session.close()
				else:
					for server in self.session:
						await self.session[server].close()
		except Exception as err:
			error(exception=err)

		
	# Class methods  ----------------------------------------------------------

	@classmethod
	def get_server(cls, account_id: int) -> str:
		"""Get Realm/server of an account based on account ID"""
		if account_id >= 1e9:
			if account_id >= 31e8:
				debug('Chinese account/server: no stats available')
				return None
			if account_id >= 2e9:
				return 'asia'
			return 'na'
		else:
			if account_id < 5e8:
				return 'ru'
			return 'eu'
		return None


	@classmethod
	def update_maps(cls, map_data: dict):
		"""Update maps data"""
		cls.maps = map_data
		return None


	@classmethod
	def get_map(self, map_str: str) -> str:
		"""Return map name from short map string in replays"""
		try:
			return self.maps[map_str]
		except:
			error('Map ' + map_str + ' not found')
		return None
	

	@classmethod
	def get_tank_user_strs(self) -> str:
		return self.tanks["userStr"].keys()


	@classmethod
	def chk_JSON(cls, json_obj, check = None) -> bool:
		try:
			if (check is None): 
				# nothing to check
				return True
			if (check == 'tank_stats'):
				if cls.chk_JSON_tank_stats(json_obj):
					return True
				else: 
					debug('Checking tank list JSON failed.')
					return False
			elif (check == 'player_stats'):
				if cls.chk_JSON_player_stats(json_obj):
					return True
				else: 
					debug('Checking player JSON failed.')
					return False
			elif (check == 'tankopedia'):
				if cls.chk_JSON_tankopedia(json_obj):
					return True
				else:
					debug('Checking tank JSON failed.')
					return False
			elif (check == 'account_id'):
				if cls.chk_JSON_get_account_id(json_obj):
					return True
				else: 
					debug('Checking account_id JSON failed.')
					return False
		except (TypeError, ValueError) as err:
			debug(str(err))
		return False


	@classmethod
	def chk_JSON_status(cls, json_resp: dict) -> bool:
		try:
			if (json_resp is None) or ('status' not in json_resp) or (json_resp['status'] is None):
				return False
			elif (json_resp['status'] == 'ok') and ('data' in json_resp):
				return True
			elif json_resp['status'] == 'error':
				if ('error' in json_resp):
					error_msg = 'Received an error'
					if ('message' in json_resp['error']) and (json_resp['error']['message'] is not None):
						error_msg = error_msg + ': ' +  json_resp['error']['message']
					if ('value' in json_resp['error']) and (json_resp['error']['value'] is not None):
						error_msg = error_msg + ' Value: ' + json_resp['error']['value']
					debug(error_msg)
				return False
			else:
				error('Unknown status-code: ' + json_resp['status'])
				return False
		except KeyError as err:
			error('No field found in JSON data', err)
		except Exception as err:
			error("JSON format error", err)
		return False


	@classmethod
	def chk_JSON_get_account_id(cls, json_resp: dict) -> bool:
		try:
			if cls.chk_JSON_status(json_resp): 
				if (json_resp['meta']['count'] > 0):
					return True                
		except KeyError as err:
			error('Key not found', err)
		except Exception as err:
			error(exception=err)
		return False

	# @classmethod
	# def chkJSONplayer(cls, json_resp: dict) -> bool:
	#     """"Check String for being a valid Player JSON file"""
	#     try:
	#         if cls.chk_JSON_status(json_resp): 
	#             if int(json_resp[0]['account_id']) > 0:
	#                 return True
	#     except KeyError as err:
	#         error('Key not found', err)
	#     except:
	#         debug("JSON check failed")
	#     return False
	
	@classmethod
	def chk_JSON_tankopedia(cls, json_resp: dict) -> bool:
		""""Check String for being a valid Tankopedia JSON file"""
		try:
			if cls.chk_JSON_status(json_resp):
				if int(json_resp[0]['tank_id']) > 0:
					return True
		except KeyError as err:
			error('Key not found', err)
		except:
			debug("JSON check failed")
		return False
	

	@classmethod    
	def chk_JSON_player_stats(cls, json_resp: dict) -> bool:
		""""Check String for being a valid Tank JSON file"""
		try:
			if cls.chk_JSON_status(json_resp): 
				for acc in json_resp['data']:
					if json_resp['data'][acc] is not None:
						return True 
		except KeyError as err:
			error('Key not found', err)
		except:
			debug("JSON check failed")
		return False


	@classmethod
	def chk_JSON_tank_stats(cls, json_resp: dict) -> bool:
		""""Check String for being a valid Tank list JSON file"""
		try:
			if cls.chk_JSON_status(json_resp):
				if ('data' in json_resp) and (len(json_resp['data']) > 0):
					debug('JSON tank list check OK')
					return True
		except Exception as err:
			error('JSON check FAILED: ' + str(json_resp) )
			error(exception=err)
		return False


	# Methods --------------------------------------------------
	def load_tanks(self, tankopedia_fn: str):
		"""Load tanks from tankopedia JSON"""
		if tankopedia_fn is None:
			return False
		try:
			with open(tankopedia_fn, 'rt', encoding='utf8') as f:
				self.tanks = json.loads(f.read())
				self.tanks['tankStr'] = dict()
				p = re.compile('.+_short$')
				for usr_str in self.tanks['userStr']:
					if p.match(usr_str) is None:
						self.tanks['tankStr'][self.tanks['userStr'][usr_str]] = usr_str
				self.tanks_by_tier = dict()
				for tier in range(1,11):
					self.tanks_by_tier[str(tier)] = list()
				for tank in self.tanks['data'].values():
					self.tanks_by_tier[str(tank['tier'])].append(tank['tank_id'])
				return True
		except Exception as err:
			error('Could not read tankopedia: ' + tankopedia_fn, err) 
		return False     
	

	def get_replay_filename(self, replay: dict):
		try:
			summary = replay['data']['summary']
			timestamp   = int(summary['battle_start_timestamp'])
			timestamp    = get_date_str(timestamp, date_format='%Y%m%d_%H%M')
			player      = summary['player_name']
			arena_id    = int(summary['arena_unique_id'])
			vehicle     = self.name2tank_str(summary['vehicle'])
			vehicle.replace(' ','_')

			return '{:s}_{:s}_{:s}_{:d}.wotbreplay.json'.format(timestamp, player, vehicle, arena_id)
		except Exception as err:
			error('Unexpected Exception', err) 
			return None 

	def get_map_user_strs(self) -> str:
		return self.maps.keys()


	def tank_str2name(self, tank_str: str) -> str:
		"""Return tank name from short tank string in replays"""
		try:
			return self.tanks["userStr"][tank_str]
		except:
			error('Tank ' + tank_str + ' not found')
		return tank_str

	
	def name2tank_str(self, name: str) -> str:
		"""Return shorttank string from tank string in replays"""
		try:
			return self.tanks["tankStr"][name]
		except:
			debug('Tank ' + name + ' not found')
		return name


	def get_tanks_by_tier(self, tier: int) -> list():
		"""Returns tank_ids by tier"""
		try:
			return self.tanks_by_tier[str(tier)]
		except KeyError as err:
			error('Invalid tier', err)
		return None  
	

	def get_url_clan_info(self, server: str, clan_id: int) -> str:
		try:
			if server is None:
				return None 
			return self.URL_WG_SERVER[server] + self.URL_WG_CLAN_INFO + self.WG_app_id + '&clan_id=' + str(clan_id)
		except Exception as err:
			if (server is None) or (server.lower() not in WG.ACCOUNT_ID_SERVER.keys()):
				error('No server name or invalid server name given: ' + server if (server !=  None) else '')
				error('Available servers: ' + ', '.join(WG.ACCOUNT_ID_SERVER.keys()))
			error(exception=err)
		return None


	def get_url_player_tank_list(self, account_id: int) -> str:
		return self.get_url_player_tanks_stats(account_id, fields='tank_id')


	def get_url_player_tanks_stats(self, account_id: int, tank_ids: list = [], fields: list = []) -> str: 
		server = self.get_server(account_id)
		if server is None:
			return None        
		if (tank_ids is not None) and (len(tank_ids) > 0):
			tank_id_str= '&tank_id=' + '%2C'.join([ str(x) for x in tank_ids])
		else:
			# emtpy tank-id list returns all the player's tanks  
			tank_id_str = ''

		if (fields is not None) and (len(fields) > 0):
			field_str =  '&fields=' + '%2C'.join(fields)
		else:
			# return all the fields
			field_str = ''

		return self.URL_WG_SERVER[server] + self.URL_WG_PLAYER_TANK_STATS + self.WG_app_id + '&account_id=' + str(account_id) + tank_id_str + field_str
		

	def get_url_player_stats(self, account_id,  fields) -> str: 
		try:
			server = self.get_server(account_id)
			if server is None:
				return None 
			if (fields is not None) and (len(fields) > 0):
				field_str =  '&fields=' + '%2C'.join(fields)
			else:
				# return all the fields
				field_str = ''

			return self.URL_WG_SERVER[server] + self.URL_WG_PLAYER_STATS + self.WG_app_id + '&account_id=' + str(account_id) + field_str
		except Exception as err:
			if (server is None):
				error('Invalid account_id')
			error(exception=err)
		return None


	def get_url_player_achievements(self, account_ids: list,  fields : str = 'max_series') -> str: 
		try:
			# assumming that all account_ids are from the same server. This has to be taken care. 
			server = self.get_server(account_ids[0])

			if server is None:
				return None 
			account_ids_str = '%2C'.join(str(id) for id in account_ids)
			if (fields is not None) and (len(fields) > 0):
				field_str =  '&fields=' + '%2C'.join(fields)
			else:
				# return all the fields
				field_str = ''

			return self.URL_WG_SERVER[server] + self.URL_WG_PLAYER_ACHIEVEMENTS + self.WG_app_id + '&account_id=' + account_ids_str + field_str
		except Exception as err:
			if (server is None):
				error('Invalid account_id')
			error(exception=err)
		return None


	def get_url_account_id(self, nickname, server) -> int:
		try:
			return self.URL_WG_SERVER[server] + self.URL_WG_ACCOUNT_ID + self.WG_app_id + '&search=' + urllib.parse.quote(nickname)
		except Exception as err:
			if nickname is None or len(nickname) == 0:
				error('No nickname given')            
			if (server is None) or (server.lower() not in WG.ACCOUNT_ID_SERVER.keys()):
				error('No server name or invalid server name given: ' + server if (server !=  None) else '')
				error('Available servers: ' + ', '.join(WG.ACCOUNT_ID_SERVER.keys()))
			error(exception=err)
		return None


	def url_get_server(self, url: str) -> str: 
		"""Decode WG server from the URL"""         
		try:            
			for server in self.session:
				if url.startswith(self.URL_WG_SERVER[server]):
					return server
		except Exception as err:
			error(exception=err)
		return 'eu'  # default


	def print_request_stats(self):
		"""Print session statics"""
		if self.global_rate_limit:
			verbose_std('Globar rate limit: ' + self.session.get_stats_str())
		else:
			for server in self.session:
				verbose_std('Per server rate limits: '  + server + ': '+ self.session[server].get_stats_str())


	async def get_url_JSON(self, url: str, chk_JSON_func = None, max_tries = MAX_RETRIES) -> dict:
		"""Class WG get_url_JSON() for load balancing between WG servers 
		that have individial rate limits"""
		
		if self.global_rate_limit:
			session = self.session
		else:
			server = self.url_get_server(url)
			session = self.session[server]
			debug('server:' + server)
		return await get_url_JSON(session, url, chk_JSON_func, max_tries)


	async def get_account_id(self, nickname: str) -> int:
		"""Get WG account_id for a nickname"""
		try:
			nick    = None
			server  = None
			nick, server = nickname.split('@')
			debug(nick + ' @ '+ server)
			server = server.lower()
			if nick is None or server is None:
				raise ValueError('Invalid nickname given: ' + nickname)
			url = self.get_url_account_id(nick, server)

			json_data = await self.get_url_JSON(url, self.chk_JSON_status)
			for res in json_data['data']:
				if res['nickname'].lower() == nick.lower(): 
					return res['account_id']
			error('No WG account_id found: ' + nickname)
			
		except Exception as err:
			error(exception=err)
		return None
	  

	async def get_player_tank_stats(self, account_id: int, tank_ids = [], fields = [], cache=True, cache_only = False) -> dict:
		"""Get player's stats (WR, # of battles) in a tank or all tanks (empty tank_ids[])"""
		try:
			stats = None

			# try cached stats first:
			if cache:
				stats = await self.get_cached_tank_stats(account_id, tank_ids, fields)
				if stats is not None:
					return stats
				if cache_only: 
					return None

			# Cached stats not found, fetching new ones
			url = self.get_url_player_tanks_stats(account_id, tank_ids, fields)
			json_data = await self.get_url_JSON(url, self.chk_JSON_status)
			if json_data is not None:
				# debug('JSON Response received: ' + str(json_data))
				stats = json_data['data'][str(account_id)]
				if cache:
					await self.save_tank_stats(account_id, tank_ids, stats)
				return stats
		except Exception as err:
			error(exception=err)
		return None

   
	async def get_player_stats(self, account_id: int, fields = [], cache=True, cache_only = False) -> dict:
		"""Get player's global stats """
		try:
			# debug('account_id: ' + str(account_id) )
			stats = None

			# try cached stats first:
			if cache:
				stats = await self.get_cached_player_stats(account_id,fields)
				# stats found unless StatsNotFound exception is raised 
				return stats

		except StatsNotFound as err:
			debug('No Cached stats for account_id=' + str(account_id))
			if cache_only: 
			   return None
			# No cached stats found, need to retrieve
			
		try:
			# Cached stats not found, fetching new ones
			url = self.get_url_player_stats(account_id, fields)
			json_data = await self.get_url_JSON(url, self.chk_JSON_status)
			if json_data is not None:
				# debug('JSON Response received: ' + str(json_data))
				stats = json_data['data'][str(account_id)]
				if cache:
					await self.save_player_stats(account_id, stats)
				return stats
		except Exception as err:
			error(exception=err)
		return None


	async def get_player_achievements(self, account_ids: list, fields = [], cache=True) -> dict:
		"""Get player's achievements stats """
		try:
			account_ids = set(account_ids)
			stats = dict()
			if len(account_ids) == 0:
				debug('Zero account_ids given')
				return None

			# try cached stats first:
			if cache:
				debug('Checking for cached stats')
				account_ids_cached = set()
				for account_id in account_ids:
					try:
						stats[str(account_id)] = await self.get_cached_player_achievements(account_id,fields)
						account_ids_cached.add(account_id)
					except StatsNotFound as err:
						# No cached stats found, need to retrieve
						debug(exception=err)                
				account_ids = account_ids.difference(account_ids_cached)
				if len(account_ids) == 0:
					return stats
			debug('fetching new stats')
			# Cached stats not found, fetching new ones
			url = self.get_url_player_achievements(list(account_ids), fields)
			json_data = await self.get_url_JSON(url, self.chk_JSON_status)
			if (json_data is not None) and ('data' in json_data):
				# debug('JSON Response received: ' + str(json_data))
				for account_id in json_data['data'].keys():
					stats[account_id] = json_data['data'][account_id]
					if cache:
						await self.save_player_achievements(int(account_id), json_data['data'][account_id])
				return stats
		except Exception as err:
			error(exception=err)
		return None


	def merge_player_stats(self, stats1: dict, stats2: dict) -> dict:
		try:
			if stats2 is None: return stats1								
			for keyA in stats2:
				if keyA not in stats1:
					stats1[keyA] = stats2[keyA]
				else:
					for keyB in stats2[keyA]:
						stats1[keyA][keyB] = stats2[keyA][keyB] 
			return stats1
		except KeyError as err:
			error('Key not found', err) 
		return None


	def get_tank_data(self, tank_id: int, field: str):
		if self.tanks is None:
			return None
		try:
			return self.tanks['data'][str(tank_id)][field]
		except KeyError as err:
			error('Key not found', err)
		return None

 
	def get_tank_tier(self, tank_id: int) -> str:
		try:
			return self.get_tank_data(tank_id, 'tier')
		except Exception as err:
			error(exception=err)
		return None   

	
	def get_tank_name(self, tank_id: int) -> str:
		try:
			return self.get_tank_data(tank_id, 'name')
		except Exception as err:
			error(exception=err)
		return None   


	def get_tank_type(self, tank_id: int) -> str:
		try:
			return self.get_tank_data(tank_id, 'type')
		except Exception as err:
			error(exception=err)
		return None   


	def get_tank_nation(self, tank_id: int) -> str:
		try:
			return self.get_tank_data(tank_id, 'nation')
		except Exception as err:
			error(exception=err)
		return None   


	def get_tank_type_str(self, tank_id: int) -> str:
		try:
			tank_type = self.get_tank_data(tank_id, 'type')
			return self.TANK_TYPE_STR[self.TANK_TYPE_ID[tank_type]]
		except Exception as err:
			error(exception=err)
		return None   


	def get_tank_nation_str(self, tank_id: int) -> str:
		try:
			nation = self.get_tank_data(tank_id, 'nation')
			return self.NATION_STR[self.NATION_ID[nation]]
		except Exception as err:
			error(exception=err)
		return None   


	def get_tank_type_id(self, tank_id: int) -> str:
		try:
			tank_type = self.get_tank_data(tank_id, 'type')
			return self.TANK_TYPE_ID[tank_type]
		except Exception as err:
			error(exception=err)
		return None   


	def get_tank_nation_id(self, tank_id: int) -> str:
		try:
			nation = self.get_tank_data(tank_id, 'nation')
			return self.NATION_ID[nation]
		except Exception as err:
			error(exception=err)
		return None 

	def is_premium(self, tank_id: int) -> bool:
		try:
			return self.get_tank_data(tank_id, 'is_premium')
		except Exception as err:
			error(exception=err)
		return None


	async def save_stats(self, statsType: str, key: list, stats: list):
		"""Save stats to a async queue to be saved by the stat_saver -task"""
		if self.statsQ is None:
			return False
		else:
			await self.statsQ.put([ statsType, key, stats, NOW() ])
			return True


	async def save_player_stats(self, account_id: int, stats: dict):
		try:
			await self.save_stats(MODE_PLAYER_STATS, [int(account_id)], stats)
		except Exception as err:
			error(exception=err)
		return None


	async def save_player_achievements(self, account_id: int, stats: dict):
		try:
			await self.save_stats(MODE_PLAYER_ACHIEVEMENTS, [int(account_id)], stats)
		except Exception as err:
			error(exception=err)
		return None


	async def save_tank_stats(self, account_id: int, tank_ids: list, stats: dict):
		try:
			await self.save_stats(MODE_TANK_STATS, [int(account_id), tank_ids], stats)
		except Exception as err:
			error(exception=err)
		return None


	async def stat_saver(self): 
		"""Async task for saving stats into cache in background"""

		if self.statsQ is None:
			error('No statsQ defined')
			return None
		try:
			self.cache = await aiosqlite.connect(WG.CACHE_DB_FILE)
			# Create cache tables table
			await self.cache.execute(WG.SQL_TANK_STATS_CREATE_TBL)
			await self.cache.execute(WG.SQL_PLAYER_STATS_CREATE_TBL)
			await self.cache.execute(WG.SQL_PLAYER_ACHIEVEMENTS_CREATE_TBL)

			await self.cache.commit()
			
			if is_debug():
				async with self.cache.execute(WG.SQL_PLAYER_STATS_COUNT) as cursor:
					debug('Cache contains: ' + str((await cursor.fetchone())[0]) + ' player stat records' )
				async with self.cache.execute(WG.SQL_PLAYER_ACHIEVEMENTS_COUNT) as cursor:
					debug('Cache contains: ' + str((await cursor.fetchone())[0]) + ' player achievement records' )
				async with self.cache.execute(WG.SQL_TANK_STATS_COUNT) as cursor:
					debug('Cache contains: ' + str((await cursor.fetchone())[0]) + ' player tank stat records' )
		except Exception as err:
			error(exception=err)
			sys.exit(1)

		while True:
			try:
				stats = await self.statsQ.get()
			
				stat_type  = stats[0]
				key         = stats[1]
				stats_data  = stats[2]
				update_time = stats[3]

				#debug('stat_type: ' + stat_type + ', key: ' + str(key))
				if stat_type == MODE_TANK_STATS:
					await self.store_tank_stats(key, stats_data, update_time)
				elif stat_type == MODE_PLAYER_STATS:
					await self.store_player_stats(key, stats_data, update_time)
				elif stat_type == MODE_PLAYER_ACHIEVEMENTS:
					await self.store_player_achievements(key, stats_data, update_time)
				else: 
					error('Function to saves stats type \'' + stat_type + '\' is not implemented yet')
			
			except asyncio.CancelledError:
				# this is an eternal loop that will wait until cancelled	
				return None

			except Exception as err:
				error(exception=err)
			self.statsQ.task_done()
		return None


	async def cleanup_cache(self, grace_time = CACHE_GRACE_TIME):
		"""Clean old cache records"""
		if self.cache is None:
			debug('No active cache')
			return None
		for table in WG.SQL_TABLES:
			async with self.cache.execute(WG.SQL_CHECK_TABLE_EXITS, (table,)) as cursor:
				if (await cursor.fetchone()) is not None:
					debug('Pruning cache table: ' + table)
					await self.cache.execute(WG.SQL_PRUNE_CACHE.format(table, NOW() - grace_time))
					await self.cache.commit()
		return None


	async def store_tank_stats(self, key: list , stats_data: list, update_time: int):
		"""Save tank stats into cache"""
		assert self.cache is not None, "cache must be initiated"
		
		try:
			account_id  = key[0]
			tank_ids    = set(key[1])
			if stats_data is not None:
				for stat in stats_data:
					tank_id = stat['tank_id']
					await self.cache.execute(WG.SQL_TANK_STATS_UPDATE, (account_id, tank_id, update_time, json.dumps(stat)))
					tank_ids.discard(tank_id)
			# no stats found => Add None to mark that
			for tank_id in tank_ids:
				await self.cache.execute(WG.SQL_TANK_STATS_UPDATE, (account_id, tank_id, update_time, None))
			await self.cache.commit()
			debug('Cached tank stats saved for account_id: ' + str(account_id) )
			return True
		except Exception as err:
			error(exception=err)
			return False


	async def store_player_stats(self, key: list , stats_data: list, update_time: int):
		"""Save player stats into cache"""
		assert self.cache is not None, "cache must be initiated"
		
		try:
			account_id  = key[0]
			if stats_data is not None:
				await self.cache.execute(WG.SQL_PLAYER_STATS_UPDATE, (account_id, update_time, json.dumps(stats_data)))
			else:
				await self.cache.execute(WG.SQL_PLAYER_STATS_UPDATE, (account_id, update_time, None))
			await self.cache.commit()
			debug('Cached player stats saved for account_id: ' + str(account_id) )
			return True
		except Exception as err:
			error(exception=err)
			return False


	async def store_player_achievements(self, key: list , stats_data: list, update_time: int):
		"""Save player stats into cache"""
		assert self.cache is not None, "cache must be initiated"
		
		try:
			account_id  = key[0]
			if stats_data is not None:
				await self.cache.execute(WG.SQL_PLAYER_ACHIEVEMENTS_UPDATE, (account_id, update_time, json.dumps(stats_data)))
			else:
				await self.cache.execute(WG.SQL_PLAYER_ACHIEVEMENTS_UPDATE, (account_id, update_time, None))
			await self.cache.commit()
			debug('Cached player achievements saved for account_id: ' + str(account_id) )
			return True
		except Exception as err:
			error(exception=err)
			return False


	async def get_cached_tank_stats(self, account_id: int, tank_ids: list, fields: list ):
		assert self.cache is not None, "cache must be initiated"

		try:
			# test for cacheDB existence
			debug('Trying cached stats first')
			if self.cache is None:
				debug('No cache DB')
				return None
			
			stats = []
			if len(tank_ids) > 0:
				sql_query = 'SELECT * FROM ' +  WG.SQL_TANK_STATS_TBL + ' WHERE account_id = ? AND update_time > ? AND tank_id IN (' + ','.join([str(x) for x in tank_ids]) + ')'
			else:
				sql_query = 'SELECT * FROM ' +  WG.SQL_TANK_STATS_TBL + ' WHERE account_id = ? AND update_time > ?'

			async with self.cache.execute(sql_query, [account_id, NOW() - WG.CACHE_GRACE_TIME] ) as cursor:
				tank_ids = set(tank_ids)
				async for row in cursor:
					# debug('account_id: ' + str(account_id) + ': 1')
					if row[3] is None:
						# None/null stats found in cache 
						# i.e. stats have been requested, but not returned from WG API
						tank_ids.remove(row[1])
						continue
					# debug('account_id: ' + str(account_id) + ': 2')
					stats.append(json.loads(row[3]))
					# debug('account_id: ' + str(account_id) + ': 3')
					tank_ids.remove(row[1])
				
				# return stats ONLY if ALL the requested stats were found in cache
				if tank_ids == set():
					debug('Cached stats found: ' + str(account_id))
					return stats
		   
		except Exception as err:
			error(exception=err)
		debug('No cached stats found')
		return None


	async def get_cached_player_stats(self, account_id, fields):
		try:
			# test for cacheDB existence
			debug('Trying cached stats first')
			if self.cache is None:
				# debug('No cache DB')
				raise StatsNotFound('No cache DB in use')
					  
			async with self.cache.execute(WG.SQL_PLAYER_STATS_CACHED, [account_id, NOW() - WG.CACHE_GRACE_TIME] ) as cursor:
				row = await cursor.fetchone()
				# debug('account_id: ' + str(account_id) + ': 1')
				if row is None:
					# no cached stats found, marked with an empty array
					# debug('No cached stats found')
					raise StatsNotFound('No cached stats found')
				
				debug('Cached stats found')    
				if row[2] is None:
					# None/null stats found in cache 
					# i.e. stats have been requested, but not returned from WG API
					return None
				else:
					# Return proper stats 
					return json.loads(row[2])
		except StatsNotFound as err:
			debug(exception=err)
			raise
		except Exception as err:
			error('Error trying to look for cached stats', exception=err)
		return None


	async def get_cached_player_achievements(self, account_id, fields):
		try:
			# test for cacheDB existence
			debug('Trying cached stats first')
			if self.cache is None:
				# debug('No cache DB')
				raise StatsNotFound('No cache DB in use')
					  
			async with self.cache.execute(WG.SQL_PLAYER_ACHIEVEMENTS_CACHED, [account_id, NOW() - WG.CACHE_GRACE_TIME] ) as cursor:
				row = await cursor.fetchone()
				# debug('account_id: ' + str(account_id) + ': 1')
				if row is None:
					# no cached stats found, marked with an empty array
					# debug('No cached stats found')
					raise StatsNotFound('No cached stats found')
				
				debug('Cached stats found')    
				if row[2] is None:
					# None/null stats found in cache 
					# i.e. stats have been requested, but not returned from WG API
					return None
				else:
					# Return proper stats 
					return json.loads(row[2])
		except StatsNotFound as err:
			debug(exception=err)
			raise
		except Exception as err:
			error('Error trying to look for cached stats', exception=err)
		return None


# -----------------------------------------------------------
# Class WoTinspector 
# -----------------------------------------------------------

class WoTinspector:
	URL_WI          = 'https://replays.wotinspector.com'
	URL_REPLAY_LIST = URL_WI + '/en/sort/ut/page/'
	URL_REPLAY_DL   = URL_WI + '/en/download/'  
	URL_REPLAY_VIEW = URL_WI +'/en/view/'
	URL_REPLAY_UL   = 'https://api.wotinspector.com/replay/upload?'
	URL_REPLAY_INFO = 'https://api.wotinspector.com/replay/upload?details=full&key='
	URL_TANK_DB     = "https://wotinspector.com/static/armorinspector/tank_db_blitz.js"

	REPLAY_N = 1
	DEFAULT_RATE_LIMIT : float = 10/60  # 10 requests / min
	DEFAULT_TOKEN : str = 'a5428af49bf7495986577d59b0e5bcff'

	def __init__(self, rate_limit: float = DEFAULT_RATE_LIMIT, auth_token: Optional[str] = None):

		headers : Optional[dict[str, str]] = None

		if auth_token is None:
			auth_token = self.DEFAULT_TOKEN        
		headers = dict()
		headers['Authorization'] = 'Token ' + auth_token
		self.session = ThrottledClientSession(rate_limit=rate_limit, 
												filters=[self.URL_REPLAY_LIST, self.URL_REPLAY_INFO], 
												re_filter=False, limit_filtered=True, headers = headers)
		

	async def close(self) -> None:
		if self.session is not None:
			debug('Closing aiohttp session')
			await self.session.close()
		

	async def get_tankopedia(self, filename = 'tanks.json'):
		"""Retrieve Tankpedia from WoTinspector.com"""
	
		async with self.session.get(self.URL_TANK_DB) as r:
			if r.status == 200:
				WI_tank_db=await r.text()
				WI_tank_db = WI_tank_db.split("\n")
			else:
				print('Error: Could not get valid HTTPS response. HTTP: ' + str(r.status) )  
				sys.exit(1) 
			tanks = {}
			n = 0
			p = re.compile('\\s*(\\d+):\\s{"en":"([^"]+)",.*?"tier":(\\d+), "type":(\\d), "premium":(\\d).*')
			for line in WI_tank_db[1:-1]:
				try:
					m = p.match(line)
					tank = {}
					tank['tank_id'] = int(m.group(1))
					tank['name'] = m.group(2)
					tank['tier'] = int(m.group(3))
					tank['type'] = WG.TANK_TYPE[int(m.group(4))]
					tank['is_premium'] = (int(m.group(5)) == 1)
					tanks[str(m.group(1))] = tank
					n += 1
				except Exception as err:
					error(exception=err)
			
			tankopedia = {}
			tankopedia['status'] = "ok"
			tankopedia['meta'] = {"count" : n}
			tankopedia['data'] = tanks
			
			verbose_std("Tankopedia has " + str(n) + " tanks in: " + filename)
			with open(filename,'w') as outfile:
				outfile.write(json.dumps(tankopedia, ensure_ascii=False, indent=4, sort_keys=False))
			return None


	async def get_replay_JSON(self, replay_id: str):
		json_resp = await get_url_JSON(self.session, self.URL_REPLAY_INFO + replay_id, chk_JSON_func=None)
		try:
			if self.chk_JSON_replay(json_resp):
				return json_resp
			else:
				return None
		except Exception as err:
			error('Unexpected Exception', err) 
			return None


	async def post_replay(self,  data, filename = 'Replay', account_id = 0, title = 'Replay', priv = False, N = None):
		try:
			N = N if N is not None else self.REPLAY_N
			self.REPLAY_N += 1

			hash = hashlib.md5()
			hash.update(data)
			replay_id = hash.hexdigest()

			##  Testing if the replay has already been posted
			json_resp = await self.get_replay_JSON(replay_id)
			if json_resp is not None:
				debug('Already uploaded: ' + title, id=N)
				return json_resp

			params = {
				'title'			: title,
				'private' 		: (1 if priv else 0),
				'uploaded_by'	: account_id,
				'details'		: 'full',
				'key'           : replay_id
			} 

			url = self.URL_REPLAY_UL + urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
			#debug('URL: ' + url)
			headers ={'Content-type':  'application/x-www-form-urlencoded'}
			payload = { 'file' : (filename, base64.b64encode(data)) }
		except Exception as err:
			error('Unexpected Exception', exception=err, id=N)
			return None

		json_resp  = None
		for retry in range(MAX_RETRIES):
			debug('Posting: ' + title + ' Try #: ' + str(retry + 1) + '/' + str(MAX_RETRIES), id=N )
			try:
				async with self.session.post(url, headers=headers, data=payload) as resp:
					debug('HTTP response: '+ str(resp.status), id=N)
					if resp.status == 200:								
						debug('HTTP POST 200 = Success. Reading response data', id=N)
						json_resp = await resp.json()
						if self.chk_JSON_replay(json_resp):
							debug('Response data read. Status OK', id=N) 
							return json_resp	
						debug(title + ' : Receive invalid JSON', id=N)
					else:
						debug('Got HTTP/' + str(resp.status), id=N)
			except Exception as err:
				debug(exception=err, id=N)
			await asyncio.sleep(SLEEP)
			
		debug(' Could not post replay: ' + title, id=N)
		return json_resp


	async def get_replay_listing(self, page: int = 0) -> aiohttp.ClientResponse:
		url = self.get_url_replay_listing(page)
		return await self.session.get(url)


	@classmethod
	def get_url_replay_listing(cls, page : int):
		return cls.URL_REPLAY_LIST + str(page) + '?vt=#filters'


	@classmethod
	def get_url_replay_view(cls, replay_id):
		return cls.URL_REPLAY_VIEW + replay_id


	@classmethod
	def get_replay_links(cls, doc: str):
		"""Get replay download links from WoTinspector.com replay listing page"""
		try:
			soup = BeautifulSoup(doc, 'lxml')
			links = soup.find_all('a')
			replay_links = set()
			for tag in links:
				link = tag.get('href',None)
				if (link is not None) and link.startswith(cls.URL_REPLAY_DL):
					replay_links.add(link)
					debug('Adding replay link:' + link)
		except Exception as err:
			error(exception=err)
		return replay_links
	

	@classmethod
	def read_replay_id(cls, json_replay):
		"""Read replay_id from replay JSON file""" 
		try:
			url = json_replay['data']['view_url']
			return cls.get_replay_id(url)
		except Exception as err:
			error(exception=err)
		return None


	@classmethod
	def get_replay_id(cls, url):
		return url.rsplit('/', 1)[-1]


	@classmethod
	def chk_JSON_replay(cls, json_resp) -> bool:
		""""Check String for being a valid JSON file"""
		try:
			if ('status' in json_resp) and json_resp['status'] == 'ok' and \
				(get_JSON_value(json_resp, key='data.summary.exp_base') is not None) :
				debug("JSON check OK")
				return True 
		except KeyError as err:
			debug('Replay JSON check failed', exception=err)
		except:
			debug("Replay JSON check failed: " + str(json_resp))
		return False      

# -----------------------------------------------------------
# Class BlitzStars 
# -----------------------------------------------------------

class BlitzStars:

	URL_BLITZSTARS          = 'https://www.blitzstars.com'
	URL_PLAYER_STATS        = URL_BLITZSTARS + '/api/playerstats'
	URL_PLAYER_TANK_STATS   = URL_BLITZSTARS + '/api/tanks'
	URL_TANK_AVERAGES       = URL_BLITZSTARS + '/tankaverages.json'
	URL_ACTIVE_PLAYERS      = URL_BLITZSTARS +  '/api/playerstats/activeinlast30days'


	def __init__(self, rate_limit=30):
		headers = {'Accept-Encoding': 'gzip, deflate'} 	
		self.session = ThrottledClientSession(rate_limit=rate_limit, headers=headers)

	async def close(self):
		if self.session is not None:
			await self.session.close()        


	@classmethod
	def get_url_tank_averages(cls):
		return cls.URL_TANK_AVERAGES


	@classmethod
	def get_url_player_stats(cls, account_id: int):
		return cls.URL_PLAYER_STATS + '/' + str(account_id)


	@classmethod
	def get_url_player_tank_stats(cls, account_id: int, tank_id: int):
		return cls.URL_PLAYER_TANK_STATS + '/' + str(account_id) + '/' + str(tank_id)


	@classmethod
	def get_url_player_tanks_stats(cls, account_id: int):
		return cls.URL_PLAYER_TANK_STATS + '/' + str(account_id) 


	@classmethod
	def get_url_active_players(cls):
		return cls.URL_ACTIVE_PLAYERS

 
	@classmethod
	def chk_JSON_tank_stats(cls, json_resp):
		"""Check BlitzStars player tank stats"""
		try:
			if (len(json_resp) > 0) and ('tank_id' in json_resp[0]):
				debug('JSON check OK')
				return True
		except Exception as err:
			error('JSON check FAILED: ' + str(json_resp) )
			error(exception=err)
		return False

	@classmethod
	def chk_JSON_player_stats(cls, json_resp : list):
		"""Check BlitzStars player stats"""
		try:
			if (len(json_resp) > 0) and ('account_id' in json_resp[0]):
				debug('JSON check OK')
				return True
		except Exception as err:
			error('JSON check FAILED: ' + str(json_resp) )
			error(exception=err)
		return False


	@classmethod
	async def tank_stats2WG(cls, BS_tank_stats: list) -> list:
		"""Convert BlitzStars player tank stats to WG API format""" 
		try:
			if BS_tank_stats is None:
				return None
			WG_tank_stats = []
			for stat in BS_tank_stats:
				try:
					tmp = {}
					for field in ['all', 'last_battle_time', 'tank_id', 'battle_life_time', 'account_id']:
						tmp[field] = stat[field]
					WG_tank_stats.append(tmp)
				except KeyError as err:
					error(exception=err)
			if len(WG_tank_stats) > 0:
				return WG_tank_stats
		
		except Exception as err:
			error(exception=err)
			pass
		return None


	async def get_player_stats(self, account_id: int, cache=True):
		"""Get player stats from BlitzStars"""
		try:
			# debug('account_id: ' + str(account_id) )
			stats = None

			if cache:
				error('CACHE NOT IMPLEMENTED YET FOR BlitzStars()')
				sys.exit(1)
				# stats = await self.get_cached_player_stats(account_id,fields)
				# stats found unless StatsNotFound exception is raised 
				return stats

		except StatsNotFound as err:
			# No cached stats found, need to retrieve
			debug(exception=err)
			pass
		
		try:
			# Cached stats not found, fetching new ones
			url = self.get_url_player_stats(account_id)
			stats = await get_url_JSON(self.session, url, self.chk_JSON_player_stats)
			if stats is not None:
				# debug('JSON Response received: ' + str(json_data))
				if cache:
					error('CACHE NOT IMPLEMENTED YET FOR BlitzStars()')
					sys.exit(1)
					# await self.save_stats('player_stats', [account_id], stats)
				return stats
		except Exception as err:
			error(exception=err)
		return None

	async def get_player_tank_stats(self, account_id: int, tank_id = None, cache=True):
		"""Get player stats for all his tanks from BlitzStars"""
		try:
			# debug('account_id: ' + str(account_id) )
			stats = None

			if cache:
				error('CACHE NOT IMPLEMENTED YET FOR BlitzStars()')
				sys.exit(1)
				# stats = await self.get_cached_player_stats(account_id,fields)
				# stats found unless StatsNotFound exception is raised 
				return stats

		except StatsNotFound as err:
			# No cached stats found, need to retrieve
			debug(exception=err)
			pass
		
		try:
			# Cached stats not found, fetching new ones
			if tank_id is None:
				# get stats for all the player's tanks
				url = self.get_url_player_tanks_stats(account_id)
			else:
				url = self.get_url_player_tank_stats(account_id, tank_id)
			stats = await get_url_JSON(self.session, url, self.chk_JSON_tank_stats)
			if stats is not None:
				# debug('JSON Response received: ' + str(json_data))
				if cache:
					error('CACHE NOT IMPLEMENTED YET FOR BlitzStars()')
					sys.exit(1)
					# await self.save_stats('player_stats', [account_id], stats)
				return stats
		except Exception as err:
			error(exception=err)
		return None


