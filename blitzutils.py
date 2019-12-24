#!/usr/bin/python3.7

import sys, os, json, time,  base64, urllib, inspect, hashlib, re
import asyncio, aiofiles, aiohttp, aiosqlite, lxml
from pathlib import Path
from bs4 import BeautifulSoup

MAX_RETRIES= 3
SLEEP = 3

LOG_LEVELS = { 'silent': 0, 'normal': 1, 'verbose': 2, 'debug': 3 }
SILENT  = 0
NORMAL  = 1 
VERBOSE = 2
DEBUG   = 3
_log_level = NORMAL

## Progress dots
_progress_N = 100
_progress_i = 0

UMASK= os.umask(0)
os.umask(UMASK)


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

def set_log_level(silent: bool,verbose: bool, debug: bool):
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


def verbose(msg = "", id = None):
    """Print a message"""
    if _log_level >= VERBOSE:
        if id == None:
            print(msg)
        else:
            print('[' + str(id) + ']: ' + msg)
    return None


def verbose_std(msg = "", id = None):
    """Print a message"""
    if _log_level >= NORMAL:
        if id == None:
            print(msg)
        else:
            print('[' + str(id) + ']: ' + msg)            
    return None


def debug(msg = "", n = None):
    """print a conditional debug message"""
    if _log_level >= DEBUG:
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        caller = calframe[1][3]
        if n == None:
            print('DEBUG: ' + caller + '(): ' + msg)
        else:
            print('DEBUG: ' + caller + '()' + '[' + str(n) + ']: ' + msg)
    return None


def error(msg = "", exception = None, id = None):
    """Print an error message"""
    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe, 2)
    caller = calframe[1][3]
    exception_msg = ''
    if (exception != None) and isinstance(exception, Exception):
        exception_msg = ' : Exception: ' + str(type(exception)) + ' : ' + str(exception)

    if id == None:
        print('ERROR: ' + caller + '(): ' + msg + exception_msg)
    else:
        print('ERROR: ' + caller + '()' + '[' + str(id) + ']: ' + msg + exception_msg)
    return None


def print_progress(force = False):
    """Print progress dots"""
    global _progress_N, _progress_i
    if (_log_level > SILENT) and ( force or (_log_level < DEBUG ) ):
        _progress_i = (_progress_i + 1) % _progress_N
        if _progress_i == 0:
            print('.', end='', flush=True)    
        
def set_progress_step(n: int):
    """Set the frquency of the progress dots. The bigger 'n', the fewer dots"""
    global _progress_N 
    if n > 0:
        _progress_N = n        
    return


def wait(sec : int):
    for i in range(0, sec): 
       i=i   ## to get rid of the warning... 
       time.sleep(1)
       print_progress(True)
    print('', flush=True)  


def print_new_line(force = False):
    if (_log_level > SILENT) and ( force or (_log_level < DEBUG ) ):
        print('', flush=True)


def NOW() -> int:
    return int(time.time())


async def read_int_list(filename: str) -> list():
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


async def save_JSON(filename: str, json_data: dict, sort_keys = False) -> bool:
    """Save JSON data into file"""
    try:
        dirname = os.path.dirname(filename)
        if (dirname != '') and not os.path.isdir(dirname):
            os.makedirs(dirname, 0o770-UMASK)
        async with aiofiles.open(filename,'w', encoding="utf8") as outfile:
            await outfile.write(json.dumps(json_data, ensure_ascii=False, indent=4, sort_keys=sort_keys))
            return True
    except Exception as err:
        error('Error saving JSON', err)
    return False


async def open_JSON(filename: str, chk_JSON_func = None) -> dict:
    try:
        async with aiofiles.open(filename) as fp:
            json_data = json.loads(await fp.read())
            if (chk_JSON_func == None):
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
        try:
            if session == None:
                error('Session must be initialized first')
                sys.exit(1)
            #debug(url)
            ## To avoid excessive use of servers            
            for retry in range(1,max_tries+1):
                async with session.get(url) as resp:
                    if resp.status == 200:
                        debug('HTTP request OK')
                        json_resp = await resp.json()       
                        if (chk_JSON_func == None) or chk_JSON_func(json_resp):
                            # debug("Received valid JSON: " + str(json_resp))
                            return json_resp
                        else:
                            debug('Received JSON error. URL: ' + url)
                            # Sometimes WG API returns JSON error even a retry gives valid JSON
                            # return None                            
                    if retry == max_tries:                        
                        if json_resp != None:
                            str_json = str(json_resp)
                        else:
                            str_json = 'None'   ## Change to None ??
                        raise aiohttp.ClientError('Request failed: ' + str(resp.status) + ' JSON Response: ' + str_json )
                    debug('Retrying URL [' + str(retry) + '/' +  str(max_tries) + ']: ' + url )
                    await asyncio.sleep(SLEEP)

        except aiohttp.ClientError as err:
            error("Could not retrieve URL: " + url)
            error(exception=err)
        except asyncio.CancelledError as err:
            error('Queue gets cancelled while still working.')        
        except Exception as err:
            error('Unexpected Exception', err)
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


## -----------------------------------------------------------
#### Class StatsNotFound 
## -----------------------------------------------------------

class CachedStatsNotFound(Exception):
    pass

## -----------------------------------------------------------
#### Class WG 
## -----------------------------------------------------------

class WG:

    URL_WG_CLAN_INFO         = 'clans/info/?application_id='
    #URL_WG_PLAYER_TANK_LIST   = 'tanks/stats/?fields=tank_id%2Clast_battle_time&application_id='
    #URL_WG_PLAYER_TANK_LIST   = 'tanks/stats/?fields=account_id%2Ctank_id%2Clast_battle_time%2Cbattle_life_time%2Call&application_id='
    URL_WG_PLAYER_TANK_STATS  = 'tanks/stats/?application_id='
    URL_WG_ACCOUNT_ID        = 'account/list/?fields=account_id%2Cnickname&application_id='
    URL_WG_PLAYER_STATS      = 'account/info/?application_id='
    CACHE_DB_FILE           = '.blitzutils_cache.sqlite3' 
    CACHE_GRACE_TIME        =  2*7*24*3600  # 2 weeks cache

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
          
    
    SQL_TANK_STATS_TBL          = 'tank_stats'

    SQL_TANK_STATS_CREATE_TBL   = 'CREATE TABLE IF NOT EXISTS ' + SQL_TANK_STATS_TBL + \
                                    """ ( account_id INTEGER NOT NULL, 
                                    tank_id INTEGER NOT NULL, 
                                    update_time INTEGER NOT NULL, 
                                    stats TEXT, 
                                    PRIMARY KEY (account_id, tank_id) )"""

    SQL_TANK_STATS_COUNT        = 'SELECT COUNT(*) FROM ' + SQL_TANK_STATS_TBL

    SQL_TANK_STATS_UPDATE       = 'REPLACE INTO ' + SQL_TANK_STATS_TBL + '(account_id, tank_id, update_time, stats) VALUES(?,?,?,?)'

    SQL_PLAYER_STATS_TBL        = 'player_stats'

    SQL_PLAYER_STATS_CREATE_TBL = 'CREATE TABLE IF NOT EXISTS ' + SQL_PLAYER_STATS_TBL + \
                                    """ ( account_id INTEGER PRIMARY KEY, 
                                    update_time INTEGER NOT NULL, 
                                    stats TEXT)"""

    SQL_PLAYER_STATS_COUNT       = 'SELECT COUNT(*) FROM ' + SQL_PLAYER_STATS_TBL

    SQL_PLAYER_STATS_UPDATE     = 'REPLACE INTO ' + SQL_PLAYER_STATS_TBL + '(account_id, update_time, stats) VALUES(?,?,?)'

    SQL_PLAYER_STATS_CACHED     = 'SELECT * FROM ' +  SQL_PLAYER_STATS_TBL + ' WHERE account_id = ? AND update_time > ?'

    SQL_TABLES                  = [ SQL_PLAYER_STATS_TBL, SQL_TANK_STATS_TBL ]

    SQL_CHECK_TABLE_EXITS       = """SELECT name FROM sqlite_master WHERE type='table' AND name=?"""

    SQL_PRUNE_CACHE             = """DELETE from {} WHERE update_time < {}""" 

## Default data. Please use the latest maps.json

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

    NATIONS = [ 'ussr', 'germany', 'usa', 'china', 'france', 'uk', 'japan', 'other', 'european']    
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

    TANK_TYPE = [ 'lightTank', 'mediumTank', 'heavyTank', 'AT-SPG' ]
    TANK_TYPE_ID = {
        'lightTank'     : 0,
        'mediumTank'    : 1,
        'heavyTank'     : 2,
        'AT-SPG'        : 3
        }

    URL_WG_SERVER = {
        'eu' : 'https://api.wotblitz.eu/wotb/',
        'ru' : 'https://api.wotblitz.ru/wotb/',
        'na' : 'https://api.wotblitz.com/wotb/',
        'asia' : 'https://api.wotblitz.asia/wotb/'
        }

    ACCOUNT_ID_SERVER= {
        'ru'    : range(0, int(5e8)),
        'eu'    : range(int(5e8), int(10e8)),
        'na'    : range(int(1e9),int(2e9)),
        'asia'  : range(int(2e9),int(4e9))
        }

    def __init__(self, WG_app_id = None, tankopedia_fn =  None, maps_fn = None, stats_cache = False):
        
        self.WG_app_id = WG_app_id
        self.load_tanks(tankopedia_fn)
        WG.tanks = self.tanks

        if (maps_fn != None):
            if os.path.exists(maps_fn) and os.path.isfile(maps_fn):
                try:
                    with open(maps_fn, 'rt', encoding='utf8') as f:
                        WG.maps = json.loads(f.read())
                except Exception as err:
                    error('Could not read maps file: ' + maps_fn + '\n' + str(err))  
            else:
                verbose('Could not find maps file: ' + maps_fn)    
        if self.WG_app_id != None:
            self.session = aiohttp.ClientSession()
            debug('WG aiohttp session initiated')
        else:
            self.session = None
            debug('WG aiohttp session NOT initiated')
        
        # cache TBD
        self.cache = None
        self.statsQ = None
        self.stat_saver_task = None

    
    @classmethod
    async def create(cls,  WG_app_id = None, tankopedia_fn =  None, maps_fn = None, stats_cache = False):
        """Separete Constuctor method to handle async calls required to initialize the object"""
        self = WG(WG_app_id, tankopedia_fn , maps_fn )

        if stats_cache:
            try:
                self.cache = await aiosqlite.connect(WG.CACHE_DB_FILE)
                ## Player Tank stats cache table
                await self.cache.execute(WG.SQL_TANK_STATS_CREATE_TBL) 

                await self.cache.commit()
                
                async with self.cache.execute(WG.SQL_TANK_STATS_COUNT) as cursor:
                    debug('Cache contains: ' + str((await cursor.fetchone())[0]) + ' cached player tank stat records' )
                
                self.statsQ = asyncio.Queue()
                self.stat_saver_task = asyncio.create_task(self.stat_saver())
            except Exception as err:
                error(exception=err)
                sys.exit(1)
        return self

    async def close(self):
        # close stats queue 
        if self.statsQ != None:
            debug('WG.close(): Waiting for statsQ to finish')
            await self.statsQ.join()
            debug('WG.close(): statsQ finished')
            self.stat_saver_task.cancel()
            debug('statsCacheTask cancelled')
            await self.stat_saver_task 
        

        
        # close cacheDB
        if self.cache != None:
            # prune old cache records
            await self.cleanup_cache()   
            await self.cache.commit()
            await self.cache.close()
        
        if self.session != None:
            await self.session.close()        
        return

    ## Class methods  ----------------------------------------------------------

    @classmethod
    def get_server(cls, account_id: int) -> str:
        """Get Realm/server of an account based on account ID"""
        if account_id > 1e9:
            if account_id > 2e9:
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
    def get_map(cls, map_str: str) -> str:
        """Return map name from short map string in replays"""
        try:
            return cls.maps[map_str]
        except:
            error('Map ' + map_str + ' not found')
        return None
    

    @classmethod
    def get_map_user_strs(cls) -> str:
        return cls.maps.keys()


    @classmethod
    def get_tank_user_strs(cls) -> str:
        return cls.tanks["userStr"].keys()


    @classmethod
    def chk_JSON(cls, json_obj, check = None) -> bool:
        try:
            if (check == None): 
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
            if (json_resp['status'] == 'ok') and ('data' in json_resp):
                return True
            elif json_resp['status'] == 'error':
                if ('error' in json_resp):
                    error_msg = 'Received an error'
                    if ('message' in json_resp['error']):
                        error_msg = error_msg + ': ' +  json_resp['error']['message']
                    if ('value' in json_resp['error']):
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
                    if json_resp['data'][acc] != None:
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


    ## Methods --------------------------------------------------
    def load_tanks(self, tankopedia_fn: str):
        """Load tanks from tankopedia JSON"""
        if tankopedia_fn == None:
            return False 

        try:
            with open(tankopedia_fn, 'rt', encoding='utf8') as f:
                self.tanks = json.loads(f.read())
                self.tanks_by_tier = dict()
                for tier in range(1,11):
                    self.tanks_by_tier[str(tier)] = list()
                for tank in self.tanks['data'].values():
                    self.tanks_by_tier[str(tank['tier'])].append(tank['tank_id'])
                return True
        except Exception as err:
            error('Could not read tankopedia: ' + tankopedia_fn, err) 
        return False     
     

    def get_tanks_by_tier(self, tier: int) -> list():
        """Returns tank_ids by tier"""
        try:
            return self.tanks_by_tier[str(tier)]
        except KeyError as err:
            error('Invalid tier', err)
        return None  
    
    def get_url_clan_info(self, server: str, clan_id: int) -> str:
        try:
           return self.URL_WG_SERVER[server] + self.URL_WG_CLAN_INFO + self.WG_app_id + '&clan_id=' + str(clan_id)
        except Exception as err:
            if (server == None) or (server.lower() not in WG.ACCOUNT_ID_SERVER.keys()):
                error('No server name or invalid server name given: ' + server if (server !=  None) else '')
                error('Available servers: ' + ', '.join(WG.ACCOUNT_ID_SERVER.keys()))
            error(exception=err)
        return None


    def get_url_player_tank_list(self, account_id: int) -> str:
        return self.get_url_player_tanks_stats(account_id, fields='tank_id')


    def get_url_player_tanks_stats(self, account_id: int, tank_ids = [], fields = []) -> str: 
        server = self.get_server(account_id)
        
        if (tank_ids != None) and (len(tank_ids) > 0):
            tank_id_str= '&tank_id=' + '%2C'.join([ str(x) for x in tank_ids])
        else:
            # emtpy tank-id list returns all the player's tanks  
            tank_id_str = ''

        if (fields != None) and (len(fields) > 0):
            field_str =  '&fields=' + '%2C'.join(fields)
        else:
            # return all the fields
            field_str = ''

        return self.URL_WG_SERVER[server] + self.URL_WG_PLAYER_TANK_STATS + self.WG_app_id + '&account_id=' + str(account_id) + tank_id_str + field_str
        

    def get_url_player_stats(self, account_id,  fields) -> str: 
        try:
            server = self.get_server(account_id)
            if (fields != None) and (len(fields) > 0):
                field_str =  '&fields=' + '%2C'.join(fields)
            else:
                # return all the fields
                field_str = ''

            return self.URL_WG_SERVER[server] + self.URL_WG_PLAYER_STATS + self.WG_app_id + '&account_id=' + str(account_id) + field_str
        except Exception as err:
            if (server == None):
                error('Invalid account_id')
            error(exception=err)
        return None


    def get_url_account_id(self, nickname, server) -> int:
        try:
            return self.URL_WG_SERVER[server] + self.URL_WG_ACCOUNT_ID + self.WG_app_id + '&search=' + urllib.parse.quote(nickname)
        except Exception as err:
            if nickname == None or len(nickname) == 0:
                error('No nickname given')            
            if (server == None) or (server.lower() not in WG.ACCOUNT_ID_SERVER.keys()):
                error('No server name or invalid server name given: ' + server if (server !=  None) else '')
                error('Available servers: ' + ', '.join(WG.ACCOUNT_ID_SERVER.keys()))
            error(exception=err)
        return None
  

    async def get_account_id(self, nickname: str) -> int:
        """Get WG account_id for a nickname"""
        try:
            nick    = None
            server  = None
            nick, server = nickname.split('@')
            debug(nick + ' @ '+ server)
            server = server.lower()
            if nick == None or server == None:
                raise ValueError('Invalid nickname given: ' + nickname)
            url = self.get_url_account_id(nick, server)
            json_data = await get_url_JSON(self.session, url, self.chk_JSON_status)
            for res in json_data['data']:
                if res['nickname'].lower() == nick.lower(): 
                    return res['account_id']
            error('No WG account_id found: ' + nickname)
            
        except Exception as err:
            error(exception=err)
        return None
      

    async def get_player_tanks_stats(self, account_id: int, tank_ids = [], fields = []) -> dict:
        """Get player's stats (WR, # of battles) in a tank"""
        # debug('Started')        
        
        try:
            #debug('account_id: ' + str(account_id) + ' TankID: ' + ','.join([ str(id) for id in tank_ids]))
            stats = None

            # try cached stats first:
            stats = await self.get_cached_tank_stats(account_id, tank_ids, fields)
            if stats != None:
                return stats

            # Cached stats not found, fetching new ones
            url = self.get_url_player_tanks_stats(account_id, tank_ids, fields)
            json_data = await get_url_JSON(self.session, url, self.chk_JSON_status)
            if json_data != None:
                #debug('JSON Response received: ' + str(json_data))
                stats = json_data['data'][str(account_id)]
                await self.save_stats('tank_stats', [account_id, tank_ids], stats)
                return stats
        except Exception as err:
            error(exception=err)
        return None

   
    async def get_player_stats(self, account_id: int, fields = []) -> dict:
        """Get player's global stats """
        try:
            #debug('account_id: ' + str(account_id) )
            stats = None

            # try cached stats first:
            stats = await self.get_cached_player_stats(account_id,fields)
            # stats found unless CachedStatsNotFound exception is raised 
            return stats

        except CachedStatsNotFound as err:
            # No cached stats found, need to retrieve
            debug(str(err))
            pass
        
        try:
            # Cached stats not found, fetching new ones
            url = self.get_url_player_stats(account_id, fields)
            json_data = await get_url_JSON(self.session, url, self.chk_JSON_status)
            if json_data != None:
                #debug('JSON Response received: ' + str(json_data))
                stats = json_data['data'][str(account_id)]
                await self.save_stats('player_stats', [account_id], stats)
                return stats
        except Exception as err:
            error(exception=err)
        return None


    def merge_player_stats(self, stats1: dict, stats2: dict) -> dict:
        try:
            if stats2 == None: return stats1								
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
        if self.tanks == None:
            return None
        try:
            return self.tanks['data'][str(tank_id)][field]
        except KeyError as err:
            error('Key not found', err)
        return None
        
    def get_tank_tier(self, tank_id: int):
        return self.get_tank_data(tank_id, 'tier')


    async def save_stats(self, statsType: str, key: list, stats: list):
        """Save stats to a async queue to be saved by the stat_saver -task"""
        if self.statsQ == None:
            return False
        else:
            await self.statsQ.put([ statsType, key, stats, NOW() ])
            return True

    async def stat_saver(self): 
        """Async task for saving stats into cache in background"""

        if self.statsQ == None:
            error('No statsQ defined')
            return None
        while True:
            try:
                stats = await self.statsQ.get()
            
                stats_type  = stats[0]
                key         = stats[1]
                stats_data  = stats[2]
                update_time = stats[3]

                if stats_type == 'tank_stats':
                    await self.store_tank_stats(key, stats_data, update_time)
                elif stats_type == 'player_stats':
                    await self.store_player_stats(key, stats_data, update_time)
                else: 
                    error('Function to saves stats type \'' + stats_type + '\' is not implemented yet')
            
            except (asyncio.CancelledError):
                # this is an eternal loop that will wait until cancelled	
                return None

            except Exception as err:
                error(exception=err)
            self.statsQ.task_done()
        return None


    async def cleanup_cache(self, grace_time = CACHE_GRACE_TIME):
        """Clean old cache records"""
        if self.cache == None:
            debug('No active cache')
            return None
        for table in WG.SQL_TABLES:
            async with self.cache.execute(WG.SQL_CHECK_TABLE_EXITS, (table,)) as cursor:
                if (await cursor.fetchone()) != None:
                    debug('Pruning cache table: ' + table)
                    await self.cache.execute(WG.SQL_PRUNE_CACHE.format(table, NOW() - grace_time))
                    await self.cache.commit()
        return None


    async def store_tank_stats(self, key: list , stats_data: list, update_time: int):
        """Save tank stats into cache"""
        try:
            account_id  = key[0]
            tank_ids    = set(key[1])
            if stats_data != None:
                for stat in stats_data:
                    tank_id = stat['tank_id']
                    await self.cache.execute(WG.SQL_TANK_STATS_UPDATE, (account_id, tank_id, update_time, json.dumps(stat)))
                    tank_ids.remove(tank_id)
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
        try:
            account_id  = key[0]
            if stats_data != None:
                await self.cache.execute(WG.SQL_PLAYER_STATS_UPDATE, (account_id, update_time, json.dumps(stats_data)))
            else:
                await self.cache.execute(WG.SQL_PLAYER_STATS_UPDATE, (account_id, update_time, None))
            await self.cache.commit()
            debug('Cached player stats saved for account_id: ' + str(account_id) )
            return True
        except Exception as err:
            error(exception=err)
            return False


    async def get_cached_tank_stats(self, account_id: int, tank_ids: list, fields: list ):
        try:
            # test for cacheDB existence
            debug('Trying cached stats first')
            if self.cache == None:
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
                    #debug('account_id: ' + str(account_id) + ': 1')
                    if row[3] == None:
                        # None/null stats found in cache 
                        # i.e. stats have been requested, but not returned from WG API
                        tank_ids.remove(row[1])
                        continue
                    #debug('account_id: ' + str(account_id) + ': 2')
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
            if self.cache == None:
                #debug('No cache DB')
                raise CachedStatsNotFound('No cache DB in use')
                      
            async with self.cache.execute(WG.SQL_PLAYER_STATS_CACHED, (account_id, NOW() - WG.CACHE_GRACE_TIME) ) as cursor:
                row = await cursor.fetchone()
                #debug('account_id: ' + str(account_id) + ': 1')
                if row == None:
                    # no cached stats found, marked with an empty array
                    #debug('No cached stats found')
                    raise CachedStatsNotFound('No cached stats found')
                
                debug('Cached stats found')    
                if row[3] == None:
                    # None/null stats found in cache 
                    # i.e. stats have been requested, but not returned from WG API
                    return None
                else:
                    # Return proper stats 
                    return json.loads(row[3])
        except CachedStatsNotFound as err:
            debug(str(err))
            raise
        except Exception as err:
            error('Error trying to look for cached stats', err)
        return None



## -----------------------------------------------------------
#### Class WoTinspector 
## -----------------------------------------------------------

class WoTinspector:
    URL_WI          = 'https://replays.wotinspector.com'
    URL_REPLAY_LIST = URL_WI + '/en/sort/ut/page/'
    URL_REPLAY_DL   = URL_WI + '/en/download/'  
    URL_REPLAY_UL   = 'https://api.wotinspector.com/replay/upload?'
    URL_REPLAY_INFO = 'https://api.wotinspector.com/replay/upload?details=full&key='
    URL_TANK_DB     ="https://wotinspector.com/static/armorinspector/tank_db_blitz.js"

    REPLAY_N = 1

    def __init__(self):
        self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session != None:
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
        json_resp = await get_url_JSON(self.session, self.URL_REPLAY_INFO + replay_id, None)
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
            N = N if N != None else self.REPLAY_N
            msg_str = 'Replay[' + str(N) + ']: '
            self.REPLAY_N += 1

            hash = hashlib.md5()
            hash.update(data)
            replay_id = hash.hexdigest()

            ##  Testing if the replay has already been posted
            json_resp = await self.get_replay_JSON(replay_id)
            if json_resp != None:
                debug(msg_str + 'Already uploaded: ' + title)
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
            error(msg_str + 'Unexpected Exception', err)
            return None

        for retry in range(MAX_RETRIES):
            debug(msg_str + 'Posting: ' + title + ' Try #: ' + str(retry + 1) + '/' + str(MAX_RETRIES) )
            try:
                async with self.session.post(url, headers=headers, data=payload) as resp:
                    debug(msg_str + 'HTTP response: '+ str(resp.status))
                    if resp.status == 200:								
                        debug('HTTP POST 200 = Success. Reading response data')
                        json_resp = await resp.json()
                        if json_resp.get('status', None) == None:
                            error(msg_str +' : ' + title + ' : Received invalid JSON')
                        elif (json_resp['status'] == 'ok'): 
                            debug('Response data read')
                            verbose(msg_str + title + ' posted')
                            return json_resp	
                        elif (json_resp['status'] == 'error'):  
                            error(msg_str + json_resp['error']['message'] + ' : ' + title)
                        else:
                            error(msg_str + ' Unspecified error: ' + title)											
                    else:
                        debug(msg_str + 'Got HTTP/' + str(resp.status))
            except Exception as err:
                error(exception=err)
            await asyncio.sleep(SLEEP)
            
        error(msg_str + ' Could not post replay: ' + title)
        return None

    @classmethod
    def get_url_replay_listing(cls, page : int):
        return cls.URL_REPLAY_LIST + str(page) + '?vt=#filters'

    @classmethod
    def get_replay_links(cls, doc: str):
        """Get replay download links from WoTinspector.com replay listing page"""
        try:
            soup = BeautifulSoup(doc, 'lxml')
            links = soup.find_all('a')
            replay_links = set()
            for tag in links:
                link = tag.get('href',None)
                if (link is not None) and (link.find(cls.URL_REPLAY_DL) == 0 ):
                    replay_links.add(link)
                    debug('Adding replay link:' + link)
        except Exception as err:
            error(exception=err)
        return replay_links
    
    @classmethod
    def get_replay_id(cls, url):
        return url.rsplit('/', 1)[-1]

    @classmethod
    def chk_JSON_replay(cls, json_resp):
        """"Check String for being a valid JSON file"""
        try:
            if ('status' in json_resp) and json_resp['status'] == 'ok' and ('data' in json_resp) and json_resp['data'] != None:
                debug("JSON check OK")
                return True 
        except KeyError as err:
            error('Key not found', err)
        except:
            debug("JSON check failed: " + str(json_resp))
        return False      

class BlitzStars:

    URL_BlitzStars      = 'https://www.blitzstars.com'
    URL_playerStats     = URL_BlitzStars + '/api/playerstats'
    URL_playerTankStats = URL_BlitzStars + '/api/tanks'
    URL_playersTankStats= URL_BlitzStars +  '/api/top/forjylpah?'
    URL_tankAverages    = URL_BlitzStars + '/tankaverages.json'
    URL_activeplayers   = URL_BlitzStars +  '/api/playerstats/activeinlast30days'


    def __init__(self):
        self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session != None:
            await self.session.close()        

    @classmethod
    def getUrlTankAvgs(cls):
        return cls.URL_tankAverages

    @classmethod
    def get_url_player_stats(cls, account_id: int):
        return cls.URL_playerStats + '/' + str(account_id)

    @classmethod
    def getUrlPlayerTankStats(cls, account_id: int, tank_id: int):
        return cls.URL_playerTankStats + '/' + str(account_id) + '/' + str(tank_id)

    @classmethod
    def getUrlPlayersTankStats(cls, account_id: int):
        return cls.URL_playersTankStats + '&accountId=' + str(account_id) 

    @classmethod
    def getUrlActivePlayers(cls):
        return cls.URL_activeplayers
    
    @classmethod
    def chkJSONtankStats(cls, json_resp):
        """"Check String for being a valid JSON file"""
        try:
            # print(str(json_resp['data']))
            if ('data' in json_resp):
                debug("JSON check OK")
                return True 
        except KeyError as err:
            error('Key not found', err)
        except:
            debug("JSON check failed")
        return False        
