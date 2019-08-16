#!/usr/bin/python3.7

import sys, os, json, time,  base64, urllib, inspect, hashlib
import asyncio, aiofiles, aiohttp, aiosqlite, lxml
from bs4 import BeautifulSoup

MAX_RETRIES= 3
SLEEP = 3
DEBUG = False
VERBOSE = False
SILENT = False
UMASK= os.umask(0)
os.umask(UMASK)

# def chkJSONaccountList(account_list: list) -> bool:
#     """"Check String for being a valid accountID list JSON file"""
#     try:
#         if int(account_list[0]) > 0:
#             return True
#     except:
#         cls.debug("JSON check failed")
#     return False

def setDebug(debug: bool):
    global DEBUG, VERBOSE, SILENT
    if debug != None:
        DEBUG = debug
    if DEBUG: 
        VERBOSE = True
        SILENT  = False

def setVerbose(verbose: bool):
    global VERBOSE, SILENT
    if verbose != None:
        VERBOSE = verbose
    if VERBOSE: 
        SILENT = False

def setSilent(silent: bool):
    global DEBUG, VERBOSE, SILENT
    if silent != None:
        SILENT = silent
    if SILENT:
        VERBOSE = False
        DEBUG   = False

def verbose(msg = ""):
    """Print a message"""
    if VERBOSE:
        print(msg)
    return None

def verbose_std(msg = ""):
    """Print a message"""
    if not SILENT:
        print(msg)
    return None

def printWaiter(force = False):
    if not DEBUG and (not SILENT  or force):
        print('.', end='', flush=True)

    
def printNewline(force = False):
    if not DEBUG and (not SILENT  or force):
        print('', flush=True)


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

def NOW() -> int:
    return int(time.time())


async def readPlainList(filename: str) -> list():
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
        error('Unexpected error when reading file: ' + filename + ' : ' + str(type(err)) + ' : '+ str(err))
    return input_list


async def saveJSON(filename: str, json_data: dict, sort_keys = False) -> bool:
    """Save JSON data into file"""
    try:
        dirname = os.path.dirname(filename)
        if (dirname != '') and not os.path.isdir(dirname):
            os.makedirs(dirname, 0o770-UMASK)
        async with aiofiles.open(filename,'w', encoding="utf8") as outfile:
            await outfile.write(json.dumps(json_data, ensure_ascii=False, indent=4, sort_keys=sort_keys))
            return True
    except Exception as err:
        error(str(err))
    return False


async def readJSON(filename: str, chkJSONfunc = None):
    try:
        async with aiofiles.open(filename) as fp:
            json_data = json.loads(await fp.read())
            if (chkJSONfunc == None):
                debug("JSON file content not checked: " + filename)
                return json_data                
            elif chkJSONfunc(json_data):
                debug("JSON File is valid: " + filename)
                return json_data
            else:
                debug('JSON File has invalid content: ' + filename)
    except Exception as err:
        error('Unexpected error when reading file: ' + filename + ' : ' + str(type(err)) + ' : '+ str(err))
    return None

async def getUrlJSON(session: aiohttp.ClientSession, url: str, chkJSONfunc = None, max_tries = MAX_RETRIES) -> dict:
        """Retrieve (GET) an URL and return JSON object"""
        try:
            debug(url)
            ## To avoid excessive use of servers            
            for retry in range(1,max_tries+1):
                async with session.get(url) as resp:
                    if resp.status == 200:
                        debug('HTTP request OK')
                        json_resp = await resp.json()       
                        if (chkJSONfunc == None) or chkJSONfunc(json_resp):
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
                            str_json = 'None'
                        raise aiohttp.ClientError('Request failed: ' + str(resp.status) + ' JSON Response: ' + str_json )
                    debug('Retrying URL [' + str(retry) + '/' +  str(max_tries) + ']: ' + url )
                    await asyncio.sleep(SLEEP)

        except aiohttp.ClientError as err:
            error("Could not retrieve URL: " + url)
            error(str(err))
        except asyncio.CancelledError as err:
            error('Queue gets cancelled while still working.')
        
        except Exception as err:
            error('Unexpected Exception: ' + str(err))
        return None

def bldDictHierarcy(d : dict, key : str, value) -> dict:
    """Build hierarcical dict based on multi-level key separated with  """
    try:
        key_hier = key.split('.')
        sub_key = key_hier.pop(0)
        if len(key_hier) == 0:
            d[sub_key] = value
        elif sub_key not in d:   
            d[sub_key] = bldDictHierarcy({}, key_hier, value)
        else:
            d[sub_key] = bldDictHierarcy(d[sub_key], key_hier, value)

        return d    
    except KeyError as err:
        error('Key not found: ' + str(err))
    except Exception as err:
        error(str(err))
    return None


class WG:

    URL_WG_clanInfo         = 'clans/info/?application_id='
    #URL_WG_playerTankList   = 'tanks/stats/?fields=tank_id%2Clast_battle_time&application_id='
    URL_WG_playerTankList   = 'tanks/stats/?fields=account_id%2Ctank_id%2Clast_battle_time%2Cbattle_life_time%2Call&application_id='
    URL_WG_playerTankStats  = 'tanks/stats/?application_id='
    URL_WG_accountID        = 'account/list/?fields=account_id%2Cnickname&application_id='
    URL_WG_playerStats      = 'account/info/?application_id='
    SQLITE_STATS_CACHE      = '.stats_cache.db'  

    sql_create_player_stats_tbl = """CREATE TABLE IF NOT EXISTS player_stats (
                                account_id INTEGER NOT NULL,
                                date INTEGER NOT NULL,
                                stat TEXT,
                                value FLOAT
                                ); """
    
    sql_create_player_tank_stats_tbl = """CREATE TABLE IF NOT EXISTS player_tank_stats (
                            account_id INTEGER NOT NULL,
                            tank_id INTEGER DEFAULT NULL,
                            date INTEGER NOT NULL,
                            stat TEXT,
                            value FLOAT
                            ); """
    
    sql_select_player_stats = """SELECT value FROM player_stats ORDERBY date ASC
                                    WHERE account_id = {} AND stat = {} 
                                    AND date >= {} LIMIT 1;"""
          

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

    nations = [ 'ussr', 'germany', 'usa', 'china', 'france', 'uk', 'japan', 'other', 'european']    
    nation_id = {
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

    tank_type = [ 'lightTank', 'mediumTank', 'heavyTank', 'AT-SPG' ]
    tank_type_id = {
        'lightTank'     : 0,
        'mediumTank'    : 1,
        'heavyTank'     : 2,
        'AT-SPG'        : 3
        }

    URL_WG_server = {
        'eu' : 'https://api.wotblitz.eu/wotb/',
        'ru' : 'https://api.wotblitz.ru/wotb/',
        'na' : 'https://api.wotblitz.com/wotb/',
        'asia' : 'https://api.wotblitz.asia/wotb/'
        }

    accountID_server= {
        'ru'  : range(0, int(5e8)),
        'eu'  : range(int(5e8), int(10e8)),
        'na' : range(int(1e9),int(2e9)),
        'asia': range(int(2e9),int(4e9))
        }

    def __init__(self, WG_appID = None, tankopedia_fn =  None, maps_fn = None):
        
        self.WG_appID = WG_appID

        if (tankopedia_fn != None):
            if os.path.exists(tankopedia_fn) and os.path.isfile(tankopedia_fn):
                try:
                    with open(tankopedia_fn, 'rt', encoding='utf8') as f:
                        WG.tanks = json.loads(f.read())
                        debug(str(WG.tanks["meta"]["count"]) + " tanks")
                except Exception as err:
                    error('Could not read tankopedia: ' + tankopedia_fn + '\n' + str(err))  
            else:
                verbose('Could not find Tankopedia file: ' + tankopedia_fn)    
        if (maps_fn != None):
            if os.path.exists(maps_fn) and os.path.isfile(maps_fn):
                try:
                    with open(maps_fn, 'rt', encoding='utf8') as f:
                        WG.maps = json.loads(f.read())
                except Exception as err:
                    error('Could not read maps file: ' + maps_fn + '\n' + str(err))  
            else:
                verbose('Could not find maps file: ' + maps_fn)    
        if self.WG_appID != None:
            self.session = aiohttp.ClientSession()
            debug('WG aiohttp session initiated')
        else:
            self.session = None
            debug('WG aiohttp session NOT initiated')

        self.cache = None
        self.statsQ = None
        self.statCacheTask = None
        #if os.path.isfile(self.SQLITE_STATS_CACHE):
        if False: 		 
            try:
                self.cache = aiosqlite.connect('SQLITE_STATS_CACHE')
                self.cache.execute(self.sql_create_player_stats_tbl) 
                self.cache.execute(self.sql_create_player_tank_stats_tbl) 
                self.cache.commit()
                self.statsQ = asyncio.Queue()
                self.statCacheTask = asyncio.create_task(self.statsSaver())

            except Exception as err:
                error(str(err))    
        
    async def close(self):
        if self.statsQ != None:
            await self.statsQ.join()
            self.statCacheTask.cancel()
            await asyncio.gather(*self.statCacheTask) 
        
        if self.session != None:
            await self.session.close()        


    ## Class methods  ------------------------------


    @classmethod
    def getServer(cls, accountID: int) -> str:
        """Get Realm/server of an account based on account ID"""
        if accountID > 1e9:
            if accountID > 2e9:
                return 'asia'
            return 'na'
        else:
            if accountID < 5e8:
                return 'ru'
            return 'eu'
        return None

    @classmethod
    def updateMaps(cls, mapdata: dict):
        """Update maps data"""
        cls.maps = mapdata
        return None

    @classmethod
    def getMap(cls, mapStr: str) -> str:
        """Return map name from short map string in replays"""
        try:
            return cls.maps[mapStr]
        except:
            error('Map ' + mapStr + ' not found')
        return None
    
    @classmethod
    def getMapUserStrs(cls) -> str:
        return cls.maps.keys()

    @classmethod
    def getTankUserStrs(cls) -> str:
        return cls.tanks["userStr"].keys()

    @classmethod
    def chkJSONcontent(cls, json_obj, check = None) -> bool:
        try:
            if (check == 'player') and (not cls.chkJSONplayerStats(json_obj)): 
                debug('Checking player JSON failed.')
                return False
            elif (check == 'tank') and (not cls.chkJSONtank(json_obj)): 
                debug('Checking tank JSON failed.')
                return False
            # elif (check == 'clan') and (not chkJSONclan(json_obj)):
            #     debug('Checking clan JSON failed.')
            #     return False
            elif (check == 'tankList') and (not cls.chkJSONtankList(json_obj)): 
                debug('Checking tank list JSON failed.')
                return False
            # elif (check == 'accountlist') and (not cls.chkJSONaccountList(json_obj)):
            #     cls.debug('Checking account list JSON failed.')
            #     return False
        except (TypeError, ValueError) as err:
            debug(str(err))
            return False
        return True

    @classmethod
    def chkJSONstatus(cls, json_resp: dict) -> bool:
        try:
            if json_resp['status'] == 'ok':
                return True
            elif json_resp['status'] == 'error':
                #error(str(json_resp['error']['code']) + ' : ' + json_resp['error']['message'] )
                return False
            else:
                debug('Unknown status-code: ' + json_resp['status'])
                return False
        except KeyError as err:
            error('No field found in JSON data: ' + str(err))
        except:
            error("JSON format error")
        return False

    @classmethod
    def chkJSONgetAccountID(cls, json_resp: dict) -> bool:
        try:
            if cls.chkJSONstatus(json_resp): 
                if (json_resp['meta']['count'] > 0):
                    return True                
        except KeyError as err:
            error('Key :' + str(err) + ' not found')
        except Exception as err:
            error(str(err))
        return False

    # @classmethod
    # def chkJSONplayer(cls, json_resp: dict) -> bool:
    #     """"Check String for being a valid Player JSON file"""
    #     try:
    #         if cls.chkJSONstatus(json_resp): 
    #             if int(json_resp[0]['account_id']) > 0:
    #                 return True
    #     except KeyError as err:
    #         error('Key :' + str(err) + ' not found')
    #     except:
    #         debug("JSON check failed")
    #     return False
    
    @classmethod
    def chkJSONtank(cls, json_resp: dict) -> bool:
        """"Check String for being a valid Tank JSON file"""
        try:
            if cls.chkJSONstatus(json_resp):
                if int(json_resp[0]['tank_id']) > 0:
                    return True
        except KeyError as err:
            error('Key :' + str(err) + ' not found')
        except:
            debug("JSON check failed")
        return False
    

    @classmethod    
    def chkJSONplayerStats(cls, json_resp: dict) -> bool:
        """"Check String for being a valid Tank JSON file"""
        try:
            if cls.chkJSONstatus(json_resp): 
                for acc in json_resp['data']:
                    if json_resp['data'][acc] != None:
                        return True 
        except KeyError as err:
            error('Key :' + str(err) + ' not found')
        except:
            debug("JSON check failed")
        return False

    @classmethod
    def chkJSONtankList(cls, json_resp: dict) -> bool:
        """"Check String for being a valid Tank list JSON file"""
        try:
            accountID = next(iter(json_resp['data']))
            if (json_resp['data'][accountID] != None) and (int(json_resp['data'][accountID][0]['tank_id']) > 0):
                debug('JSON tank list check OK')
                return True
        except Exception as err:
            error('JSON check FAILED: ' + str(type(err)) + ' : ' +  str(err) + ' : ' + str(json_resp) )
        return False

    ## Methods --------------------------------------------------
    async def loadTanks(self, tankopedia_fn: str):
        """Load tanks from tankopedia JSON"""
        if tankopedia_fn != None:
            try:
                async with aiofiles.open(tankopedia_fn, 'rt', encoding='utf8') as f:
                    self.tanks = json.loads(await f.read())
                    return True
            except Exception as err:
                error('Could not read tankopedia: ' + tankopedia_fn + '\n' + str(err))           
        return False        
     
    def getUrlClanInfo(self, server: str, clanID: int) -> str:
        return self.URL_WG_server[server] + self.URL_WG_clanInfo + self.WG_appID + '&clan_id=' + str(clanID)

    def getUrlPlayerTankList(self, accountID: int) -> str:
        server = self.getServer(accountID)
        return self.URL_WG_server[server] + self.URL_WG_playerTankList + self.WG_appID + '&account_id=' + str(accountID)
    
    def getUrlPlayerTankStats(self, accountID, tankID, fields) -> str: 
        server = self.getServer(accountID)
        url = self.URL_WG_server[server] + self.URL_WG_playerTankStats + self.WG_appID + '&account_id=' + str(accountID) + '&tank_id=' + str(tankID)
        if (fields != None) and (len(fields) > 0):
            field_str =  '%2C' + '%2C'.join(fields)
        else:
            field_str = ''
        return url + '&fields=tank_id' + field_str												  

    def getUrlPlayerStats(self, accountID,  fields) -> str: 
        server = self.getServer(accountID)
        return self.URL_WG_server[server] + self.URL_WG_playerStats + self.WG_appID + '&account_id=' + str(accountID) + '&fields=' + '%2C'.join(fields)

    def getUrlAccountID(self, nickname, server) -> int:
        try:
            return self.URL_WG_server[server] + self.URL_WG_accountID + self.WG_appID + '&search=' + urllib.parse.quote(nickname)
        except Exception as err:
            print('ERROR: getUrlAccountID(): ' + str(err))
            return None
  
    async def getAccountID(self, nickname: str) -> int:
        """Get WG account_id for a nickname"""
        try:
            nick, server = nickname.split('@')
            debug(nick + ' @ '+ server)
            server = server.lower()
            if nick == None or server == None:
                raise ValueError('Invalid nickname given: ' + nickname)
            url = self.getUrlAccountID(nick, server)
            json_data = await getUrlJSON(self.session, url)
            for res in json_data['data']:
                if res['nickname'].lower() == nick.lower(): 
                    return res['account_id']
            raise ValueError('No WG account_id found: ' + nickname)
            
        except Exception as err:
            error(str(err))
            return None
    
    async def getPlayerTankStats(self, accountID: int, tankID : int, fields: list) -> dict:
        """Get player's stats (WR, # of battles) in a tank"""
        # debug('Started')        
        if self.session == None:
            error('Session must be initialized first')
            sys.exit(1)
        try:
            debug('AccountID: ' + str(accountID) + ' TankID: ' + str(tankID))
            url = self.getUrlPlayerTankStats(accountID, tankID, fields)
            json_data = await getUrlJSON(self.session, url, self.chkJSONtankList)
            if json_data != None:
                debug('JSON Response received: ' + str(json_data))
                return json_data['data'][str(accountID)][0]
        except Exception as err:
            error(err)
        return None

    async def getPlayerStats(self, accountID: int, fields: list, battle_time = None) -> dict:
        """Get player's global stats """
        # debug('Started')
        fields_missing  = fields
        cached_stats = None
        if self.cache != None:
            cached_stats, fields_missing = await self.getCachedPlayerStats(accountID, fields)

        if fields_missing != None:
            if self.session == None:
                error('Session must be initialized first')
                sys.exit(1)
            try:
                debug('AccountID: ' + str(accountID) + ' Fields: ' + ', '.join(fields_missing))
                url = self.getUrlPlayerStats(accountID, fields_missing)
                json_data = await getUrlJSON(self.session, url, self.chkJSONplayerStats)
                if json_data != None:
                    debug('JSON Response received: ' + str(json_data))
                    stats = json_data['data'][str(accountID)]
                    if self.statsQ != None:
                        self.statsQ.put([accountID, 'player', stats])
                    return self.mergePlayerStats(stats, cached_stats)
            except Exception as err:
                error('Unexpected Exception: ' + str(type(err)) + ' : ' +  str(err))
        else:
            return cached_stats
        return None

    async def getCachedPlayerStats(self, accountID, fields):
        if self.cache == None:
            error('No stats cache available')
            return None, fields            
        try:
            res_tmp = {}
            missing = []
            for field in fields:
                curr = self.cache.execute(self.sql_select_player_stats.format(accountID, field, NOW() ))
                res_tmp[field] = curr.fetchone()
                if res_tmp[field] == None:
                    res_tmp.pop(field, None)
                    missing.append(field)
                else:
                    res_tmp[field] = res_tmp[field][0]

            res = {}
            for field in res_tmp.keys():
                res = bldDictHierarcy(res, field, res_tmp[field])
                
            if len(missing) == 0:  
                missing = None
            if len(res.keys()) == 0:
                res = None
            return res, missing
        except KeyError as err:
            error('Key not found: ' + str(err))
        except Exception as err:
            error(str(err))
        return None, fields

    def mergePlayerStats(self, stats1: dict, stats2: dict) -> dict:
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
            error('Key not found: ' + str(err))
        return None


    def getTankData(self, tank_id: int, field: str):
        if self.tanks == None:
            return None
        try:
            return self.tanks['data'][str(tank_id)][field]
        except KeyError as err:
            error('Key not found: ' + str(err))
        return None

    async def statsSaver(self): 
        while True:
            stats = await self.statsQ.get()
            try:
                return stats  ## FIX
                
          

            except Exception as err:
                error(str(err))
        return None

class WoTinspector:
    URL_WI = 'https://replays.wotinspector.com'
    URL_REPLAYS     = URL_WI + '/en/sort/ut/page/'
    URL_REPLAY_DL   = URL_WI + '/en/download/'  
    URL_REPLAY_UL   = 'https://api.wotinspector.com/replay/upload?'
    URL_REPLAY_INFO = 'https://api.wotinspector.com/replay/upload?details=full&key='

    REPLAY_N = 1

    def __init__(self):
        self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session != None:
            await self.session.close()        

    async def getReplayJSON(self, replay_id: str):
        json_resp = await getUrlJSON(self.session, self.URL_REPLAY_INFO + replay_id, None)
        try:
            if self.chkJSONreplay(json_resp):
                return json_resp
            else:
                return None
        except Exception as err:
            error('Unexpected Exception: ' + str(type(err)) + ' : ' + str(err) )
            return None

    async def postReplay(self,  data, filename = 'Replay', account_id = 0, title = 'Replay', priv = False, N = None):
        try:
            N = N if N != None else self.REPLAY_N
            msg_str = 'Replay[' + str(N) + ']: '
            self.REPLAY_N += 1

            hash = hashlib.md5()
            hash.update(data)
            replay_id = hash.hexdigest()

            ##  Testing if the replay has already been posted
            json_resp = await self.getReplayJSON(replay_id)
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
            error(msg_str + 'Unexpected Exception: ' + str(type(err)) + ' : ' + str(err) )
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
                error(str(err))
            await asyncio.sleep(SLEEP)
            
        error(msg_str + ' Could not post replay: ' + title)
        return None

    @classmethod
    def getUrlReplayListing(cls, page : int):
        return cls.URL_REPLAYS + str(page) + '?vt=#filters'

    @classmethod
    def getReplayLinks(cls, doc: str):
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
            debug(str(err))
        return replay_links
    
    @classmethod
    def getReplayID(cls, url):
        return url.rsplit('/', 1)[-1]

    @classmethod
    def chkJSONreplay(cls, json_resp):
        """"Check String for being a valid JSON file"""
        try:
            if ('status' in json_resp) and json_resp['status'] == 'ok' and ('data' in json_resp) and json_resp['data'] != None:
                debug("JSON check OK")
                return True 
        except KeyError as err:
            error('Key :' + str(err) + ' not found')
        except:
            debug("JSON check failed: " + str(json_resp))
        return False      

class BlitzStars:

    URL_BlitzStars      = 'https://www.blitzstars.com'
    URL_playerStats     = URL_BlitzStars + '/api/playerstats'
    URL_playerTankStats = URL_BlitzStars + '/api/tanks'
    URL_tankAverages    = URL_BlitzStars + '/tankaverages.json'
    URL_activeplayers   = URL_BlitzStars +  '/api/playerstats/activeinlast30days'
    URL_playersTankStats= URL_BlitzStars +  '/api/top/forjylpah?'

    def __init__(self):
        self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session != None:
            await self.session.close()        

    @classmethod
    def getUrlTankAvgs(cls):
        return cls.URL_tankAverages

    @classmethod
    def getUrlPlayerStats(cls, accountID: int):
        return cls.URL_playerStats + '/' + str(accountID)

    @classmethod
    def getUrlPlayerTankStats(cls, accountID: int, tankID: int):
        return cls.URL_playerTankStats + '/' + str(accountID) + '/' + str(tankID)

    @classmethod
    def getUrlPlayersTankStats(cls, accountID: int):
        return cls.URL_playersTankStats + '&accountId=' + str(accountID) 

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
            error('Key :' + str(err) + ' not found')
        except:
            debug("JSON check failed")
        return False        
