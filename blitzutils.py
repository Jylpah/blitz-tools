#!/usr/bin/python3.7

import sys, os, json, asyncio, aiofiles, aiohttp, base64, urllib, inspect

MAX_RETRIES= 3
SLEEP = 2
DEBUG = False
VERBOSE = False
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
    global DEBUG, VERBOSE
    if debug != None:
        DEBUG = debug
    if DEBUG: VERBOSE = True

def setVerbose(verbose: bool):
    global VERBOSE
    if verbose != None:
        VERBOSE = verbose
        
def verbose(msg = ""):
    """Print a message"""
    if VERBOSE:
        print(msg)
    return None

def printWaiter(force = False):
    if VERBOSE  or force:
        print('.', end='', flush=True)

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

async def saveJSON(filename: str, json_data: dict, sort_keys = False) -> bool:
    """Save JSON data into file"""
    try:
        dirname = os.path.dirname(filename)
        if not os.path.isdir(dirname):
            os.makedirs(dirname, 0o770-UMASK)
        async with aiofiles.open(filename,'w', encoding="utf8") as outfile:
            await outfile.write(json.dumps(json_data, ensure_ascii=False, indent=4, sort_keys=sort_keys))
            return True
    except Exception as err:
        error(str(err))
    return False

async def getUrlJSON(session: aiohttp.ClientSession, url: str, chkJSONfunc = None) -> dict:
        """Retrieve (GET) an URL and return JSON object"""
        try:
            debug(url)
            ## To avoid excessive use of servers            
            for retry in range(1,MAX_RETRIES+1):
                async with session.get(url) as resp:
                    if resp.status == 200:
                        debug('HTTP request OK')
                        json_resp = await resp.json()       
                        if (chkJSONfunc == None) or chkJSONfunc(json_resp):
                            debug("Received valid JSON: " + str(json_resp))
                            return json_resp
                        else:
                            debug('Received JSON error.')                            
                    if retry == MAX_RETRIES:                        
                        raise aiohttp.ClientError('Request failed: ' + str(resp.status) + ' JSON Response: ' + str(json_resp) )
                    verbose('Retrying URL [' + str(retry) + ']: ' + url )
                    await asyncio.sleep(SLEEP)

        except aiohttp.ClientError as err:
            error("Could not retrieve URL: " + url)
            error(str(err))
        except asyncio.CancelledError as err:
            error('Queue gets cancelled while still working.')
        
        except Exception as err:
            error('Unexpected Exception: ' + str(err))
        return None

class WG:

    URL_WG_clanInfo         = 'clans/info/?application_id='
    URL_WG_playerTankList   = 'tanks/stats/?fields=tank_id%2Clast_battle_time&application_id='
    URL_WG_playerTankStats  = 'tanks/stats/?application_id='
    URL_WG_accountID        = 'account/list/?fields=account_id%2Cnickname&application_id='
    URL_WG_playerStats      = 'account/info/?application_id='
  
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

    nations = [ 'ussr', 'germany', 'usa', 'china', 'france', 'uk', 'japan', 'other']    
    nation_id = {
        'ussr'      : 0,
        'germany'   : 1, 
        'usa'       : 2, 
        'china'     : 3,
        'france'    : 4,
        'uk'        : 5,
        'japan'     : 6,
        'other'     : 7
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
        self.tanks = None        
        if (tankopedia_fn != None):
            if os.path.exists(tankopedia_fn) and os.path.isfile(tankopedia_fn):
                try:
                    with open(tankopedia_fn, 'rt', encoding='utf8') as f:
                        self.tanks = json.loads(f.read())
                except Exception as err:
                    error('Could not read tankopedia: ' + tankopedia_fn + '\n' + str(err))  
            else:
                verbose('Could not find Tankopedia file: ' + tankopedia_fn)    
        if (maps_fn != None):
            if os.path.exists(maps_fn) and os.path.isfile(maps_fn):
                try:
                    with open(maps_fn, 'rt', encoding='utf8') as f:
                        self.maps = json.loads(f.read())
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


        

    ## Class methods  ------------------------------

    @classmethod
    def getServer(cls, accountID: int) -> str:
        """Get Realm/server of an account based on account ID"""
        # for server in cls.accountID_server.keys():
        #     if accountID in cls.accountID_server[server]:
        #         return server
        # print('ERROR: AccountID not in range: ' + str(accountID))
        
        # faster, but can fail for negatives
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
    def chkJSONresponse(cls, json_resp: dict) -> bool:
        try:
            if ('status' in json_resp) and (json_resp['status'] == 'error'):
                debug(str(json_resp['error']['code']) + ' : ' + json_resp['error']['message'] )
                return False
            return True
        except:
            error("JSON format error")
            return False

    @classmethod
    def chkJSONgetAccountID(cls, json_resp: dict) -> bool:
        try:
            if (json_resp['status'] == 'ok'):
                return True                
            else:
                error_msg = str(json_resp['error']['code']) + ' : ' + json_resp['error']['message']
                raise ValueError('Received JSON error: ' + error_msg)    
        except KeyError as err:
            error('Key :' + str(err) + ' not found')
        except Exception as err:
            error(str(err))
        return False

    @classmethod
    def chkJSONcontent(cls, json_obj, check = None) -> bool:
        try:
            if (check == 'player') and (not cls.chkJSONplayer(json_obj)): 
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
    def chkJSONplayer(cls, playerInfo: dict) -> bool:
        """"Check String for being a valid Player JSON file"""
        try:
            accountID = playerInfo[0]['account_id']
            if int(accountID) > 0:
                return True
        except KeyError as err:
            error('Key :' + str(err) + ' not found')
        except:
            debug("JSON check failed")
        return False
    
    @classmethod
    def chkJSONtank(cls, tankInfo: dict) -> bool:
        """"Check String for being a valid Tank JSON file"""
        try:
            if int(tankInfo[0]['tank_id']) > 0:
                return True
        except KeyError as err:
            error('Key :' + str(err) + ' not found')
        except:
            debug("JSON check failed")
        return False
    
    @classmethod
    def chkJSONtankStats(cls, playerTankStat: dict) -> bool:
        """"Check String for being a valid Tank JSON file"""
        try:
            if playerTankStat['status'] != 'ok': 
                return False
            for acc in playerTankStat['data']:
                if playerTankStat['data'][acc] == None:
                    return False 
            return True
        except KeyError as err:
            error('Key :' + str(err) + ' not found')
        except:
            debug("JSON check failed")
        return False

    @classmethod    
    def chkJSONplayerStats(cls, playerStat: dict) -> bool:
        """"Check String for being a valid Tank JSON file"""
        try:
            if playerStat['status'] != 'ok': 
                return False
            for acc in playerStat['data']:
                if playerStat['data'][acc] == None:
                    return False 
            return True
        except KeyError as err:
            error('Key :' + str(err) + ' not found')
        except:
            debug("JSON check failed")
        return False

    @classmethod
    def chkJSONtankList(cls, tankList: dict) -> bool:
        """"Check String for being a valid Tank list JSON file"""
        try:
            accountID = next(iter(tankList['data']))
            tankID = tankList['data'][accountID][0]['tank_id']
            if int(tankID) > 0:
                debug('JSON tank list check OK')
                return True
        except Exception as err:
            error('JSON check FAILED: ' + str(err))            
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
        return url + '&fields=' + '%2C'.join(fields)

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
            json_data = await getUrlJSON(self.session, url, self.chkJSONtankStats)
            if json_data != None:
                debug('JSON Response received: ' + str(json_data))
                return json_data['data'][str(accountID)][0]
        except Exception as err:
            error(err)
        return None

    async def getPlayerStats(self, accountID: int, fields: list) -> dict:
        """Get player's global stats """
        # debug('Started')        
        if self.session == None:
            error('Session must be initialized first')
            sys.exit(1)
        try:
            debug('AccountID: ' + str(accountID) + ' Fields: ' + ', '.join(fields))
            url = self.getUrlPlayerStats(accountID, fields)
            json_data = await getUrlJSON(self.session, url, self.chkJSONplayerStats)
            if json_data != None:
                debug('JSON Response received: ' + str(json_data))                
                return json_data['data'][str(accountID)]
        except Exception as err:
            error(err)
        return None

    def getTankData(self, tank_id: int, field: str):
        if self.tanks == None:
            return None
        try:
            return self.tanks['data'][str(tank_id)][field]
        except KeyError as err:
            error('Key not found: ' + str(err))
        return None
        

class BlitzStars:

    URL_BlitzStars      = 'https://www.blitzstars.com'
    URL_playerStats     = URL_BlitzStars + '/api/playerstats'
    URL_playerTankStats = URL_BlitzStars + '/api/tanks'
    URL_tankAverages    =      URL_BlitzStars + '/tankaverages.json'
    URL_activeplayers   =     URL_BlitzStars +  '/api/playerstats/activeinlast30days'
    URL_playersTankStats= URL_BlitzStars +  '/api/top/forjylpah?'

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
    def getUrlPlayersTankStats(cls, tankID: int, account_list: list):
        return cls.URL_playersTankStats + 'tankId=' + str(tankID) + '&accountIds=' + ','.join(str(x) for x in account_list)

    @classmethod
    def getUrlActivePlayers(cls):
        return cls.URL_activeplayers
