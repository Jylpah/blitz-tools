#!/usr/bin/python3.7    

# Extract tankopedia data in WG API JSON format from Blitz app files (Android APK unzipped)

import sys, argparse, json, os, inspect, aiohttp, asyncio, aiofiles, re, logging, time, xmltodict

logging.getLogger("asyncio").setLevel(logging.DEBUG)

BLITZAPP_STRINGS='/assets/Data/Strings/en.yaml'
BLITZAPP_VEHICLES_DIR='/assets/Data/XML/item_defs/vehicles/'
BLITZAPP_VEHICLE_FILE='/list.xml'

VERBOSE = False
DEBUG = False

NATIONS = [ 'ussr', 'germany', 'usa', 'china', 'france', 'uk', 'japan', 'other']
NATION_ID = {}
for i in range(len(NATIONS)): 
    NATION_ID[NATIONS[i]] = i 

TANK_TYPE = [ 'lightTank', 'mediumTank', 'heavyTank', 'AT-SPG' ]

## main() -------------------------------------------------------------
async def main(argv):
    global VERBOSE, DEBUG

    parser = argparse.ArgumentParser(description='Extract Tankopedia data from Blitz game files')
    parser.add_argument('blitzAppBase', type=str,  metavar="BLITZAPP_FOLDER", default=".", help='Base dir of the Blitz App files')
    parser.add_argument('file', type=str, metavar="FILE", help='Write Tankopedia to file')
    parser.add_argument('-d', '--debug', action='store_true', default=False, help='Debug mode')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Verbose mode')
        
    args = parser.parse_args()

    VERBOSE = args.verbose
    DEBUG = args.debug
    if DEBUG: VERBOSE = True

    tasks = []
    for nation in NATIONS:
        tasks.append(asyncio.create_task(extractTanks(args.blitzAppBase, nation)))

    tanklist = []
    for tanklist_tmp in await asyncio.gather(*tasks):
        tanklist.extend(tanklist_tmp)
    
    async with aiofiles.open(args.file, 'w', encoding="utf8") as outfile:
        tankopedia = {}
        tankopedia['status'] = 'ok'
        tankopedia['meta'] = { "count":  len(tanklist) }
        (tankopedia['data'], tankopedia['userStr']) = await convertTankName(args.blitzAppBase, tanklist)

        #print(tankopedia)
        await outfile.write(json.dumps(tankopedia, ensure_ascii=False, indent=4, sort_keys=True))

    return None
    
async def extractTanks(blitzAppBase : str, nation: str):

    tanks = []
    list_xml_file = blitzAppBase + BLITZAPP_VEHICLES_DIR + nation + BLITZAPP_VEHICLE_FILE
    if not os.path.isfile(list_xml_file): 
        print('ERROR: cannot open ' + list_xml_file)
        return None
    debug('Opening file: ' + list_xml_file + ' (Nation: ' + nation + ')')
    async with aiofiles.open(list_xml_file, 'r') as f:
        try: 
            tankList = xmltodict.parse(await f.read())
            for data in tankList['root'].keys():
                tank_xml = tankList['root'][data]
                tank = {}
                tank['tank_id'] = await getTankID(nation, int(tank_xml['id']))
                tank['userStr'] = tank_xml['userString']
                tank['nation'] = nation
                tank['tier'] = int(tank_xml['level'])
                #debug(tank_xml['price'])
                tank['is_premium'] = issubclass(type(tank_xml['price']), dict)
                tank['type'] = await getTankType(tank_xml['tags'])
                tanks.append(tank)
        except Exception as err:
            error(err)
            sys.exit(2)
    return tanks

async def convertTankName(blitzAppBase : str , tanklist : list) -> dict:
    """Convert list.xml UserStrings to tank names for Tankopedia"""
    strings = {}   
    filename = blitzAppBase + BLITZAPP_STRINGS
    debug('Opening file: ' + filename + ' for reading UserStrings')
    try:
        async with aiofiles.open(filename, 'r', encoding="utf8") as f:
            str2find = '_vehicles:'
            p = re.compile('"(.+?)": "(.+)"')
            async for l in f:
                if l.find(str2find) > -1:
                    m = p.match(l)
                    strings[m.group(1)] = m.group(2)

        tankdict = {}
        userStrs = {}
        for tank in tanklist:
            tank['name'] = strings[tank['userStr']]
            userStrs[tank['userStr'].split(':')[1]] = tank['name']
            tank.pop('userStr', None)
            tankdict[str(tank['tank_id'])] = tank
    except Exception as err:
        error(err)
        sys.exit(1)
    return tankdict, userStrs

async def getTankID(nation: str, tankID : int) -> int:
    return (tankID << 8) + (NATION_ID[nation] << 4) + 1 

async def getTankType(tagstr : str):
    tags = tagstr.split(' ')
    for t_type in TANK_TYPE:
        if tags[0] == t_type:
            return t_type
    return None

def verbose(msg = ""):
    """Print a message"""
    if VERBOSE:
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        caller = calframe[1][3]
        print(caller + '(): ' + msg)
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
    
   #asyncio.run(main(sys.argv[1:]), debug=True)
   asyncio.run(main(sys.argv[1:]))
