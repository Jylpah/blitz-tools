#!/usr/bin/python3.7

# Extract tankopedia data in WG API JSON format from Blitz app files (Android APK unzipped)

import sys, argparse, json, os, inspect, aiohttp, asyncio, aiofiles, re, logging, time, xmltodict, collections
from blitzutils import WG

logging.getLogger("asyncio").setLevel(logging.DEBUG)

BLITZAPP_STRINGS='/assets/Data/Strings/en.yaml'
BLITZAPP_VEHICLES_DIR='/assets/Data/XML/item_defs/vehicles/'
BLITZAPP_VEHICLE_FILE='/list.xml'

VERBOSE = False
DEBUG = False

wg = WG()
NATION_ID = {}
for i in range(len(wg.nations)): 
    NATION_ID[wg.nations[i]] = i 

## main() -------------------------------------------------------------
async def main(argv):
    global VERBOSE, DEBUG

    parser = argparse.ArgumentParser(description='Extract Tankopedia data from Blitz game files')
    parser.add_argument('blitzAppBase', type=str,  metavar="BLITZAPP_FOLDER", default=".", help='Base dir of the Blitz App files')
    parser.add_argument('file', type=str, metavar="FILE", help='Write Tankopedia to file')
    parser.add_argument('-fm', '--file-maps', dest='file_maps', type=str, default=None, help='File to write map names')
    parser.add_argument('-d', '--debug', action='store_true', default=False, help='Debug mode')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Verbose mode')
        
    args = parser.parse_args()

    VERBOSE = args.verbose
    DEBUG = args.debug
    if DEBUG: VERBOSE = True

    tasks = []
    for nation in wg.nations:
        tasks.append(asyncio.create_task(extractTanks(args.blitzAppBase, nation)))

    tanklist = []
    for tanklist_tmp in await asyncio.gather(*tasks):
        tanklist.extend(tanklist_tmp)
    
    tank_strs, map_strs = await readUserStrs(args.blitzAppBase)

    async with aiofiles.open(args.file, 'w', encoding="utf8") as outfile:
        tankopedia = {}
        tankopedia['status'] = 'ok'
        tankopedia['meta'] = { "count":  len(tanklist) }
        tankopedia['data'], tankopedia['userStr'] = await convertTankNames(tanklist, tank_strs)
        #debug(tankopedia)
        await outfile.write(json.dumps(tankopedia, ensure_ascii=False, indent=4, sort_keys=False))
    
    if args.file_maps != None:
        async with aiofiles.open(args.file_maps, 'w', encoding="utf8") as outfile:
            await outfile.write(json.dumps(map_strs, ensure_ascii=False, indent=4, sort_keys=True))

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

async def readUserStrs(blitzAppBase : str) -> dict:
    """Read user strings to convert map and tank names"""
    tank_strs = {}
    map_strs = {}
    filename = blitzAppBase + BLITZAPP_STRINGS
    debug('Opening file: ' + filename + ' for reading UserStrings')
    try:
        async with aiofiles.open(filename, 'r', encoding="utf8") as f:
            p_tank = re.compile('^"(#\\w+?_vehicles:.+?)": "(.+)"$')
            p_map = re.compile('^"#maps:(.+?):.+?: "(.+?)"$')
            
            async for l in f:
                m = p_tank.match(l)
                if m != None: 
                    tank_strs[m.group(1)] = m.group(2)
                
                m = p_map.match(l)
                if m != None:
                    map_strs[m.group(1)] = m.group(2)   
    
    except Exception as err:
        error(err)
        sys.exit(1)

    return tank_strs, map_strs
    
async def convertTankNames(tanklist : list, tank_strs: dict) -> dict:
    """Convert tank names for Tankopedia"""
    tankopedia = {}
    userStrs = {}
    tank_ids = []
    try:
        for tank in tanklist:
            tank['name'] = tank_strs[tank['userStr']]
            userStrs[tank['userStr'].split(':')[1]] = tank['name']
            tank.pop('userStr', None)
            tankopedia[str(tank['tank_id'])] = tank
            tank_ids.append(tank['tank_id'])   # for sorting...

        tank_ids.sort()
        tankopedia_sorted = collections.OrderedDict()
        for tank_id in tank_ids:
            tankopedia_sorted[str(tank_id)] = tankopedia[str(tank_id)]

    except Exception as err:
        error(err)
        sys.exit(1)
    return tankopedia_sorted, userStrs

async def getTankID(nation: str, tankID : int) -> int:
    return (tankID << 8) + (NATION_ID[nation] << 4) + 1 

async def getTankType(tagstr : str):
    tags = tagstr.split(' ')
    for t_type in wg.tank_type:
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
