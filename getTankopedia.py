#!/usr/local/bin/python3.7    

# Get Tankpedia from WoTinspector.com

import sys, argparse, json, os, inspect, aiohttp, asyncio, re, logging
from pathlib import Path

logging.getLogger("asyncio").setLevel(logging.DEBUG)

VERBOSE = False
DEBUG   = False

tank_type = [ 'lightTank', 'mediumTank', 'heavyTank', 'AT-SPG' ]

#@asyncio.coroutine
async def get_tankopedia(filename: str):
    url="https://wotinspector.com/static/armorinspector/tank_db_blitz.js"
    async with aiohttp.ClientSession() as client:
        async with client.get(url) as r:
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
                m = p.match(line)
                tank = {}
                tank['tank_id'] = int(m.group(1))
                tank['name'] = m.group(2)
                tank['tier'] = int(m.group(3))
                tank['type'] = tank_type[int(m.group(4))]
                tank['is_premium'] = (int(m.group(5)) == 1)
                tanks[str(m.group(1))] = tank
                n += 1

            tankopedia = {}
            tankopedia['status'] = "ok"
            tankopedia['meta'] = {"count" : n}
            tankopedia['data'] = tanks
            print("Tankopedia has " + str(n) + " tanks in: " + filename)
            with open(filename,'w') as outfile:
                outfile.write(json.dumps(tankopedia, ensure_ascii=False, indent=4, sort_keys=True))
            return None


def main(argv):
    global VERBOSE, DEBUG

    parser = argparse.ArgumentParser(description='Retrieve Tankopedia from WoTinspector.com')
    parser.add_argument('--file', dest="outfile", help='Write Tankopedia to file')
    parser.add_argument('--debug', '-d', action='store_true', default=False, help='Debug mode')
    parser.add_argument('--verbose', '-v', action='store_true', default=False, help='Verbose mode')
    
    args = parser.parse_args()
    VERBOSE = args.verbose
    DEBUG = args.debug
    if DEBUG: VERBOSE = True

    asyncio.run(get_tankopedia(args.outfile))
    

if __name__ == "__main__":
   main(sys.argv[1:])
