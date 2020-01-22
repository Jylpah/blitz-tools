# blitz-tools
Python scripts to analyze WoT Blitz replays and stats. All the scripts require [Python 3.7](https://www.python.org/downloads/) or higher. I am using 3.8 myself so you may have to change the Python interpreter version in the scripts' first line. 

### Replays
* [upload_wotb_replays.py](wotbreplay_uploader.py): An asynchronous replay uploader. 
* [analyze_wotb_replays.py](analyze_wotb_replays.py): A tool to calculate statistics of a set of replays. Upload the replays first with [upload_wotb_replays.py](upload_wotb_replays.py) and then calculate the statistics. 

### Tank data
* [get_tankopedia.py](get_tankopedia.py): Get the lastest tankopedia data from wotinspector.com. The script fetches only a limited set of data. You may edit to your needs. 
* [extract_tankopedia.py](extract_tankopedia.py): Extract Blitz data directly from the Blitz app files. You need to download the Blitz App APK package and unzip it to a folder. Requires [Python 3.7](https://www.python.org/downloads/) or higher.

### Utils
* [blitzutils.py](blitzutils.py): A utils class for various Blitz related to functions. *Other scripts use this*, so you need to place it to the same folder with the other scripts

### Data
* [tanks.json](tanks.json): Tankopedia in WG JSON format extracted from the Blitz app files with [extract_tankopedia.py]](extractTankopedia.py]). Contains only limited information required by the scripts. 
* [maps.json](maps.json): Blitz map names extracted from the Blitz app files with [extract_tankopedia.py]](extract_tankopedia.py]). 

### Quick instructions

1. Install [Python 3.8](https://www.python.org/downloads/) on to your computer/laptop
1. Download all the files including the [maps.json](maps.json) and [tanks.json](tanks.json) files
1. Install the required Python modules: aiohttp, aioconsole, aiofiles, beautifulsoup4, lxml, xmltodict

```
# Remember to run the command with a Python interpreter >= 3.7
python3.8 -m pip install motor aiohttp aioconsole aiofiles aiosqlite beautifulsoup4 lxml xmltodict progress
```

4. Upload your replays. Run _python.exe .\wotbreplay_uploader.py --help_ (Windows) or _./wotbreplay_uploader.py --help_ (Linux)
5. Analyze replays.  Run _python.exe .\analyzeReplays.py --help_ (Windows) or _./analyzeReplays.py --help_ (Linux)


You can reach me via email: Jylpah@gmail.com or [Discord](https://discordapp.com/): Jylpah#4662. Happy tanking and stats analysis!
