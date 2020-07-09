# blitz-tools
Python scripts to analyze WoT Blitz replays and stats. All the scripts require [Python 3.7](https://www.python.org/downloads/) or higher. I am using 3.8 myself so you may have to change the Python interpreter version in the scripts' first line. 

### Replays
* [upload_wotb_replays.py](upload_wotb_replays.py): An asynchronous replay uploader. 
* [analyze_wotb_replays.py](analyze_wotb_replays.py): A tool to calculate statistics of a set of replays. **Upload the replays first** with [upload_wotb_replays.py](upload_wotb_replays.py) and then calculate the statistics with analyze_wotb_replays.py based on the resulting **.json** files. 

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
2. Install the required Python modules: motor, aiohttp, aioconsole, aiofiles, aiosqlite, beautifulsoup4, lxml, xmltodict, progress
```
# Remember to run the command with a Python interpreter >= 3.7
python -m pip install motor aiohttp aioconsole aiofiles aiosqlite beautifulsoup4 lxml xmltodict progress 
```
3. Download all the files including the [maps.json](maps.json) and [tanks.json](tanks.json) files. A recommened way is to install [GitHub Desktop](https://desktop.github.com/) and clone the repository via _git_. This makes it easy to receive the updates. 

4. Upload your replays. 
```
# Windows, run directly from Command Prompt or Powershell
# Check the options
python.exe .\upload_wotb_replays.py --help

# Syntax
python.exe .\upload_wotb_replays.py REPLAY_FILE.wotbreplay

# example
python.exe .\upload_wotb_replays.py 20200223_0122__jylpah_T-54_grossberg.wotbreplay

# Linux
./upload_wotb_replays.py --help
```
5. Analyze replays. 
```
# Windows, run directly from Command Prompt or Powershell
python.exe .\analyze_wotb_replays.py --help

# Syntax
python.exe .\analyze_wotb_replays.py REPLAY_FILE.wotbreplay.json

# Example
python.exe .\analyze_wotb_replays.py 20200223_0122__jylpah_T-54_grossberg.wotbreplay.json

# Linux
./analyze_wotb_replays.py --help
```

### Blitz releases

[WoT Blitz releases](releases.md): launch dates and key content.

### Other

* Please remember to run the scripts from command line, not from the Python interpreter. On Windows open Command Prompt or Powershell. 
* Even though the code has an option to use MongoDB, you should not bother with that. The MongoDB support is solely for my own purposes and you do not need MongoDB to run the scripts. 

You can reach me via email: [Jylpah@gmail.com](mailto:Jylpah@gmail.com) or [Discord](https://discordapp.com/): Jylpah#4662. Happy tanking and stats analysis!
