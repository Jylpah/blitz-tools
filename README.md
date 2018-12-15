# blitz-tools
Python scripts to analyze WoT Blitz replays and stats. All the scripts require [Python 3.7](https://www.python.org/downloads/) or higher.

### Replays
* [wotbreplay_uploader.py](wotbreplay_uploader.py): An asynchronous replay uploader. 
* [postwotreplay.py](postwotreplay.py): A simple WoT Replay uploader. Prints the received JSON from https://wotinspector.com
* [analyzeReplays.py](analyzeReplays.py): A tool to calculate statistics of a set of replays. Upload the replays first with [wotbreplay_uploader.py](wotbreplay_uploader.py) and then calculate the statistics. 

### Tank data
* [getTankopedia.py](getTankopedia.py): Get the lastest tankopedia data from wotinspector.com. The script fetches only a limited set of data. You may edit to your needs. 
* [extractTankopedia.py](extractTankopedia.py): Extract Blitz data directly from the Blitz app files. You need to download the Blitz App APK package and unzip it to a folder. Requires [Python 3.7](https://www.python.org/downloads/) or higher.

### Utils
* [blitzutils.py](blitzutils.py): A utils class for various Blitz related to functions. *Other scripts use this*, so you need to place it to the same folder with the other scripts

### Data
* [tanks.json](tanks.json): Tankopedia in WG JSON format extracted from the Blitz app files with [extractTankopedia.py]](extractTankopedia.py]). Contains only limited information required by the scripts. Blitz version 5.3. 
* [maps.json](maps.json): Blitz map names extracted from the Blitz app files with [extractTankopedia.py]](extractTankopedia.py]). Blitz version 5.3.

### Quick instructions

1. Install [Python 3.7](https://www.python.org/downloads/)
1. Download all the files including the [maps.json](maps.json) and [tanks.json](tanks.json) files
1. Upload your replays. Run _python.exe .\wotbreplay_uploader.py --help_ (Windows) or _./wotbreplay_uploader.py --help_ (Linux)
1. Analyze replays.  Run _python.exe .\analyzeReplays.py --help_ (Windows) or _./analyzeReplays.py --help_ (Linux)


You can reach me via email: Jylpah@gmail.com or [Discord](https://discordapp.com/): Jylpah#4662. Happy tanking and stats analysis!
