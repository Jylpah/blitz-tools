# blitz-tools
Python scripts to analyze WoT Blitz replays and stats:

* getTankopedia.py: Get the lastest tankopedia data from wotinspector.com. The script fetches only a limited set of data. You may edit to your needs. 
* extractTankopedia.py: Extract Blitz data directly from the Blitz app files. You need to download the Blitz App APK package and unzip it to a folder.  
* [postwotreplay.py](postwotreplay.py): A simple WoT Replay uploader. Prints the received JSON from https://wotinspector.com
* [wotbreplay_uploader.py](wotbreplay_uploader.py): An asynchronous replay uploader. Requires [Python 3.7](https://www.python.org/downloads/)
