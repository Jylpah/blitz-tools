## Unmaintained

Please use [blitz-replays](https://github.com/Jylpah/blitz-replays) instead. It is faster and more feature-rich. 

# blitz-tools
Python scripts to analyze WoT Blitz replays and stats. All the scripts require [Python 3.7](https://www.python.org/downloads/) or later. 

### Replays
* [upload_wotb_replays.py](upload_wotb_replays.py): An asynchronous replay uploader. 
* [analyze_wotb_replays.py](analyze_wotb_replays.py): A tool to calculate statistics of a set of replays. **Upload the replays first** with [upload_wotb_replays.py](upload_wotb_replays.py) and then calculate the statistics with analyze_wotb_replays.py based on the resulting **.json** files. 

### Tank data
* [get_tankopedia.py](get_tankopedia.py): Get the lastest tankopedia data from wotinspector.com. The script fetches only a limited set of data. You may edit to your needs. 
* [extract_tankopedia.py](extract_tankopedia.py): Extract Blitz data directly from the Blitz app files. You need to download the Blitz App APK package and unzip it to a folder. Requires [Python 3.7](https://www.python.org/downloads/) or higher.

### Utils
* [blitzutils.py](blitzutils.py): A utils class for various Blitz related to functions. *Other scripts use this*, so you need to place it to the same folder with the other scripts

### Data
* [tanks.json](tanks.json): Tankopedia in WG JSON format extracted from the Blitz app files with [extract_tankopedia.py](extractTankopedia.py). Contains only limited information required by the scripts. 
* [maps.json](maps.json): Blitz map names extracted from the Blitz app files with [extract_tankopedia.py](extract_tankopedia.py). 

### Quick instructions

1. Install [Python 3.8](https://www.python.org/downloads/) or later to your computer/laptop
2. Install the required Python modules (see [requirements.txt](requirements.txt): motor, aiohttp, aioconsole, aiofiles, aiosqlite, beautifulsoup4, lxml, xmltodict, progress
```
python -m pip install -r requirements.txt
```
3. Clone the repository. [GitHub Desktop](https://desktop.github.com/) is an excellent GUI for `git`. This makes it easy to receive the updates via `git pull`. 
```
git clone https://github.com/Jylpah/blitz-tools.git
```
4. Upload your replays with `upload_wotb_replays.py` 
```
## Check the options
# Windows Command Prompt or Powershell
python.exe .\upload_wotb_replays.py --help

# Linux or Windows Subsystem for Linux (WSL)
./upload_wotb_replays.py --help

# Upload replays (Windows)
python.exe .\upload_wotb_replays.py REPLAY_FILE.wotbreplay  DIR_WITH_REPLAYS

# example
python.exe .\upload_wotb_replays.py 20200223_0122__jylpah_T-54_grossberg.wotbreplay
```
5. Analyze replays with `analyze_wotb_replays.py`
```
## Check options
# Windows Command Prompt or Powershell
python.exe .\analyze_wotb_replays.py --help

# Syntax (Windows)
python.exe .\analyze_wotb_replays.py REPLAY_FILE.wotbreplay.json [DIR_WITH_REPLAYS ...]

# Example (Windows)
python.exe .\analyze_wotb_replays.py 20200223_0122__jylpah_T-54_grossberg.wotbreplay.json

# Linux or Windows Subsystem for Linux (WSL)
./analyze_wotb_replays.py --help
```

### Blitz releases

[WoT Blitz releases](releases.md): launch dates and key content.

### Other

* Please remember to run the scripts from command line, not from the Python interpreter. On Windows open Command Prompt or Powershell. 
* [GitHub Desktop](https://desktop.github.com/) is the recommended way of downloading the scripts you can update the scripts easily via git/GitHub Deskop. To prevent GitHub Desktop from uploading your other files to github.com, you can "ignore" the files by right-clicking a file on GitHub Desktop or by manually editing the _.gitignore_ file.
* Even though the code has an option to use MongoDB, you should not bother with that. The MongoDB support is solely for my own purposes and you do not need MongoDB to run the scripts. 

You can reach me via email: [Jylpah@gmail.com](mailto:Jylpah@gmail.com) or [Discord](https://discordapp.com/): Jylpah#4662. Happy tanking and stats analysis!


## Examples

### upload_wotb_replays.py

```
% ./upload_wotb_replays.py ../../replays/2020-10-24
Replay[5]: T-54 @ Port Bay posted
Replay[6]: T54E1 @ Winter Malinovka posted
Replay[4]: T54E1 @ Canal posted
Replay[2]: T-54 @ Himmelsdorf posted
Replay[1]: Prototipo Standard B @ Oasis Palms posted
Replay[3]: Prototipo Standard B @ Yamato Harbor posted
Replay[7]: T-54 @ Fort Despair posted
Replay[8]: T-54 @ Mayan Ruins posted
Replay[11]: T-54 @ Desert Sands posted
Replay[9]: T-54 @ Canyon posted
Replay[10]: T-54 @ Hellas posted
Replay[12]: T-54 @ Rockfield posted
Replay[14]: IS-5 (Object 730) @ Vineyards posted
Replay[15]: Sturer Emil @ New Bay posted
Replay[13]: WZ-111G FT @ Canal posted
Replay[17]: SU-130PM @ Castilla posted
17 replays: 17 uploaded, 0 skipped, 0 errors
``` 
### analyze_wotb_replays.py

#### Default options

```
% ./analyze_wotb_replays.py ./replays/
Fetching player stats |███████████████████████████▉    | 1500/1727 86% ETA 0 h 0 mins


TOTAL_______________   Battles    % Battles     WR      DPB    Player WR   Allies WR   Enemies WR   Allies Btls   Enemies Btls
               Total :   125    :   100% :  52.0% :  1780 :    58.85% :    51.55% :     51.76% :       21353 :        20981

Battle Type_________   Battles    % Battles     WR      DPB    Player WR   Allies WR   Enemies WR   Allies Btls   Enemies Btls
           Encounter :    53    :    42% :  45.3% :  1626 :    58.85% :    51.96% :     52.39% :       20415 :        22003
           Supremacy :    72    :    58% :  56.9% :  1893 :    58.85% :    51.25% :     51.30% :       22044 :        20228

Tier________________   Battles    % Battles     WR      DPB    Player WR   Allies WR   Enemies WR   Allies Btls   Enemies Btls
         Bottom tier :    77    :    62% :  51.9% :  1758 :    58.85% :    51.88% :     51.84% :       21669 :        21755
            Top tier :    48    :    38% :  52.1% :  1814 :    58.85% :    51.02% :     51.65% :       20847 :        19738

Battle Mode_________   Battles    % Battles     WR      DPB    Player WR   Allies WR   Enemies WR   Allies Btls   Enemies Btls
       Burning Games :    4     :     3% :  25.0% :  1994 :    58.85% :    49.34% :     51.37% :       14873 :        17542
              Random :    89    :    71% :  60.7% :  1721 :    58.85% :    50.21% :     50.66% :       21980 :        21261
              Rating :    32    :    26% :  31.2% :  1915 :    58.85% :    55.56% :     54.87% :       20420 :        20632

Tank________________   Battles    % Battles     WR      DPB    Player WR   Allies WR   Enemies WR   Allies Btls   Enemies Btls
             E 75 TS :    1     :     1% : 100.0% :  3221 :    58.85% :    50.41% :     46.68% :       19687 :        30106
             Emil II :    55    :    44% :  45.5% :  2039 :    58.85% :    53.08% :     53.07% :       21485 :        21209
   IS-5 (Object 730) :    1     :     1% : 100.0% :  1062 :    58.85% :    51.84% :     53.28% :       18667 :        27114
                 Leo :    21    :    17% :  71.4% :  1364 :    58.85% :    50.42% :     50.70% :       23932 :        20707
                Löwe :    1     :     1% : 100.0% :  2402 :    58.85% :    46.91% :     48.25% :       23951 :        24234
Progetto M35 mod. 46 :    7     :     6% :  28.6% :  1653 :    58.85% :    52.51% :     53.32% :       12270 :        15514
Prototipo Standard B :    2     :     2% :  50.0% :  2190 :    58.85% :    50.94% :     53.95% :       23216 :        27703
             Strv 74 :    9     :     7% :  88.9% :  1036 :    58.85% :    49.75% :     48.56% :       15543 :        16848
         Sturer Emil :    1     :     1% : 100.0% :   699 :    58.85% :    52.00% :     50.42% :       18490 :        15746
            SU-130PM :    2     :     2% :   0.0% :  2033 :    58.85% :    48.57% :     51.73% :       14783 :        17109
               T 55A :    12    :    10% :  50.0% :  1735 :    58.85% :    49.48% :     50.43% :       22241 :        23411
                T-54 :    8     :     6% :  25.0% :  1802 :    58.85% :    49.56% :     50.45% :       28310 :        24777
T-54 first prototype :    1     :     1% :   0.0% :  1858 :    58.85% :    50.25% :     50.58% :       13556 :        17701
               T54E1 :    2     :     2% :  50.0% :  1750 :    58.85% :    51.00% :     51.63% :       25050 :        20464
             Type 62 :    1     :     1% :   0.0% :  1153 :    58.85% :    55.31% :     54.14% :       12027 :        13767
          WZ-111G FT :    1     :     1% : 100.0% :  3213 :    58.85% :    52.50% :     50.01% :       33311 :        22620

```

#### --mode team --hist

--mode team: Team stats
--hist:      Prints a histogram of alliied/enemy player stats

```
% ./analyze_wotb_replays.py --mode team --hist --extra room_type  ./replays/
Fetching player stats |███████████████████████████▉    | 1500/1727 86% ETA 0 h 0 mins

TOTAL_______________   Battles    Player WR   Allies WR   Enemies WR   Allies Btls   Enemies Btls   Allies Avg Dmg   Enemies Avg Dmg
               Total :   125    :    58.85% :    51.55% :     51.76% :       21353 :        20981 :        1130 :         1144

Battle Type_________   Battles    Player WR   Allies WR   Enemies WR   Allies Btls   Enemies Btls   Allies Avg Dmg   Enemies Avg Dmg
           Encounter :    53    :    58.85% :    51.96% :     52.39% :       20415 :        22003 :        1135 :         1169
           Supremacy :    72    :    58.85% :    51.25% :     51.30% :       22044 :        20228 :        1127 :         1125

Tier________________   Battles    Player WR   Allies WR   Enemies WR   Allies Btls   Enemies Btls   Allies Avg Dmg   Enemies Avg Dmg
         Bottom tier :    77    :    58.85% :    51.88% :     51.84% :       21669 :        21755 :        1193 :         1184
            Top tier :    48    :    58.85% :    51.02% :     51.65% :       20847 :        19738 :        1030 :         1079

Battle Mode_________   Battles    Player WR   Allies WR   Enemies WR   Allies Btls   Enemies Btls   Allies Avg Dmg   Enemies Avg Dmg
       Burning Games :    4     :    58.85% :    49.34% :     51.37% :       14873 :        17542 :        1021 :         1049
              Random :    89    :    58.85% :    50.21% :     50.66% :       21980 :        21261 :        1062 :         1079
              Rating :    32    :    58.85% :    55.56% :     54.87% :       20420 :        20632 :        1335 :         1337

Tank________________   Battles    Player WR   Allies WR   Enemies WR   Allies Btls   Enemies Btls   Allies Avg Dmg   Enemies Avg Dmg
             E 75 TS :    1     :    58.85% :    50.41% :     46.68% :       19687 :        30106 :        1035 :          829
             Emil II :    55    :    58.85% :    53.08% :     53.07% :       21485 :        21209 :        1252 :         1263
   IS-5 (Object 730) :    1     :    58.85% :    51.84% :     53.28% :       18667 :        27114 :        1204 :         1120
                 Leo :    21    :    58.85% :    50.42% :     50.70% :       23932 :        20707 :        1027 :         1030
                Löwe :    1     :    58.85% :    46.91% :     48.25% :       23951 :        24234 :         933 :          919
Progetto M35 mod. 46 :    7     :    58.85% :    52.51% :     53.32% :       12270 :        15514 :        1108 :         1123
Prototipo Standard B :    2     :    58.85% :    50.94% :     53.95% :       23216 :        27703 :        1019 :         1307
             Strv 74 :    9     :    58.85% :    49.75% :     48.56% :       15543 :        16848 :         893 :          838
         Sturer Emil :    1     :    58.85% :    52.00% :     50.42% :       18490 :        15746 :        1143 :          972
            SU-130PM :    2     :    58.85% :    48.57% :     51.73% :       14783 :        17109 :         985 :         1024
               T 55A :    12    :    58.85% :    49.48% :     50.43% :       22241 :        23411 :        1058 :         1103
                T-54 :    8     :    58.85% :    49.56% :     50.45% :       28310 :        24777 :        1051 :         1147
T-54 first prototype :    1     :    58.85% :    50.25% :     50.58% :       13556 :        17701 :        1023 :          971
               T54E1 :    2     :    58.85% :    51.00% :     51.63% :       25050 :        20464 :        1148 :         1109
             Type 62 :    1     :    58.85% :    55.31% :     54.14% :       12027 :        13767 :        1082 :         1317
          WZ-111G FT :    1     :    58.85% :    52.50% :     50.01% :       33311 :        22620 :        1239 :         1070

Player Histograms______
Win rate     | Allies        | Enemies       | TOTAL
0% - 35%     |     0 ( 0.0%) |     0 ( 0.0%) |     0 ( 0.0%)
35% - 40%    |     3 ( 0.4%) |     8 ( 0.9%) |    11 ( 0.7%)
40% - 45%    |    72 ( 9.9%) |   104 (11.9%) |   176 (11.0%)
45% - 48%    |   142 (19.5%) |   148 (16.9%) |   290 (18.1%)
48% - 50%    |   121 (16.6%) |   111 (12.7%) |   232 (14.5%)
50% - 52%    |   108 (14.9%) |   132 (15.1%) |   240 (15.0%)
52% - 55%    |   110 (15.1%) |   144 (16.5%) |   254 (15.9%)
55% - 60%    |   104 (14.3%) |   140 (16.0%) |   244 (15.2%)
60% - 65%    |    39 ( 5.4%) |    60 ( 6.9%) |    99 ( 6.2%)
65% - 70%    |    18 ( 2.5%) |    16 ( 1.8%) |    34 ( 2.1%)
70% -        |    10 ( 1.4%) |    12 ( 1.4%) |    22 ( 1.4%)

Avg. Dmg.    | Allies        | Enemies       | TOTAL
0 - 200      |     0 ( 0.0%) |     1 ( 0.1%) |     1 ( 0.1%)
200 - 300    |     0 ( 0.0%) |     2 ( 0.2%) |     2 ( 0.1%)
300 - 400    |     6 ( 0.8%) |     8 ( 0.9%) |    14 ( 0.9%)
400 - 500    |    14 ( 1.9%) |    22 ( 2.5%) |    36 ( 2.2%)
500 - 600    |    31 ( 4.3%) |    30 ( 3.4%) |    61 ( 3.8%)
600 - 700    |    45 ( 6.2%) |    61 ( 7.0%) |   106 ( 6.6%)
700 - 800    |    52 ( 7.2%) |    70 ( 8.0%) |   122 ( 7.6%)
800 - 900    |    67 ( 9.2%) |    80 ( 9.1%) |   147 ( 9.2%)
900 - 1000   |    77 (10.6%) |    82 ( 9.4%) |   159 ( 9.9%)
1000 - 1100  |    95 (13.1%) |    80 ( 9.1%) |   175 (10.9%)
1100 - 1200  |    75 (10.3%) |    80 ( 9.1%) |   155 ( 9.7%)
1200 - 1300  |    66 ( 9.1%) |    83 ( 9.5%) |   149 ( 9.3%)
1300 - 1400  |    44 ( 6.1%) |    62 ( 7.1%) |   106 ( 6.6%)
1400 - 1500  |    34 ( 4.7%) |    43 ( 4.9%) |    77 ( 4.8%)
1500 - 1750  |    63 ( 8.7%) |   104 (11.9%) |   167 (10.4%)
1750 - 2000  |    33 ( 4.5%) |    35 ( 4.0%) |    68 ( 4.2%)
2000 - 2250  |    16 ( 2.2%) |    19 ( 2.2%) |    35 ( 2.2%)
2250 - 2500  |     6 ( 0.8%) |     7 ( 0.8%) |    13 ( 0.8%)
2500 - 2750  |     3 ( 0.4%) |     4 ( 0.5%) |     7 ( 0.4%)
2750 - 3000  |     0 ( 0.0%) |     1 ( 0.1%) |     1 ( 0.1%)
3000 -       |     0 ( 0.0%) |     1 ( 0.1%) |     1 ( 0.1%)

Battles      | Allies        | Enemies       | TOTAL
0k - 1k      |     9 ( 1.2%) |     8 ( 0.9%) |    17 ( 1.1%)
1k - 2k      |    20 ( 2.8%) |    20 ( 2.3%) |    40 ( 2.5%)
2k - 5k      |    46 ( 6.3%) |    58 ( 6.6%) |   104 ( 6.5%)
5k - 7k      |    84 (11.6%) |   116 (13.3%) |   200 (12.5%)
7k - 10k     |    96 (13.2%) |   112 (12.8%) |   208 (13.0%)
10k - 15k    |   103 (14.2%) |   126 (14.4%) |   229 (14.3%)
15k - 25k    |   145 (19.9%) |   167 (19.1%) |   312 (19.5%)
25k - 50k    |   167 (23.0%) |   204 (23.3%) |   371 (23.2%)
50k -        |    57 ( 7.8%) |    64 ( 7.3%) |   121 ( 7.6%)
```
