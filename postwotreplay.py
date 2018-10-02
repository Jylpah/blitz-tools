#!/usr/bin/python3

import json, requests, sys, base64  

filename = str(sys.argv[1])

#Change this to your own WG id. Find it here: https://api.wotblitz.eu/wotb/account/list/?account_id%2Cnickname
# WG account id of the uploader:
uploader_account_ID = 0    

# edit this or use arg.parse() to add proper suppod for commandline arguments 
title="Some title"
url='https://wotinspector.com/api/replay/upload?title=' + title + '&details=full&uploaded_by=' + str(uploader_account_ID)

# additional option: 'private=1' to keep the replay way from public listing at WoTinspector.com/replays

with open(filename,'rb') as f:
   payload = {'file' : (filename, base64.b64encode(f.read()))}     
   r = requests.post(url, data=payload)
   print(r.json())