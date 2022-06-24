import os
from dotenv import load_dotenv
import urllib
import urllib.request
import json

def fdp_request(url):
    load_dotenv()
    fdp_url = os.getenv("FDP_URL")

    FDP_ERROR = True
    while FDP_ERROR:
        try:
            request = urllib.request.Request(fdp_url+'/'+url)
            request.add_header("User-Agent", "cheese")
            response = urllib.request.urlopen(request).read()
            response_json = json.loads(response)
            FDP_ERROR = False
        except:
            reason = "exception when requesting {}".format(fdp_url+'/'+url)
            response_json = {"status":"ko", "reason":reason}
            FDP_ERROR = True
            print(response_json)
    
    return response_json


