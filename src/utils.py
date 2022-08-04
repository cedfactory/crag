import os
from dotenv import load_dotenv
import urllib
import urllib.parse
import urllib.request
import json

def fdp_request(url):
    load_dotenv()
    fdp_url = os.getenv("FDP_URL")
    if not fdp_url or fdp_url == "":
        return {"status":"ko", "info":"fdp url not found"}

    n_attempts = 3
    while n_attempts > 0:
        try:
            request = urllib.request.Request(fdp_url+'/'+url)
            request.add_header("User-Agent", "cheese")
            response = urllib.request.urlopen(request).read()
            response_json = json.loads(response)
            break
        except:
            reason = "exception when requesting {}".format(fdp_url+'/'+url)
            response_json = {"status":"ko", "info":reason}
            n_attempts = n_attempts - 1
            print('FDP ERROR : ', reason)
    
    return response_json

def fdp_request_post(url, params):
    load_dotenv()
    fdp_url = os.getenv("FDP_URL")

    request = urllib.request.Request(fdp_url+'/'+url, urllib.parse.urlencode(params).encode())
    response = urllib.request.urlopen(request).read().decode()
    return json.loads(response)
