import os
from dotenv import load_dotenv
import urllib
import urllib.request
import json

def fdp_request(url):
    load_dotenv()
    fdp_url = os.getenv("FDP_URL")

    request = urllib.request.Request(fdp_url+'/'+url)
    request.add_header("User-Agent", "cheese")
    response = urllib.request.urlopen(request).read()
    response_json = json.loads(response)
    return response_json