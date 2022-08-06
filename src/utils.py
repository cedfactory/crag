import os
from dotenv import load_dotenv
import urllib
import urllib.parse
import urllib.request
import json
import concurrent.futures

def _atomic_fdp_request(url):
    n_attempts = 3
    while n_attempts > 0:
        try:
            request = urllib.request.Request(url)
            request.add_header("User-Agent", "cheese")
            response = urllib.request.urlopen(request).read()
            response_json = json.loads(response)
            break
        except:
            reason = "exception when requesting {}".format(url)
            response_json = {"status":"ko", "info":reason}
            n_attempts = n_attempts - 1
            print('FDP ERROR : ', reason)
    return response_json


def fdp_request(params, multithreading = False):
    load_dotenv()
    fdp_url = os.getenv("FDP_URL")
    if not fdp_url or fdp_url == "":
        return {"status":"ko", "info":"fdp url not found"}

    service = params.get("service")
    if service == "history":
        exchange = params.get("exchange", "")
        symbol = params.get("symbol", "")
        start = params.get("start", "")
        interval = params.get("interval", "")
        end = params.get("end", "")
        url = "history?exchange=" + exchange + "&start=" + start
        if interval != "":
            url = url + "&interval=" + interval
        if end != "":
            url = url + "&end=" + end
        url = url + "&symbol=" #  + symbol
    else:
        return {"status":"ko", "info":"unknown service"}

    if not multithreading:
        final_result = _atomic_fdp_request(fdp_url+url+symbol)
    else:
        final_result = {"status":"ok", "result":{}}
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(_atomic_fdp_request, fdp_url+url+current_symbol): current_symbol for current_symbol in symbol.split(',')}
            for future in concurrent.futures.as_completed(futures):
                current_symbol = futures[future]
                res = future.result()
                final_result["result"][current_symbol] = res["result"][current_symbol]
        

    return final_result

def fdp_request_post(url, params):
    load_dotenv()
    fdp_url = os.getenv("FDP_URL")

    request = urllib.request.Request(fdp_url+'/'+url, urllib.parse.urlencode(params).encode())
    response = urllib.request.urlopen(request).read().decode()
    return json.loads(response)
