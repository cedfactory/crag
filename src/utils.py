import os

import pandas as pd
from dotenv import load_dotenv
import urllib
import urllib.parse
import urllib.request
import json
import concurrent.futures
import secrets
import requests

import yfinance as yf

from datetime import datetime

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
            reason = "exception when requesting GET {}".format(url)
            response_json = {"status":"ko", "info":reason}
            n_attempts = n_attempts - 1
            print('FDP ERROR : ', reason)
    return response_json


def fdp_request(params, multithreading = True):
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
                if res["status"] == "ko":
                    final_result["result"][current_symbol] = res
                else:
                    final_result["result"][current_symbol] = res["result"][current_symbol]

    return final_result

def fdp_request_post(url, params):
    load_dotenv()
    fdp_url = os.getenv("FDP_URL")
    if not fdp_url or fdp_url == "":
        return {"status":"ko", "info":"fdp url not found"}

    final_result = {}

    n_attempts = 3
    while n_attempts > 0:
        try:
            response = requests.post(fdp_url+'/'+url, json=params)
            if response.status_code == 200:
                response_json = json.loads(response.text)
                final_result["status"] = "ok"
                final_result["elapsed_time"] = response_json["elapsed_time"]
                final_result["result"] = response_json["result"]
                break
        except:
            reason = "exception when requesting POST {}".format(url)
            final_result = {"status":"ko", "info":reason}
            n_attempts = n_attempts - 1
            print('FDP ERROR : ', reason)

    return final_result

def normalize(df):
    result = df.copy()
    for feature_name in df.columns:
        max_value = df[feature_name].max()
        min_value = df[feature_name].min()
        result[feature_name] = (df[feature_name] - min_value) / (max_value - min_value)
        result[feature_name] = result[feature_name] - result[feature_name][0]
    return result

def KeepNDecimals(value, n=3):
    return "{:.{}f}".format(value, n)

def get_variation(src, dst):
    if src == 0:
        return 0
    return 100 * (dst - src) / src

def get_random_id():
    return 10000000 + secrets.randbelow(90000000)
