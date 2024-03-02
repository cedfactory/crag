import urllib
import urllib.parse
import urllib.request
import json
import secrets
import requests
import os
from datetime import datetime, timedelta
from src.toolbox import settings_helper
import psutil

def get_memory_usage():
    process = psutil.Process()
    mem_info = process.memory_info()
    return mem_info.rss  # Return the Resident Set Size (RSS) in bytes

def format_duration(timestamp):
    duration = timedelta(seconds=timestamp)
    weeks = duration.days // 7
    days = duration.days % 7
    hours, remainder = divmod(duration.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if duration < timedelta(minutes=1):
        return f"{seconds}s"
    elif duration < timedelta(hours=1):
        return f"{minutes}m {seconds}s"
    elif duration < timedelta(days=1):
        return f"{hours}h {minutes}m"
    elif duration < timedelta(weeks=1):
        return f"{days}d {hours}h {minutes}m"
    else:
        return f"{weeks}w {days}d {hours}h {minutes}m"

def calculate_decimal_places(value):
    if value >= 0.1:
        return 1
    elif value >= 0.01:
        return 2
    else:
        return 4

def convert_ms_to_datetime(ms):
    if isinstance(ms, str):
        ms = int(ms)
    return datetime.fromtimestamp(ms/1000.0)

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

def fdp_request_post(url, params, fdp_id):
    final_result = {}

    fdp_url = settings_helper.get_fdp_url_info(fdp_id).get("url", None)
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

def count_symbols_with_position_type(current_trades, symbol, position_type):
    count = 0
    for current_trade in current_trades:
        if current_trade.symbol == symbol and current_trade.type == position_type:
            count = count + 1
    return count

def get_lst_directories(path):
    lst_dir = []
    all_files = os.listdir(path)
    for file in all_files:
        file_path = os.path.join(path, file)
        if os.path.isdir(file_path):
            lst_dir.append(file)
    return lst_dir

def get_lst_files(path):
    lst_file = []
    all_files = os.listdir(path)
    for file in all_files:
        file_path = os.path.join(path, file)
        if not os.path.isdir(file_path):
            lst_file.append(file)
    return lst_file

def get_lst_files_start_with(path, str):
    lst_file = []
    all_files = os.listdir(path)
    for file in all_files:
        file_path = os.path.join(path, file)
        if not os.path.isdir(file_path) \
                and file.startswith(str):
            lst_file.append(file)
    return lst_file

def get_lst_files_end_with(path, str):
    lst_file = []
    all_files = os.listdir(path)
    for file in all_files:
        file_path = os.path.join(path, file)
        if not os.path.isdir(file_path) \
                and file.endswith(str):
            lst_file.append(file)
    return lst_file

def get_lst_files_start_n_end_with(path, str1, str2):
    lst_file = []
    all_files = os.listdir(path)
    for file in all_files:
        file_path = os.path.join(path, file)
        if not os.path.isdir(file_path) \
                and file.startswith(str1)\
                and file.endswith(str2):
            lst_file.append(file)
    return lst_file

class ClientOIdProvider():
    def __init__(self):
        self.iter = 0

    def get_name(self, symbol, side):
        underscore = "_"
        key = "#"
        ct = datetime.now()
        ts = ct.timestamp()
        id = "".join([symbol, underscore, side, underscore, str(self.iter), key, str(ts)])
        self.iter += 1
        return id
