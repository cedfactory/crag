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
import pandas as pd

def concat_csv_files(directory):
    csv_files = [file for file in os.listdir(directory) if file.endswith('.csv')]
    if len(csv_files) == 0:
        return None
    else:
        dfs = []
        # Iterate over CSV files in the directory
        for filename in csv_files:
            # Read the CSV file into a DataFrame
            df = pd.read_csv(os.path.join(directory, filename))
            if "position" in df.columns:
                df.set_index('position', inplace=True, drop=True)
            # Append the DataFrame to the list
            dfs.append(df)
        df_new = pd.concat(dfs, axis=1)

    df_new.columns = range(len(df_new.columns))
    return df_new

def concat_csv_files_with_df(directory, existing_df=None):
    csv_files = [file for file in os.listdir(directory) if file.endswith('.csv')]
    if len(csv_files) == 0:
        return existing_df
    elif len(csv_files) > 1:
        dfs = []
        # Iterate over CSV files in the directory
        for filename in csv_files:
            # Read the CSV file into a DataFrame
            df = pd.read_csv(os.path.join(directory, filename))
            if "position" in df.columns:
                df.set_index('position', inplace=True, drop=True)
            # Append the DataFrame to the list
            dfs.append(df)
        df_new = pd.concat(dfs, axis=1)
    elif len(csv_files) == 1:
        df_new = pd.read_csv(os.path.join(directory, csv_files[0]))
        if "position" in df_new.columns:
            df_new.set_index('position', inplace=True, drop=True)

    # If no existing DataFrame is provided, create a new one
    if existing_df is None:
        df_new.columns = range(len(df_new.columns))
        return df_new
    else:
        df_new.set_index(existing_df.index, inplace=True)
        result_df = pd.concat([df_new, existing_df], axis=1)
        result_df.columns = range(len(result_df.columns))
        return result_df

# Function to read CSV files from a directory and concatenate them
def drop_duplicate_grid_columns(df):
    if "position" in df.columns:
        df.set_index('position', inplace=True, drop=True)

    df.columns = range(len(df.columns))
    lst_columns_to_drop = []
    columns = df.columns

    # Iterate over pairs of adjacent columns
    for i in range(len(columns) - 1):
        col1 = df[columns[i]]
        col2 = df[columns[i + 1]]
        # Check if values in both columns are equal
        if col1.equals(col2):
            lst_columns_to_drop.append(columns[i])
    if len(lst_columns_to_drop) > 0:
        lst_columns_to_drop = list(set(lst_columns_to_drop))
        df.drop(lst_columns_to_drop, axis=1, inplace=True)

    return df

def format_integer(num):
    return f"{num:08}"

def empty_files(directory_path, pattern=".png"):
    if os.path.exists(directory_path) and os.path.isdir(directory_path):
        for file_name in os.listdir(directory_path):
            if pattern is None or file_name.endswith(pattern):
                file_path = os.path.join(directory_path, file_name)
                os.remove(file_path)
                print(f"Deleted: {file_path}")
    else:
        print(f"Directory does not exist: {directory_path}")

def create_dir(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

def split_list(input_list, sublist_size):
    return [input_list[i:i + sublist_size] for i in range(0, len(input_list), sublist_size)]

def drop_smallest_items(lst, x):
    sorted_lst = sorted(lst)  # Sort the list
    return sorted_lst[x:]  # Slice the list to remove the smallest x items

def modify_strategy_data_files(input_dir, str):
    # Get list of all files in the directory
    files = os.listdir(input_dir)
    # Filter files containing "_df_open_positions.csv"
    matching_files = [file for file in files if str in file]
    for filename in matching_files:
        df_tmp = pd.read_csv(os.path.join(input_dir, filename))

        unnamed_columns = [col for col in df_tmp.columns if col.startswith('Unnamed')]
        df_tmp = df_tmp.drop(columns=unnamed_columns)
        df_tmp.to_csv(os.path.join(input_dir, filename))

        # if len(df_tmp) > 0:
        #     df_tmp["total"] = df_tmp["total"] / 2
        #     df_tmp.to_csv(os.path.join(input_dir, filename))

def get_memory_usage():
    process = psutil.Process()
    mem_info = process.memory_info()
    del process
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
            # response = requests.post(fdp_url+'/'+url, json=params)
            with requests.post(fdp_url+'/'+url, json=params) as response:
                pass
            response.close()
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
            del reason
    del params
    del n_attempts
    del fdp_url
    del fdp_id
    del response
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
        self.id = []

    def get_name(self, symbol, side):
        underscore = "_"
        key = "#"
        ct = datetime.now()
        ts = ct.timestamp()
        self.id = "".join([symbol, underscore, side, underscore, str(self.iter), key, str(ts)])
        self.iter += 1
        del underscore
        del key
        del ct
        del ts
        del symbol
        del side
        return self.id
