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
import random
import string
import math
import hashlib

def reduce_data_description(lst_ds):
    lst_reduced = []
    for ds in lst_ds:
        reduced_ds = {
            "symbol": ds.symbols[0],
            "timeframe": ds.str_interval
        }
        lst_reduced.append(reduced_ds)

    unique_lst = list({(d["symbol"], d["timeframe"]): d for d in lst_reduced}.values())

    return unique_lst


def flatten_list(nested_list):
    flat = []
    for element in nested_list:
        if isinstance(element, list):
            # Recursively extend the flat list with the flattened version of element
            flat.extend(flatten_list(element))
        else:
            flat.append(element)
    return flat

def calculate_adjusted_price(percent_str, base_value, trade_type, is_profit):
    """
    Returns an adjusted price as a float (or an empty string) based on a percentage value.
    For take profit (is_profit=True):
      - open_short: base_value * (1 - pct/100)
      - open_long:  base_value * (1 + pct/100)
    For stop loss (is_profit=False):
      - open_long:  base_value * (1 - pct/100)
      - open_short: base_value * (1 + pct/100)
    """
    if not percent_str:
        return ""
    try:
        pct = float(percent_str)
    except ValueError:
        return ""
    if trade_type == "open_short":
        factor = (1 - pct / 100) if is_profit else (1 + pct / 100)
    elif trade_type == "open_long":
        factor = (1 + pct / 100) if is_profit else (1 - pct / 100)
    else:
        return ""
    return base_value * factor

def chunk_list(lst, chunk_size):
    return [lst[i: i + chunk_size] for i in range(0, len(lst), chunk_size)]

def filtered_grouped_orders(order_list):
    # Filter items where "grouped_id" is None
    result = [order for order in order_list if order.get("grouped_id") is None]

    # Group orders with a "grouped_id" not None
    grouped_items = {}
    for order in order_list:
        grouped_id = order.get("grouped_id")
        if grouped_id is not None:
            # Keep only the first occurrence of each "grouped_id"
            if grouped_id not in grouped_items:
                grouped_items[grouped_id] = order

    # Add the first occurrences of each "grouped_id" to the result
    result.extend(grouped_items.values())

    return result

def assign_grouped_id(group):
    if group['grouped'].any():  # Check if any row in the group has grouped=True
        grouped_id = generate_random_id(4)
        group['grouped_id'] = group['grouped'].apply(lambda x: grouped_id if x else None)
    else:
        group['grouped_id'] = None
    return group

def to_unix_millis(dt):
    return int(dt.timestamp() * 1000)

def dict_to_string(d):
    return '\n'.join(f"{key}: {value}" for key, value in d.items())

def dicts_are_equal(dict1, dict2):
    # First, check if both dictionaries have the same keys
    if dict1.keys() != dict2.keys():
        return False

    # Then, check if the values corresponding to each key are the same
    for key in dict1:
        if dict1[key] != dict2[key]:
            return False

    return True

def create_directory(self, directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

def normalize_price(amount, pricePlace, priceEndStep):
    pricePlace = float(pricePlace)
    priceEndStep = float(priceEndStep)
    amount = amount * pow(10, pricePlace)
    amount = math.floor(amount)
    amount = amount * pow(10, -pricePlace)
    amount = round(amount, int(pricePlace))
    # Calculate the decimal without using %
    decimal_multiplier = priceEndStep * pow(10, -pricePlace)
    decimal = amount - math.floor(round(amount / decimal_multiplier)) * decimal_multiplier
    amount = amount - decimal
    amount = round(amount, int(pricePlace))

    del pricePlace
    del decimal_multiplier
    del decimal
    return amount

def normalize_size(size, sizeMultiplier):
    size = (size // sizeMultiplier) * sizeMultiplier

    return size

def create_equal_spacing_list(x, y, num_values=5):
    if num_values < 2:
        raise ValueError("num_values must be at least 2 to create a range.")

    step = (y - x) / (num_values - 1)
    return [x + i * step for i in range(num_values)]

def keep_n_smallest(df, column_name, n):
    return df.nsmallest(n, column_name)

def keep_n_highest(df, column_name, n):
    return df.nlargest(n, column_name)

def generate_random_id(length=8):
    # Define the characters to choose from: uppercase letters and digits
    characters = string.ascii_uppercase + string.digits
    # Generate a random sequence of the specified length
    random_id = ''.join(random.choice(characters) for _ in range(length))
    return random_id

def is_unique(value, lst):
    return lst.count(value) == 1

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

def concat_csv_files_with_df(directory, pattern='.csv', existing_df=None):
    csv_files = [file for file in os.listdir(directory) if file.endswith(pattern)]
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

def drop_largest_items(lst, x):
    sorted_lst = sorted(lst, reverse=True)  # Sort the list in reverse order
    return sorted_lst[x:]  # Slice the list to remove the largest x items

def filter_strings(strings, pattern):
    filtered_list = [s for s in strings if pattern not in s.lower()]
    return filtered_list

def modify_strategy_data_files(input_dir, str):
    # Get list of all files in the directory
    files = os.listdir(input_dir)
    # Filter files containing "_df_open_positions.csv"
    matching_files = [file for file in files if str in file]
    matching_files = filter_strings(matching_files, "baseline")
    matching_files = filter_strings(matching_files, "result")
    for filename in matching_files:
        df_tmp = pd.read_csv(os.path.join(input_dir, filename))
        if len(df_tmp) > 0:
            unnamed_columns = [col for col in df_tmp.columns if col.startswith('Unnamed')]
            df_tmp = df_tmp.drop(columns=unnamed_columns)
            df_tmp['orderId'] = ""
            df_tmp['gridId_str'] = pd.Series(df_tmp['gridId'], dtype="string")
            df_tmp['orderId'] = "ORDER_ID_" + df_tmp['gridId_str']
            df_tmp = df_tmp.drop(columns=['gridId_str'])
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

    if True:
        fdp_url = "http://192.168.1.205:5000/" # CEDE DEBUG
        # fdp_url_id = "http://192.168.1.205:5000"
        fdp_url = "https://fdp-1052915265688.europe-west9.run.app/"

    if not fdp_url or fdp_url == "":
        return {"status":"ko", "info":"fdp url not found"}

    final_result = {}

    n_attempts = 3
    while n_attempts > 0:
        try:
            # response = requests.post(fdp_url+'/'+url, json=params)
            with requests.post(fdp_url+'/'+url, json=params) as response:
                print("Sent URL:", response.request.url)                       # CEDE DEBUG
                print("Sent JSON params (body):", response.request.body)       # CEDE DEBUG
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
        ts = int(ct.timestamp())
        self.id = "".join([symbol, underscore, side, underscore, str(self.iter), key, str(ts)])
        self.iter += 1
        del underscore
        del key
        del ct
        del ts
        del symbol
        del side
        return self.id

def make_hashable(obj):
    """
    Recursively convert 'obj' into a hashable representation:
      - dict      -> tuple of (key, value) pairs (sorted by key)
      - list/tuple-> tuple of items
      - set       -> tuple of items (sorted)
      - DataFrame -> hashed string (via CSV, for example)
      - anything else -> returned as-is, assuming it's already hashable
    """
    if isinstance(obj, dict):
        # Convert dict to a tuple of (key, hashable(value)), sorted by key
        return tuple(sorted((k, make_hashable(v)) for k, v in obj.items()))

    elif isinstance(obj, (list, tuple)):
        # Convert list/tuple to a tuple of hashable items
        return tuple(make_hashable(x) for x in obj)

    elif isinstance(obj, set):
        # Convert set to a sorted tuple
        return tuple(sorted(make_hashable(x) for x in obj))

    elif isinstance(obj, pd.DataFrame):
        # Example approach: convert DataFrame to CSV string, then hash it
        # (If you need more sophisticated checks, adapt here.)
        csv_str = obj.to_csv(index=False)
        return hashlib.md5(csv_str.encode('utf-8')).hexdigest()

    else:
        # int, float, str, bool, None, or any other already-hashable object
        return obj

def make_key(data_desc_obj):
    """
    Produce a hashable "key" for each DataDescription object,
    ignoring 'strategy_id' and converting all fields into hashable forms.
    """
    # 1) Extract all fields from the object. Usually, vars(obj) or obj.__dict__.
    fields = vars(data_desc_obj).copy()  # shallow copy so we don't mutate

    # 2) Remove 'strategy_id' so it doesn't affect the duplicate check
    fields.pop('strategy_id', None)

    # 3) Recursively convert everything into hashable structures
    hashable_fields = {}
    for field_name, field_value in fields.items():
        hashable_fields[field_name] = make_hashable(field_value)

    # 4) Finally, produce a stable, sorted tuple of (field, value)
    return tuple(sorted(hashable_fields.items()))


def detailed_dataframes_equal(df1, df2):
    """
    Check if two pandas DataFrames are totally equal by attempting an assert.

    Returns:
        bool: True if DataFrames are equal, False otherwise.
    """
    try:
        pd.testing.assert_frame_equal(df1, df2, check_exact=True)
        return True
    except AssertionError as e:
        print("DataFrames are not equal:", e)
        return False

def are_dataframes_equal(df1, df2):
    """
    Check if two pandas DataFrames are totally equal.

    Two DataFrames are considered equal if they have the same data,
    the same column names, and the same index.

    Parameters:
        df1 (pd.DataFrame): The first DataFrame.
        df2 (pd.DataFrame): The second DataFrame.

    Returns:
        bool: True if the DataFrames are equal, False otherwise.
    """
    return df1.equals(df2)


def dict_lists_equal(lst1, lst2):
    normalized_lst1 = [normalize(d) for d in lst1]
    normalized_lst2 = [normalize(d) for d in lst2]
    return all(d in normalized_lst2 for d in normalized_lst1) and all(d in normalized_lst1 for d in normalized_lst2)

def normalize(d):
    # Create a copy so the original dict isn't modified.
    nd = d.copy()
    if nd.get("channel") == "account":
        # For account, map 'param' to 'coin' if present.
        if "param" in nd:
            nd["coin"] = nd.pop("param")
    else:
        # For non-account channels, map 'param' to 'inst_id' if present.
        if "param" in nd:
            nd["inst_id"] = nd.pop("param")
    return nd

def trim_symbol(symbol: str) -> str:
    # First check for the longer suffix to avoid partial matches.
    if symbol.endswith("USDT_UMCBL"):
        return symbol[:-len("USDT_UMCBL")]
    elif symbol.endswith("USDT"):
        return symbol[:-len("USDT")]
    return symbol


def transform_dict_to_dataframe(msg):
    """
    Transform a dictionary message back into a pandas DataFrame.

    Expects a message of the format:
    {
        'type': 'dict',
        'content': {
            'type': 'TRIGGERS',  # or another type identifier
            'data': {           # dictionary of columns, each containing a dict of index:value
                'planType': {0: 'normal_plan', 1: 'loss_plan'},
                'symbol': {0: 'XRPUSDT', 1: 'XRPUSDT'},
                ...
            }
        }
    }

    Returns:
        pd.DataFrame: DataFrame constructed from the nested 'data' dictionary.
    """
    # Verify the input is a dictionary.
    if not isinstance(msg, dict):
        raise ValueError("Input message must be a dictionary.")

    # Extract the 'content' section.
    content = msg.get("content")
    if content is None:
        raise ValueError("Message does not contain a 'content' key.")

    # Extract the 'data' part inside 'content'.
    data = content.get("data")
    if data is None:
        raise ValueError("Content does not contain a 'data' key.")

    # Convert the dictionary to a DataFrame.
    df = pd.DataFrame(data)
    return df
