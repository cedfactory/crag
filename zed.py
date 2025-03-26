import os, sys
sys.path.append(os.path.abspath("src"))

import pandas as pd
from src.bitget_ws.zed_server import ZMQServer
from src.bitget_ws.zed_client import ZMQClient
from src import broker_bitget_api
import xml.etree.ElementTree as ET
import argparse
import time
import threading
import concurrent.futures
import pandas as pd

def read_settings_conf(conf_path, account_id):
    """
    Reads the settings configuration file and extracts the api_key, api_secret,
    and api_password for the given account id.

    Args:
        conf_path (str): The path to the settings configuration file.
        account_id (str): The id of the account element to search for.

    Returns:
        dict: A dictionary with keys 'api_key', 'api_secret', and 'api_password'.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
        ValueError: If no <accounts> element or the specified account id is found.
    """
    # Ensure the configuration file exists
    if not os.path.exists(conf_path):
        raise FileNotFoundError(f"The configuration file '{conf_path}' does not exist.")

    # Parse the XML file
    tree = ET.parse(conf_path)
    root = tree.getroot()

    # Find the <accounts> element
    accounts_elem = root.find('accounts')
    if accounts_elem is None:
        raise ValueError("No <accounts> element found in the configuration file.")

    # Look for the <account> element with the specified id
    account_elem = None
    for acc in accounts_elem.findall('account'):
        if acc.get('id') == account_id:
            account_elem = acc
            break

    if account_elem is None:
        raise ValueError(f"No account with id '{account_id}' found in the configuration file.")

    # Extract the required attributes
    api_key = account_elem.get('api_key')
    api_secret = account_elem.get('api_secret')
    api_password = account_elem.get('api_password')

    return {"api_key": api_key, "api_secret": api_secret, "api_password": api_password}

def read_conf_file(file_name):
    """
    Reads the configuration file and extracts the 'account' and 'symbols'
    from the <broker>'s <params> element.

    Args:
        conf_path (str): The path to the configuration file.

    Returns:
        dict: A dictionary containing the keys 'account' and 'symbols'.
    """
    base_symbols_path = "./symbols"
    base_conf_path = "./conf"
    base_settings_path = "./conf/settings.xml"
    # Check if the file exists before parsing
    conf_path = os.path.join(base_conf_path, file_name)
    if not os.path.exists(conf_path):
        raise FileNotFoundError(f"The configuration file '{conf_path}' does not exist.")

    # Parse the XML file
    tree = ET.parse(conf_path)
    root = tree.getroot()

    # Find the <broker> element (assuming there's only one)
    broker = root.find('broker')
    if broker is None:
        raise ValueError("No <broker> element found in the configuration file.")

    # Within <broker>, find the <params> element
    params = broker.find('params')
    if params is None:
        raise ValueError("No <params> element found in the <broker> section.")

    # Extract the required attributes
    account_id = params.get('account')
    symbols_file = params.get('symbols')

    symbol_path = os.path.join(base_symbols_path, symbols_file)
    if not os.path.exists(conf_path):
        raise FileNotFoundError(f"The configuration file '{symbol_path}' does not exist.")
    symbols = pd.read_csv(symbol_path)["symbol"].to_list()

    keys = read_settings_conf(base_settings_path, account_id)

    return {"account_id": account_id, "account_keys": keys, "symbols": symbols}

def run_server(conf_file):
    print("Server is running with configuration file:", conf_file)
    conf_data = read_conf_file(conf_file)
    server = ZMQServer(bind_address="tcp://*:5555",
                       conf_data=conf_data)  # listening on port 5555
    server.start()  # this will block and run indefinitely until interrupted or shutdown signal


def run_client():
    print("Client is running...")
    client = ZMQClient(server_address="tcp://localhost:5555", timeout=100)  # using 10ms timeout

    request_status = {
        "action": "INFO",
        "request": "GET_STATUS",
    }

    while True:
        reply = client.send_request(request_status)

        if "error" in reply:
            print("INFO STATUS", reply)
            continue

        if reply.get("type") == "dict":
            content = reply.get("content", {})
            if content.get("status") == "On":
                print("INFO STATUS", reply)
                time.sleep(1)
                break

    start = time.perf_counter()  # Start timing

    lst_request = [
        "TRIGGERS",
        "OPEN_POSITIONS",
        "OPEN_ORDERS",
        "ACCOUNT",
        "PRICES",
        "USDT_EQUITY_AVAILABLE"
    ]
    lst_df_to_be_transformed = ["TRIGGERS", "OPEN_POSITIONS", "PRICES", "OPEN_ORDERS"]
    print("############################################################")
    for request in lst_request:
        request_data = {"action": "GET", "request": request}
        reply = client.send_request(request_data)
        if "error" in reply:
            print("Reply for dict:", reply["error"])
        elif "type" in reply and "content" in reply and reply["type"] == 'dict':
            content = reply["content"]
            if content["type"] in lst_df_to_be_transformed:
                df = transform_dict_to_dataframe(reply)
                print("Reply for dict:", df.to_string())
            else:
                print("Reply for dict:", reply)
        else:
            print("Reply for dict:", reply)
        print("############################################################")

    lst_symbol = ['BTC', 'ETH', 'XRP', 'DOGE', 'PEPE']
    for symbol in lst_symbol:
        request_data = {"action": "GET", "request": "PRICE_VALUE", "symbol": symbol}
        reply = client.send_request(request_data)
        if "error" in reply:
            print("Reply for dict:", reply["error"])
        else:
            print("Reply for dict:", reply)
        print("############################################################")

    end = time.perf_counter()  # End timing

    print("Elapsed time:", end - start, "seconds")


    # Send a shutdown signal to stop the server (as implemented in our example)
    client.send_request({"action": "shutdown"})
    client.close()

def run_client_test_vs_broker(conf_file):
    lst_symbol = ['BTC', 'ETH', 'XRP', 'DOGE', 'PEPE']
    conf_data = read_conf_file(conf_file)
    account_id = conf_data.get("account_id", "")
    my_broker = broker_bitget_api.BrokerBitGetApi({"account": account_id, "reset_account_start": False})

    print("Client is running...")
    client = ZMQClient(server_address="tcp://localhost:5555", timeout=10)  # using 10ms timeout

    request_status = {
        "action": "INFO",
        "request": "GET_STATUS",
    }

    while True:
        reply = client.send_request(request_status)

        if "error" in reply:
            print("INFO STATUS", reply)
            continue

        if reply.get("type") == "dict":
            content = reply.get("content", {})
            if content.get("status") == "On":
                print("INFO STATUS", reply)
                time.sleep(1)
                break

    start = time.perf_counter()  # Start timing

    lst_request = [
        "TRIGGERS",
        "OPEN_POSITIONS",
        "OPEN_ORDERS",
        "ACCOUNT",
        "PRICES",
        "USDT_EQUITY_AVAILABLE"
    ]
    lst_df_to_be_transformed = ["TRIGGERS", "OPEN_POSITIONS", "PRICES", "OPEN_ORDERS"]
    print("############################################################")
    for request in lst_request:
        request_data = {"action": "GET", "request": request}
        reply = client.send_request(request_data)
        if "error" in reply:
            print("Reply for dict:", reply["error"])
        elif "type" in reply and "content" in reply and reply["type"] == 'dict':
            content = reply["content"]
            if content["type"] in lst_df_to_be_transformed:
                df = transform_dict_to_dataframe(reply)
                print("Reply for dict:", df.to_string())
            else:
                print("Reply for dict:", reply)
        else:
            print("Reply for dict:", reply)
        print("############################################################")

    for symbol in lst_symbol:
        request_data = {"action": "GET", "request": "PRICE_VALUE", "symbol": symbol}
        reply = client.send_request(request_data)
        if "error" in reply:
            print("Reply for dict:", reply["error"])
        else:
            print("Reply for dict:", reply)
        print("############################################################")

    end = time.perf_counter()  # End timing

    print("Elapsed time:", end - start, "seconds")
    print("############################################################")
    print("############################################################")
    print("############################################################")
    print("############################################################")
    print("############################################################")
    df_open_orders = my_broker.get_open_orders(lst_symbol, by_pass=True)
    print("broker df_open_orders: \n", df_open_orders.to_string())
    df_open_orders = my_broker.get_open_orders(lst_symbol, by_pass=False)
    print("ws df_open_orders: \n", df_open_orders)
    print("############################################################")
    df_prices = my_broker.get_values(lst_symbol, by_pass=True)
    print("broker df_prices: \n", df_prices.to_string())
    df_prices = my_broker.get_values(lst_symbol, by_pass=False)
    print("ws df_prices: \n", df_prices.to_string())
    print("############################################################")
    df_open_position_v2 = my_broker.get_open_position_v2(by_pass=True)
    print("broker df_open_position_v2: \n", df_open_position_v2.to_string())
    df_open_position_v2 = my_broker.get_open_position_v2(by_pass=False)
    print("ws df_open_position_v2: \n", df_open_position_v2.to_string())
    print("############################################################")
    df_open_position = my_broker.get_open_position(by_pass=True)
    print("broker df_open_position: \n", df_open_position.to_string())
    df_open_position = my_broker.get_open_position(by_pass=False)
    print("ws df_open_position: \n", df_open_position.to_string())
    print("############################################################")
    df_get_all_triggers = my_broker.get_all_triggers(by_pass=True)
    print("broker df_get_all_triggers: \n", df_get_all_triggers.to_string())
    try:
        df_get_all_triggers = my_broker.get_all_triggers(by_pass=False)
        print("ws df_get_all_triggers: \n", df_get_all_triggers.to_string())
        print("############################################################")
    except:
        print("toto")

    maxOpenPosAvailable = my_broker.get_account_maxOpenPosAvailable(by_pass=False)
    account_equity = my_broker.get_usdt_equity(by_pass=False)
    account_available = my_broker.get_account_available(by_pass=False)

    ws_maxOpenPosAvailable = my_broker.get_account_maxOpenPosAvailable(by_pass=True)
    ws_account_equity = my_broker.get_usdt_equity(by_pass=True)
    ws_account_available = my_broker.get_account_available(by_pass=True)

    print("maxOpenPosAvailable :", maxOpenPosAvailable, ws_maxOpenPosAvailable)
    print("############################################################")
    print("account_equity :", account_equity, ws_account_equity)
    print("############################################################")
    print("account_available :", account_available, ws_account_available)
    print("############################################################")
    for symbol in lst_symbol:
        price = my_broker.get_value(symbol, by_pass=False)
        ws_price = my_broker.get_value(symbol, by_pass=True)
        print("############################################################")
        print(symbol, "price :", price, ws_price)
        print("############################################################")


    # BENCHMARK
    print("############################################################")
    print("############################################################")
    print("############################################################")
    print("############################################################")
    print("############################################################")

    start = time.perf_counter()  # Start timing

    df_open_orders = my_broker.get_open_orders(lst_symbol, by_pass=True)
    df_prices = my_broker.get_values(lst_symbol, by_pass=True)
    df_open_position_v2 = my_broker.get_open_position_v2(by_pass=True)
    df_open_position = my_broker.get_open_position(by_pass=True)
    df_get_all_triggers = my_broker.get_all_triggers(by_pass=True)
    maxOpenPosAvailable = my_broker.get_account_maxOpenPosAvailable(by_pass=True)
    account_equity = my_broker.get_usdt_equity(by_pass=True)
    account_available = my_broker.get_account_available(by_pass=True)
    for symbol in lst_symbol:
        price = my_broker.get_value(symbol, by_pass=True)

    end = time.perf_counter()  # End timing
    print("Elapsed time API REST:", end - start, "seconds")

    start = time.perf_counter()  # Start timing

    df_open_orders = my_broker.get_open_orders(lst_symbol, by_pass=False)
    df_prices = my_broker.get_values(lst_symbol, by_pass=False)
    df_open_position_v2 = my_broker.get_open_position_v2(by_pass=False)
    df_open_position = my_broker.get_open_position(by_pass=False)
    df_get_all_triggers = my_broker.get_all_triggers(by_pass=False)
    maxOpenPosAvailable = my_broker.get_account_maxOpenPosAvailable(by_pass=False)
    account_equity = my_broker.get_usdt_equity(by_pass=False)
    account_available = my_broker.get_account_available(by_pass=False)
    for symbol in lst_symbol:
        price = my_broker.get_value(symbol, by_pass=False)

    end = time.perf_counter()  # End timing

    print("Elapsed time WS:", end - start, "seconds")


    # Send a shutdown signal to stop the server (as implemented in our example)
    client.send_request({"action": "shutdown"})
    client.close()

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

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="Run as server, client, or client_test")
    subparsers = parser.add_subparsers(dest="role", required=True,
                                       help="Choose to run as 'server', 'client', or 'client_test'")

    # Subparser for the server with a required conf_file argument
    server_parser = subparsers.add_parser("server", help="Run as server")
    server_parser.add_argument("conf_file", help="Configuration file for the server")

    # Subparser for the client without any additional parameters
    client_parser = subparsers.add_parser("client", help="Run as client")

    # Subparser for the client_test with a required conf_file argument
    client_test_parser = subparsers.add_parser("client_test", help="Run client test vs broker")
    client_test_parser.add_argument("conf_file", help="Configuration file for client test vs broker")

    args = parser.parse_args()

    if args.role == "server":
        run_server(args.conf_file)
    elif args.role == "client":
        run_client()
    elif args.role == "client_test":
        run_client_test_vs_broker(args.conf_file)


    print("Zed is Dead")