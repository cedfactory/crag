import os, sys, platform
from src import broker_bitget_api, logger, utils
from src.toolbox import graph_helper
from rich import print
from datetime import datetime, timedelta
import time
import json
import csv
import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

g_os_platform = platform.system()
g_python_executable = ""
delta_window = 72

def get_now():
    return datetime.now()

def get_timestamp_from_datetime(dt):
    current_timestamp = datetime.timestamp(dt)
    return current_timestamp

def get_time_from_datetime(dt):
    current_time = dt.strftime("%Y/%m/%d %H:%M:%S.%f")
    return current_time.split('.')[0]

def encode_limit_orders(df_limit_orders):
    df = df_limit_orders[["symbol", "price", "side"]]
    df = df.astype('str')
    json_data = df[["symbol", "price", "side"]].to_json()
    str = json.dumps(json_data)
    #json_data = json.loads(str)
    #df = pd.read_json(json_data)
    #print(df)
    return str

def encode_triggers(df_triggers):
    #df = df_triggers[["symbol", "price", "side"]]
    df = df_triggers.astype('str')
    json_data = df.to_json()
    str = json.dumps(json_data)
    #json_data = json.loads(str)
    #df = pd.read_json(json_data)
    #print(df)
    return str

def generate_figure_limit_orders(df_account):
    fig = plt.figure(figsize=(10, 15))

    now = get_now()
    timestamp_begin = (now - timedelta(hours=delta_window)).timestamp()
    df_filtered = df_account.loc[df_account["timestamp"] > timestamp_begin]
    df_filtered = df_filtered[df_filtered['limit_orders'].ne(df_filtered['limit_orders'].shift())].reset_index(drop=True)

    ax = plt.gca()
    xfmt = mdates.DateFormatter("%d-%m-%Y %H:%M:%S.%f")
    ax.xaxis.set_major_formatter(xfmt)

    # limit orders
    xSell = []
    ySell = []
    xBuy = []
    yBuy = []
    for index, row in df_filtered.iterrows():
        timestamp = datetime.fromtimestamp(float(row["timestamp"]))

        str_limit_orders = row["limit_orders"]
        if str_limit_orders != "":
            json_data = json.loads(str_limit_orders[1:-1])
            df_limit_orders = pd.read_json(json_data, precise_float=True)
            df_limit_orders["price"] = df_limit_orders["price"].astype("Float64")
            if isinstance(df_limit_orders, pd.DataFrame):
                for index2, row2 in df_limit_orders.iterrows():
                    if row2["side"] == "open_long":
                        xSell.append(timestamp)
                        ySell.append(float(row2["price"]))
                    elif row2["side"] == "open_short":
                        xBuy.append(timestamp)
                        yBuy.append(float(row2["price"]))
    plt.scatter(xSell, ySell, s=400, marker="_", color="red", label="Open long ")
    plt.scatter(xBuy, yBuy, s=400, marker="_", color="blue", label="Open short")

    plt.legend()

    plt.savefig("pulsar_limit_orders.png")
    plt.close(fig)

def generate_figure_triggers(df_account):
    fig = plt.figure(figsize=(10, 15))

    now = get_now()
    timestamp_begin = (now - timedelta(hours=delta_window)).timestamp()
    df_filtered = df_account.loc[df_account["timestamp"] > timestamp_begin]
    # Filter out consecutive duplicate rows based on the 'triggers' column
    # Step 1: Compare 'triggers' column with its shifted version to identify changes
    # Step 2: Keep rows where the current 'triggers' value is not equal to the previous value
    # Step 3: Reset the index to remove gaps from dropped rows
    df_filtered = df_filtered[df_filtered['triggers'].ne(df_filtered['triggers'].shift())].reset_index(drop=True)

    ax = plt.gca()
    xfmt = mdates.DateFormatter("%d-%m-%Y %H:%M:%S.%f")
    ax.xaxis.set_major_formatter(xfmt)

    # limit orders
    xSell = []
    ySell = []
    xBuy = []
    yBuy = []
    for index, row in df_filtered.iterrows():
        timestamp = datetime.fromtimestamp(float(row["timestamp"]))

        str_triggers = row["triggers"]
        if str_triggers != "":
            json_data = json.loads(str_triggers[1:-1])
            df_triggers = pd.read_json(json_data, precise_float=True)
            df_triggers["triggerPrice"] = df_triggers["triggerPrice"].astype("Float64")
            if isinstance(df_triggers, pd.DataFrame):
                for index2, row2 in df_triggers.iterrows():
                    if row2["side"] == "sell":
                        xSell.append(timestamp)
                        ySell.append(float(row2["triggerPrice"]))
                    elif row2["side"] == "buy":
                        xBuy.append(timestamp)
                        yBuy.append(float(row2["triggerPrice"]))
    plt.scatter(xSell, ySell, s=400, marker="_", color="red", label="sell")
    plt.scatter(xBuy, yBuy, s=400, marker="_", color="blue", label="buy")

    plt.legend()

    plt.savefig("pulsar_triggers.png")
    plt.close(fig)

class Agent:
    def __init__(self):
        self.bot_id = None
        self.account_id = None
        self.message1_id = "-1"
        self.message2_id = "-1"
        self.message3_id = "-1"
        self.message4_id = "-1"

def _usage():
    usage = "pulsar <pulsar.csv>"
    print(usage)

if __name__ == '__main__':
    console = logger.LoggerConsole()

    agents = []
    active_symbols = []

    file_path = ""
    if len(sys.argv) == 2:
        file_path = sys.argv[1]
        if os.path.exists(file_path):
            with open(file_path, "r") as file:
                csvreader = csv.reader(file)
                next(csvreader)
                for row in csvreader:
                    agent = Agent()
                    agent.bot_id = row[0].strip()
                    agent.account_id = row[1].strip()
                    symbols_field = row[2].strip()
                    agent.message1_id = row[3].strip()
                    agent.message2_id = row[4].strip()
                    agent.message3_id = row[5].strip()
                    agent.message4_id = row[6].strip()

                    # Parse the symbols_field to extract symbols with True status
                    active_symbols = []
                    symbols = symbols_field.split('/')
                    for symbol_status in symbols:
                        if ':' in symbol_status:
                            symbol, status = symbol_status.split(':', 1)
                            if status.strip().lower() == 'true':
                                active_symbols.append(symbol.strip())
                    agent.symbols = active_symbols
                    agents.append(agent)
        else:
            _usage()
            exit(0)
    else:
        _usage()
        exit(0)

    console.log("Platform : " + g_os_platform)
    g_python_executable = sys.executable
    console.log("Python executable : " + g_python_executable)

    console.log(str(len(agents)) + " agents read")
    if len(agents) == 0:
        sys.exit(0)

    # initialize the agents
    for agent in agents:
        bot_id = agent.bot_id
        agent.bot = logger._initialize_crag_telegram_bot(bot_id)
        if agent.bot == None:
            console.log("💥 Bot {} failed".format(bot_id))
            exit(1)
        console.log("Bot {} initialized".format(bot_id))

        account_id = agent.account_id
        params = {"exchange": "bitget", "account": account_id,
                  "reset_account": False, "reset_account_orders": False, "zero_print": False}
        agent.broker = broker_bitget_api.BrokerBitGetApi(params)
        if agent.broker == None:
            console.log("💥 Broker {} failed".format(account_id))
            exit(1)
        console.log("Account {} initialized".format(account_id))

        agent.datafile = "pulsar_" + account_id + "_data.csv"
        agent.df_account = pd.DataFrame(columns=["account", "timestamp", "usdt_equity", "limit_orders", "triggers"])
        if os.path.exists(agent.datafile):
            agent.df_account = pd.read_csv(agent.datafile, sep=",")
            agent.df_account['timestamp'] = agent.df_account['timestamp'].astype(float)
            agent.df_account['usdt_equity'] = agent.df_account['usdt_equity'].astype(float)
            #agent.df_account["usdt_equity"] = agent.df_account.to_numeric(agent.df_account["usdt_equity"], errors='coerce')
            agent.df_account["limit_orders"] = agent.df_account["limit_orders"].fillna("")
            agent.df_account["triggers"] = agent.df_account["triggers"].fillna("")

        update_csv = False
        if agent.message1_id == "-1":
            response = agent.bot.log(account_id)
            try:
                agent.message1_id = str(response["result"]["message_id"])
                update_csv = True
            except Exception as e:
                console.log("Problem while sending message with " + agent.bot_id)

        if agent.message2_id == "-1":
            response = agent.bot.log("usdt equity", attachments=["pulsar.png"])
            try:
                agent.message2_id = str(response["result"]["message_id"])
                update_csv = True
            except Exception as e:
                console.log("Problem while sending message with " + agent.bot_id)

        if agent.message3_id == "-1":
            response = agent.bot.log("grid", attachments=["pulsar.png"])
            try:
                agent.message3_id = str(response["result"]["message_id"])
                update_csv = True
            except Exception as e:
                console.log("Problem while sending message with " + agent.bot_id)

        if agent.message4_id == "-1":
            response = agent.bot.log("grid", attachments=["pulsar.png"])
            try:
                agent.message4_id = str(response["result"]["message_id"])
                update_csv = True
            except Exception as e:
                console.log("Problem while sending message with " + agent.bot_id)

    if update_csv:
        with open(file_path, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["bot_id", "account_id", "message1_id", "message2_id", "message3_id", "message4_id"])
            for agent in agents:
                writer.writerow([agent.bot_id, agent.account_id, agent.message1_id, agent.message2_id, agent.message3_id, agent.message4_id])

    while True:
        for agent in agents:
            now = get_now()
            current_time = get_time_from_datetime(now)
            current_timestamp = get_timestamp_from_datetime(now)

            # get new data
            usdt_equity = agent.broker.get_usdt_equity()
            if usdt_equity is None:
                continue
            else:
                usdt_equity = str(utils.KeepNDecimals(usdt_equity, 2))

            current_state = agent.broker.get_current_state(agent.symbols)

            # save new data
            str_limit_orders = ""
            if not current_state["open_orders"].empty:
                str_limit_orders = "\""+encode_limit_orders(current_state["open_orders"])+"\""

            str_triggers = ""
            if not current_state["triggers"].empty:
                str_triggers = "\""+encode_triggers(current_state["triggers"])+"\""

            agent.df_account.loc[len(agent.df_account)] = [
                agent.account_id,
                float(current_timestamp),
                float(usdt_equity),
                str_limit_orders,
                str_triggers]
            agent.df_account.to_csv(agent.datafile, index=False)

            extra = {}
            if agent.message1_id != "-1":
                extra["message_id"] = agent.message1_id
            message = current_time + "\n" + "<b>" + agent.broker.account["id"] + "</b>" + " : $ " + usdt_equity

            response = agent.bot.log(message, extra=extra)

            # message 4
            extra["message_id"] = agent.message4_id
            timestamp_begin = (now - timedelta(hours=delta_window)).timestamp()
            df_filtered = agent.df_account.loc[agent.df_account["timestamp"] > timestamp_begin]

            fig = graph_helper.export_graph("pulsar_history.png", agent.account_id,
                                      [{"dataframe": df_filtered, "plots": [{"column": "usdt_equity", "label": "USDT equity"}]}])

            for symbol in agent.symbols:
                now = get_now()
                start_time = (now - timedelta(hours=delta_window)).timestamp() * 1000
                candles = agent.broker.fetch_historical_data_multithreaded(symbol, "5m", start_time, now.timestamp() * 1000, 5)

            plt.close(fig)

            response = agent.bot.log(message, attachments=["pulsar_history.png"], extra=extra)
            if response and "ok" in response and response["ok"] and "result" in response:
                if "message_id" in response["result"]:
                    message_id = response["result"]["message_id"]
                elif isinstance(response["result"], list):
                    message_id = [res["message_id"] for res in response["result"]]

            # message 2
            generate_figure_limit_orders(agent.df_account)
            extra["message_id"] = agent.message2_id
            message = current_time + "\n" + "<b>" + agent.broker.account["id"] + "</b>" + " : limit orders"
            response = agent.bot.log(message, attachments=["pulsar_limit_orders.png"], extra=extra)

            # message 3
            generate_figure_triggers(agent.df_account)
            extra["message_id"] = agent.message3_id
            message = current_time + "\n" + "<b>" + agent.broker.account["id"] + "</b>" + " : triggers"
            response = agent.bot.log(message, attachments=["pulsar_triggers.png"], extra=extra)

        time.sleep(1)  # 5min
