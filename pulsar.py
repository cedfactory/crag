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

def generate_figure_limit_orders(df_account, side="long"):
    fig = plt.figure(figsize=(10, 15))

    ax = plt.gca()
    xfmt = mdates.DateFormatter("%d-%m-%Y %H:%M:%S.%f")
    ax.xaxis.set_major_formatter(xfmt)

    # limit orders
    xSell = []
    ySell = []
    xBuy = []
    yBuy = []
    for index, row in df_account.iterrows():
        timestamp = datetime.fromtimestamp(float(row["timestamp"]))

        str_limit_orders = row["limit_orders"]
        if str_limit_orders != "":
            json_data = json.loads(str_limit_orders[1:-1])
            df_limit_orders = pd.read_json(json_data, precise_float=True)
            df_limit_orders["price"] = df_limit_orders["price"].astype("Float64")
            if isinstance(df_limit_orders, pd.DataFrame):
                for index2, row2 in df_limit_orders.iterrows():
                    if row2["side"] == "close_" + side:
                        xSell.append(timestamp)
                        ySell.append(float(row2["price"]))
                    elif row2["side"] == "open_" + side:
                        xBuy.append(timestamp)
                        yBuy.append(float(row2["price"]))
    plt.scatter(xSell, ySell, s=400, marker="_", color="red", label="Close " + side)
    plt.scatter(xBuy, yBuy, s=400, marker="_", color="blue", label="Open " + side)

    plt.legend()

    plt.savefig("pulsar_current_state.png")
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

    file_path = ""
    if len(sys.argv) == 2:
        file_path = sys.argv[1]
        if os.path.exists(file_path):
            with open(file_path, "r") as file:
                csvreader = csv.reader(file)
                next(csvreader)
                for row in csvreader:
                    agent = Agent()
                    agent.bot_id = row[0]
                    agent.account_id = row[1]
                    agent.message1_id = row[2]
                    agent.message2_id = row[3]
                    agent.message3_id = row[4]
                    agent.message4_id = row[5]
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
        agent.df_account = pd.DataFrame(columns=["account", "timestamp", "usdt_equity", "limit_orders"])
        if os.path.exists(agent.datafile):
            agent.df_account = pd.read_csv(agent.datafile, sep=",")
            agent.df_account['timestamp'] = agent.df_account['timestamp'].astype(float)
            agent.df_account['usdt_equity'] = agent.df_account['usdt_equity'].astype(float)
            #agent.df_account["usdt_equity"] = agent.df_account.to_numeric(agent.df_account["usdt_equity"], errors='coerce')
            agent.df_account["limit_orders"] = agent.df_account["limit_orders"].fillna("")

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

            current_state = agent.broker.get_current_state(["PEPE"])

            # save new data
            str_limit_orders = ""
            if not current_state["open_orders"].empty:
                str_limit_orders = "\""+encode_limit_orders(current_state["open_orders"])+"\""
            agent.df_account.loc[len(agent.df_account)] = [
                agent.account_id,
                float(current_timestamp),
                float(usdt_equity),
                str_limit_orders]
            agent.df_account.to_csv(agent.datafile, index=False)

            extra = {}
            if agent.message1_id != "-1":
                extra["message_id"] = agent.message1_id
            message = current_time + "\n" + "<b>" + agent.broker.account["id"] + "</b>" + " : $ " + usdt_equity

            response = agent.bot.log(message, extra=extra)

            # message 4
            extra["message_id"] = agent.message4_id
            timestamp_begin = (now - timedelta(hours=72)).timestamp()
            df_filtered = agent.df_account.loc[agent.df_account["timestamp"] > timestamp_begin]

            fig = graph_helper.export_graph("pulsar_history.png", agent.account_id,
                                      [{"dataframe": df_filtered, "plots": [{"column": "usdt_equity", "label": "USDT equity"}]}])
            plt.close(fig)

            response = agent.bot.log(message, attachments=["pulsar_history.png"], extra=extra)
            if response and "ok" in response and response["ok"] and "result" in response:
                if "message_id" in response["result"]:
                    message_id = response["result"]["message_id"]
                elif isinstance(response["result"], list):
                    message_id = [res["message_id"] for res in response["result"]]

            # message 2
            generate_figure_limit_orders(agent.df_account, "long")
            extra["message_id"] = agent.message2_id
            message = current_time + "\n" + "<b>" + agent.broker.account["id"] + "</b>" + " : limit orders long"
            response = agent.bot.log(message, attachments=["pulsar_current_state.png"], extra=extra)

            # message 3
            generate_figure_limit_orders(agent.df_account, "short")
            extra["message_id"] = agent.message3_id
            message = current_time + "\n" + "<b>" + agent.broker.account["id"] + "</b>" + " : limit orders short"
            response = agent.bot.log(message, attachments=["pulsar_current_state.png"], extra=extra)

        time.sleep(60*5)  # 5min
