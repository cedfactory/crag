import os, sys, platform
from src import broker_bitget_api, logger, utils
from src.toolbox import graph_helper
from rich import print
from datetime import datetime, timedelta
import time
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

def generate_figure_orders(my_broker):
    plt.figure(figsize=(10, 15))

    current_state = my_broker.get_current_state(["XRP"])
    df_prices = current_state["prices"]

    current_price = df_prices.loc[df_prices["symbols"] == "XRP", "values"][0]
    current_timestamp = df_prices.loc[df_prices["symbols"] == "XRP", "timestamp"][0]
    current_timestamp = datetime.fromtimestamp(float(current_timestamp))

    ax = plt.gca()
    xfmt = mdates.DateFormatter("%d-%m-%Y %H:%M:%S.%f")
    ax.xaxis.set_major_formatter(xfmt)
    ax.set_xticks([current_timestamp])


    plt.scatter([current_timestamp], [current_price], marker="_", color="#000000", label="Current price")

    orders = current_state["open_orders"]
    if isinstance(orders, pd.DataFrame):
        xSell = []
        ySell = []
        xBuy = []
        yBuy = []
        for index, row in orders.iterrows():
            if row["side"] == "sell":
                xSell.append(1)
                ySell.append(float(row["price"]))
            elif row["side"] == "buy":
                xBuy.append(current_timestamp)
                yBuy.append(float(row["price"]))
        plt.scatter(xSell, ySell, marker="s", color="#ff0000", label="Order sell")
        plt.scatter(xBuy, yBuy, marker="s", color="#00ff00", label="Order buy ")

    triggers = current_state["triggers"]
    if isinstance(triggers, pd.DataFrame):
        xSell = []
        ySell = []
        xBuy = []
        yBuy = []
        # triggerType ?
        for index, row in triggers.iterrows():
            if row["side"] == "sell":
                xSell.append(current_timestamp)
                ySell.append(float(row["triggerPrice"]))
            elif row["side"] == "buy":
                xBuy.append(current_timestamp)
                yBuy.append(float(row["triggerPrice"]))
        plt.scatter(xSell, ySell, marker="o", color="#ff0000", label="Trigger sell")
        plt.scatter(xBuy, yBuy, marker="o", color="#00ff00", label="Trigger buy")

    plt.legend()

    plt.savefig("pulsar_current_state.png")
    plt.close()


class Agent:
    def __init__(self):
        self.bot_id = None
        self.account_id = None
        self.message1_id = "-1"
        self.message2_id = "-1"
        self.message3_id = "-1"

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
                    agents.append(agent)
        else:
            _usage()
            exit(0)
    else:
        _usage()
        exit(0)

    console.log("Platform :" + g_os_platform)
    g_python_executable = sys.executable
    console.log("Python executable :" + g_python_executable)

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
        agent.df = pd.DataFrame(columns=["account", "timestamp", "usdt_equity"])
        if os.path.exists(agent.datafile):
            agent.df = pd.read_csv(agent.datafile)
            agent.df['timestamp'] = agent.df['timestamp'].astype(float)
            agent.df['usdt_equity'] = agent.df['usdt_equity'].astype(float)
            #agent.df["usdt_equity"] = agent.df.to_numeric(agent.df["usdt_equity"], errors='coerce')

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

    if update_csv:
        with open(file_path, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["bot_id","account_id","message1_id","message2_id","message3_id"])
            for agent in agents:
                writer.writerow([agent.bot_id, agent.account_id, agent.message1_id, agent.message2_id, agent.message3_id])


    while True:
        for agent in agents:
            usdt_equity = agent.broker.get_usdt_equity()
            if usdt_equity is None:
                continue
            else:
                usdt_equity = str(utils.KeepNDecimals(usdt_equity, 2))
            extra = {}
            if agent.message1_id != "-1":
                extra["message_id"] = agent.message1_id
            now = get_now()
            current_time = get_time_from_datetime(now)
            message = current_time + "\n" + "<b>" + agent.broker.account["id"] + "</b>" + " : $ " + usdt_equity

            current_timestamp = get_timestamp_from_datetime(now)
            agent.df.loc[len(agent.df)] = [agent.account_id, float(current_timestamp), float(usdt_equity)]
            agent.df.to_csv(agent.datafile, index=False)

            response = agent.bot.log(message, extra=extra)

            # message 2
            extra["message_id"] = agent.message2_id
            timestamp_begin = (now - timedelta(hours=72)).timestamp()
            df_filtered = agent.df.loc[agent.df["timestamp"] > timestamp_begin]

            graph_helper.export_graph("pulsar_history.png", agent.account_id,
                                      [{"dataframe": df_filtered, "plots": [{"column": "usdt_equity", "label": "USDT equity"}]}])

            response = agent.bot.log(message, attachments=["pulsar_history.png"], extra=extra)
            if response and "ok" in response and response["ok"] and "result" in response:
                if "message_id" in response["result"]:
                    message_id = response["result"]["message_id"]
                elif isinstance(response["result"], list):
                    message_id = [res["message_id"] for res in response["result"]]

            generate_figure_orders(agent.broker)
            extra["message_id"] = agent.message3_id
            message = current_time + "\n" + "<b>" + agent.broker.account["id"] + "</b>" + " : current state"
            response = agent.bot.log(message, attachments=["pulsar_current_state.png"], extra=extra)

        time.sleep(60*5)  # 5min
