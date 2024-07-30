import subprocess
import os, sys, platform
from src import crag_helper, broker_bitget_api, logger, utils
from src.toolbox import graph_helper
from rich import print
from datetime import datetime, timedelta
import time
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

def get_fig_orders(my_broker):
    fig = plt.figure(figsize=(10, 15))

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

    return fig


if __name__ == '__main__':
    print("Platform :", g_os_platform)
    g_python_executable = sys.executable
    print("Python executable :", g_python_executable)

    console = logger.LoggerConsole()

    botId = "cedfactory1"
    botTelegram = logger._initialize_crag_telegram_bot("cedfactory1")
    if botTelegram == None:
        console.log("💥 Bot {} failed".format(botId))
        exit(1)
    console.log("Bot {} initialized".format(botId))

    accountId = "subfortest2"
    params = {"exchange": "bitget", "account": accountId,
              "reset_account": False, "reset_account_orders": False, "zero_print": False}
    my_broker = broker_bitget_api.BrokerBitGetApi(params)
    if my_broker == None:
        console.log("💥 Broker {} failed".format(accountId))
        exit(1)
    console.log("Account {} initialized".format(botId))

    datafile = "pulsar_data.csv"
    df = pd.DataFrame(columns=["account", "timestamp", "usdt_equity"])
    if os.path.exists(datafile):
        df = pd.read_csv(datafile)
        #df['timestamp'] = df['timestamp'].astype(float)
        #df['usdt_equity'] = df['usdt_equity'].astype(float)
        #df["usdt_equity"] = pd.to_numeric(df["usdt_equity"], errors='coerce')

    message_id = 42
    while True:
        usdt_equity = my_broker.get_usdt_equity()
        if usdt_equity is None:
            continue
        else:
            usdt_equity = str(utils.KeepNDecimals(usdt_equity, 2))
        extra = {}
        if message_id:
            extra["message_id"] = message_id
        now = get_now()
        current_time = get_time_from_datetime(now)
        message = current_time + "\n" + "<b>" + my_broker.account["id"] + "</b>" + " : $ " + usdt_equity

        current_timestamp = get_timestamp_from_datetime(now)
        df.loc[len(df)] = [accountId, float(current_timestamp), float(usdt_equity)]
        df.to_csv(datafile, index=False)

        timestamp_begin = (now - timedelta(hours=72)).timestamp()
        df_filtered = df.loc[df["timestamp"] > timestamp_begin]

        fig = graph_helper.export_graph("pulsar_history.png", accountId,
                                  [{"dataframe": df_filtered, "plots": [{"column": "usdt_equity", "label": "USDT equity"}]}])

        response = botTelegram.log(message, attachments=["pulsar_history.png"], extra=extra)
        if response and "ok" in response and response["ok"] and "result" in response:
            if "message_id" in response["result"]:
                message_id = response["result"]["message_id"]
            elif isinstance(response["result"], list):
                message_id = [res["message_id"] for res in response["result"]]

        get_fig_orders(my_broker)
        extra["message_id"] = 57
        message = current_time + "\n" + "<b>" + my_broker.account["id"] + "</b>" + " : current state"
        response = botTelegram.log(message, attachments=["pulsar_current_state.png"], extra=extra)

        time.sleep(60*5)  # 5min
