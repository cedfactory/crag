import subprocess
import os, sys, platform
from src import crag_helper, broker_bitget_api, logger, utils
from src.toolbox import graph_helper
from rich import print
from datetime import datetime, timedelta
import time
import pandas as pd

g_os_platform = platform.system()
g_python_executable = ""

def get_now():
    return datetime.now()

def get_timestamp_from_datetime(dt):
    now = datetime.now()
    current_timestamp = datetime.timestamp(dt)
    return current_timestamp

def get_time_from_datetime(dt):
    current_time = dt.strftime("%Y/%m/%d %H:%M:%S.%f")
    return current_time.split('.')[0]

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

    usdt_equity_0 = my_broker.get_usdt_equity()
    console.log("USDT equity at start : ${}".format(str(utils.KeepNDecimals(usdt_equity_0, 2))))

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

        graph_helper.export_graph("pulsar_graph.png", accountId,
                                  [{"dataframe": df_filtered, "plots": [{"column": "usdt_equity", "label": "USDT equity"}]}])

        response = botTelegram.log(message, attachments=["pulsar_graph.png"], extra=extra)
        if message_id == None and response["ok"] == "true" and "result" in response and "message_id" in response["result"]:
            message_id = response["result"]["message_id"]

        time.sleep(60*5)  # 5min
