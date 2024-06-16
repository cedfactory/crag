import subprocess
import os
import sys
from src import crag_helper, broker_bitget_api, logger, utils
from rich import print
import platform
from datetime import datetime
import time

g_os_platform = platform.system()
g_python_executable = ""

def get_current_time():
    now = datetime.now()
    current_time = now.strftime("%Y/%m/%d %H:%M:%S.%f")
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

    accountId = "subfortest1"
    params = {"exchange": "bitget", "account": accountId,
              "reset_account": False, "reset_account_orders": False, "zero_print": False}
    my_broker = broker_bitget_api.BrokerBitGetApi(params)
    if my_broker == None:
        console.log("💥 Broker {} failed".format(accountId))
        exit(1)
    console.log("Account {} initialized".format(botId))

    usdt_equity_0 = my_broker.get_usdt_equity()
    console.log("USDT equity at start : ${}".format(str(utils.KeepNDecimals(usdt_equity_0, 2))))

    current_time_0 = get_current_time()

    message_id = 41
    while True:
        usdt_equity = my_broker.get_usdt_equity()
        usdt_equity = str(utils.KeepNDecimals(usdt_equity, 2))
        extra = {}
        if message_id:
            extra["message_id"] = message_id
        current_time = get_current_time()
        message = current_time + "\n" + "<b>" + my_broker.account["id"] + "</b>" + " : $ " + usdt_equity
        response = botTelegram.log(message, extra=extra)
        if message_id == None and response["ok"] == "true" and "result" in response and "message_id" in response["result"]:
            message_id = response["result"]["message_id"]


        break
        # time.sleep(60*30)  # 30min
        time.sleep(10)  # 30min
