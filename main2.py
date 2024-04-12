from pympler import asizeof,classtracker
import time
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from src import broker_bitget_api


if __name__ == '__main__':
    params = {"exchange": "bitget", "account": "subfortest2", "reset_account": False, "zero_print": False}
    my_broker = broker_bitget_api.BrokerBitGetApi(params)
    TEST_01 = True
    TEST_02 = True
    TEST_03 = True
    while True:
        my_broker.enable_cache()
        if TEST_01:
            start = time.time()
            df_positions = my_broker.get_open_position()
            end = time.time()
            print("get_open_position {}".format(end - start))

            start = time.time()
            df_open_orders = my_broker.get_open_orders(["XRP"])
            end = time.time()
            print("get_open_orders {}".format(end - start))

            start = time.time()
            values = my_broker.get_values(["BTC"])
            end = time.time()
            print("get_values {}".format(end - start))

            start = time.time()
            df_account = my_broker._get_df_account()
            end = time.time()
            print("_get_df_account {}".format(end - start))

        if TEST_02:
            # trade_symbol = my_broker._get_symbol("XRP") # 'XRPUSDT_UMCBL'
            start = time.time()

            clientOid = my_broker.clientOIdprovider.get_name('XRPUSDT_UMCBL', 'OPEN_LONG_ORDER')
            transaction = my_broker._open_long_order('XRPUSDT_UMCBL', 11, clientOid, 0.5)

            if "msg" in transaction and transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                end = time.time()
                print("open_long_order {}".format(end - start))

                start = time.time()
                orderId = transaction["data"]["orderId"]
                result = my_broker.orderApi.cancel_orders('XRPUSDT_UMCBL', "USDT", orderId)

                end = time.time()
                print("cancel_open_ong_orders {}".format(end - start))
            else:
                try:
                    print("===== ", transaction["msg"])
                except:
                    print("FAILED")

        if TEST_03:
            # trade_symbol = my_broker._get_symbol("XRP") # 'XRPUSDT_UMCBL'
            start = time.time()

            clientOid = my_broker.clientOIdprovider.get_name('XRPUSDT_UMCBL', 'CLOSE_LONG_ORDER')
            transaction = my_broker._close_long_order('XRPUSDT_UMCBL', 11, clientOid, 0.67)

            if "msg" in transaction and transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                end = time.time()
                print("close_long_order {}".format(end - start))

                start = time.time()
                orderId = transaction["data"]["orderId"]
                result = my_broker.orderApi.cancel_orders('XRPUSDT_UMCBL', "USDT", orderId)

                end = time.time()
                print("cancel_close_long_orders {}".format(end - start))
            else:
                try:
                    print("===== ", transaction["msg"])
                except:
                    print("FAILED")

        my_broker.disable_cache()
