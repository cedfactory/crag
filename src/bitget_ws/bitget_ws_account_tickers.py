import bitget_ws_client
import json
import pandas as pd
from datetime import datetime
import utils

class WSAccountTickers:

    def __init__(self, params = None):
        self.client_account = None
        self.client_tickers = None
        self.id = None

        self.status = "Off"

        if not params:
            return

        self.id = params.get("id", self.id)
        api_key = params.get("api_key", None)
        api_secret = params.get("api_secret", None)
        api_passphrase = params.get("api_passphrase", None)
        if not api_key or not api_secret or not api_passphrase:
            return

        self.symbols = params.get('tickers')

        self.client_account = bitget_ws_client.BitgetWsClient(
            api_key = api_key,
            api_secret = api_secret,
            passphrase = api_passphrase,
            ws_url = bitget_ws_client.CONTRACT_WS_URL_PRIVATE,
            verbose=True) \
            .error_listener(bitget_ws_client.handel_error) \
            .build()

        self.client_tickers = bitget_ws_client.BitgetWsClient(
            ws_url=bitget_ws_client.CONTRACT_WS_URL_PUBLIC,
            verbose=True) \
            .error_listener(bitget_ws_client.handel_error) \
            .build()

        self.lst_tickers = params.get("tickers", [])
        self.verbose = False
        self.reset_state = {
            "ticker_prices": {},
            "positions": None,
            "orders": None,
            "orders-algo": None,
            "account": {
                "marginCoin": None,
                "available": None,
                "maxOpenPosAvailable": None,
                "usdtEquity": None
            },
        }
        self.state = self.reset_state

        self.lst_channels = [
            {"inst_type": "USDT-FUTURES", "channel": "account", "param": "default"},
            {"inst_type": "USDT-FUTURES", "channel": "positions", "param": "default"},
            {"inst_type": "USDT-FUTURES", "channel": "orders", "param": "default"},
            {"inst_type": "USDT-FUTURES", "channel": "orders-algo", "param": "default"}
        ]
        channels = [
            bitget_ws_client.SubscribeReqCoin(info["inst_type"], info["channel"], info["param"])
            if info["channel"] == "account"
            else bitget_ws_client.SubscribeReq(info["inst_type"], info["channel"], info["param"])
            for info in self.lst_channels
        ]

        def on_message_account(message):
            if self.verbose:
                print(">> ACCOUNT >>", message)
            if "event" in json.loads(message) and "arg" in json.loads(message):
                if self.verbose:
                    print(message)
                pass
            elif "data" in json.loads(message):
                data = json.loads(message)["data"]
                arg = json.loads(message)["arg"]

                if data and isinstance(arg, dict) and arg["channel"] == "account":
                    data = data[0]
                    if self.verbose:
                        print("marginCoin: ", data['marginCoin'],
                              " available: ", data['available'],
                              " maxOpenPosAvailable: ", data['maxOpenPosAvailable'],
                              " usdtEquity: ", data['usdtEquity']
                              )
                    # self.state["account"]["marginCoin"] = data['marginCoin']
                    self.state["account"]["available"] = float(data['available'])
                    self.state["account"]["maxOpenPosAvailable"] = float(data['maxOpenPosAvailable'])
                    self.state["account"]["usdtEquity"] = float(data['usdtEquity'])

                elif data and isinstance(arg, dict) and arg["channel"] == "positions":   # CEDE TEST LST OF DCT POSITIONS
                    self.state["positions"] = pd.DataFrame(data)
                    if self.verbose:
                        print(self.state["positions"].to_string(index=False))

                elif data and isinstance(arg, dict) and arg["channel"] == "orders":
                    self.state["orders"] = pd.DataFrame(data)
                    if self.verbose:
                        print(self.state["orders"].to_string(index=False))

                elif data and isinstance(arg, dict) and arg["channel"] == "orders-algo":
                    self.state["orders-algo"] = pd.DataFrame(data)
                    if self.verbose:
                        print(self.state["orders-algo"].to_string(index=False))

                else:
                    try:
                        if isinstance(arg, dict) \
                                and arg["channel"] != "account" \
                                and arg["channel"] != "positions" \
                                and arg["channel"] != "orders" \
                                and arg["channel"] != "orders-algo":
                            if self.verbose:
                                print("Received missed:", message)
                    except:
                        if self.verbose:
                            print("Received nok:", message)

        self.client_account.subscribe(channels, on_message_account) # TORESTORE

        # Tickers
        def on_message_ticker(message):
            if self.verbose:
                print(">> TICKER >>", message)
            if "event" in json.loads(message) and "arg" in json.loads(message):
                if self.verbose:
                    print(message)
                pass
            elif "data" in json.loads(message) and "arg" in json.loads(message):
                data = json.loads(message)["data"]
                arg = json.loads(message)["arg"]

                if data and isinstance(arg, dict) and arg["channel"] == "ticker":
                    lst_data = json.loads(message)["data"]
                    symbol = lst_data[0]['instId']
                    if symbol.endswith("USDT"):
                        symbol = symbol.replace("USDT", "")
                    if symbol in self.lst_tickers:
                        self.state["ticker_prices"][lst_data[0]['instId']] = {
                            "timestamp": datetime.timestamp(datetime.now()),
                            "symbols": lst_data[0]['instId'],
                            "values": lst_data[0]['lastPr']
                        }

                elif json.loads(message)["arg"]["channel"] != "ticker":
                    if self.verbose:
                        print("Received:", message)

        for symbol in self.symbols:
            if not symbol.endswith("USDT"):
                symbol = symbol + "USDT"
            channels = [bitget_ws_client.SubscribeReq("USDT-FUTURES", "ticker", symbol)]
            self.client_tickers.subscribe(channels, on_message_ticker)
            self.lst_channels.append({
                "inst_type": "USDT-FUTURES",
                "channel": "ticker",
                "inst_id": symbol
            })

        lst_subscribed_account_channels = self.client_account.get_subscribed_channels()
        lst_subscribed_tickers_channels = self.client_tickers.get_subscribed_channels()
        if utils.dict_lists_equal(lst_subscribed_account_channels + lst_subscribed_tickers_channels,
                                  self.lst_channels):
            self.status = "On"
        else:
            self.status = "Failed"

    def stop(self):
        self.client_account.close()
        self.client_tickers.close()

    def __del__(self):
        print("destructor")
        #self.client.close()

    def get_state(self):
        state = self.state.copy()
        self.state = self.reset_state  # CEDE TBC
        return state

    def get_status(self):
        return self.status