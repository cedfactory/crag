import asyncio
import websockets
import time
import json
import base64
import hmac
import hashlib
import pandas as pd

from datetime import datetime

class BitgetWebSocketClient:
    def __init__(self, lst_ticker, api_key, passphrase, secret_key):
        self.API_KEY = api_key
        self.PASSPHRASE = passphrase
        self.SECRET_KEY = secret_key
        self.lst_ticker = lst_ticker

        self.print_out = False

        self.state = {
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

    async def subscribe_account_balance(self):
        uri = "wss://ws.bitget.com/v2/ws/private"
        async with websockets.connect(uri) as websocket:
            # Prepare login message with API credentials
            timestamp = str(int(time.time()))
            message = timestamp + "GET" + "/user/verify"
            signature = base64.b64encode(
                hmac.new(
                    self.SECRET_KEY.encode('utf-8'),
                    message.encode('utf-8'),
                    hashlib.sha256
                ).digest()
            ).decode('utf-8')
            login_req = {
                "op": "login",
                "args": [{
                    "apiKey": self.API_KEY,
                    "passphrase": self.PASSPHRASE,
                    "timestamp": timestamp,
                    "sign": signature
                }]
            }
            # Send login request and wait for success
            await websocket.send(json.dumps(login_req))
            response = await websocket.recv()
            print("Auth response:", response)
            # (You should parse response and check for login success code here)
            # Now subscribe to the account balance channel for all coins (spot)
            sub_req = {
                "op": "subscribe",
                "args": [
                    {
                        "instType": "USDT-FUTURES",
                        "channel": "account",
                        "coin": "default"
                    },
                    {
                        "instType": "USDT-FUTURES",
                        "channel": "positions",
                        "instId": "default"
                    },
                    {
                        "instType": "USDT-FUTURES",
                        "channel": "orders",
                        "instId": "default"
                    },
                    {
                        "instType": "USDT-FUTURES",
                        "channel": "orders-algo",
                        "instId": "default"
                    }
                ]
            }
            await websocket.send(json.dumps(sub_req))

            sub_req = {
                "op": "subscribe",
                "args": [{
                    "instType": "USDT-FUTURES",
                    "channel": "positions",
                    "instId": "default"
                },
                    {
                        "instType": "USDT-FUTURES",
                        "channel": "orders",
                        "instId": "default"
                    }
                ]
            }
            #await websocket.send(json.dumps(sub_req))

            sub_req = {
                "op": "subscribe",
                "args": [{
                    "instType": "USDT-FUTURES",
                    "channel": "orders",
                    "instId": "default"
                }]
            }
            #await websocket.send(json.dumps(sub_req))

            # Listen for balance updates
            while True:
                message = await websocket.recv()
                # print(message)
                if "event" in json.loads(message) and "arg" in json.loads(message):
                    print(message)
                    pass
                elif "data" in json.loads(message):
                    data = json.loads(message)["data"]
                    arg = json.loads(message)["arg"]

                    if data and isinstance(arg, dict) and arg["channel"] == "account":
                        if self.print_out:
                            print("marginCoin: ", data['marginCoin'],
                                  " available: ", data['available'],
                                  " maxOpenPosAvailable: ", data['maxOpenPosAvailable'],
                                  " usdtEquity: ", data['usdtEquity']
                                  )
                        data = data[0]
                        self.state["account"]["marginCoin"] = data['marginCoin']
                        self.state["account"]["available"] = data['available']
                        self.state["account"]["maxOpenPosAvailable"] = data['maxOpenPosAvailable']
                        self.state["account"]["usdtEquity"] = data['usdtEquity']

                    elif data and isinstance(arg, dict) and arg["channel"] == "positions":   # CEDE TEST LST OF DCT POSITIONS
                        self.state["positions"] = pd.DataFrame(data)
                        if self.print_out:
                            print(self.state["positions"].to_string(index=False))

                    elif data and isinstance(arg, dict) and arg["channel"] == "orders":
                        self.state["orders"] = pd.DataFrame(data)
                        if self.print_out:
                            print(self.state["orders"].to_string(index=False))

                    elif data and isinstance(arg, dict) and arg["channel"] == "orders-algo":
                        self.state["orders-algo"] = pd.DataFrame(data)
                        if self.print_out:
                            print(self.state["orders-algo"].to_string(index=False))

                    else:
                        try:
                            if isinstance(arg, dict) \
                                    and arg["channel"] != "account" \
                                    and arg["channel"] != "positions" \
                                    and arg["channel"] != "orders" \
                                    and arg["channel"] != "orders-algo":
                                print("Received:", message)
                        except:
                            print("Received nok:", message)

    async def subscribe_ticker(self):
        uri = "wss://ws.bitget.com/v2/ws/public"
        async with websockets.connect(uri) as websocket:
            for symbol in self.lst_ticker:
                sub_req = {
                    "op": "subscribe",
                    "args": [{
                        "instType": "USDT-FUTURES",
                        "channel": "ticker",
                        "instId": symbol
                    }]
                }
                await websocket.send(json.dumps(sub_req))
            # Await confirmation (subscribe event) and then continuously read data
            while True:
                message = await websocket.recv()

                if "event" in json.loads(message) and "arg" in json.loads(message):
                    print(message)
                    pass
                elif "data" in json.loads(message) and "arg" in json.loads(message):
                    data = json.loads(message)["data"]
                    arg = json.loads(message)["arg"]

                    if data and isinstance(arg, dict) and arg["channel"] == "ticker":
                        lst_data = json.loads(message)["data"]
                        if lst_data[0]['instId'] in self.lst_ticker:
                            self.state["ticker_prices"][lst_data[0]['instId']] = {
                                "timestamp": datetime.timestamp(datetime.now()),
                                "symbols": lst_data[0]['instId'],
                                "values": lst_data[0]['lastPr']
                            }

                    elif json.loads(message)["arg"]["channel"] != "ticker":
                        print("Received:", message)

    def get_state(self):
        """
        Accessor method that returns a copy of the current state.
        """
        return self.state.copy()

    async def run(self):
        # Run both subscriptions concurrently.
        await asyncio.gather(
            self.subscribe_ticker(),
            self.subscribe_account_balance()
        )
