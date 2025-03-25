from . import bitget_ws_client
import json
import pandas as pd
import utils

from bitget_ws_candle_data import WSCandleData

class WSCandle:

    def __init__(self, source, params=None):
        self.id = None
        self.status = "Off"

        self.candle_data = WSCandleData(params)

        if source:
            self.id = source.get("id", self.id)

        self.client = bitget_ws.BitgetWsClient(
            ws_url=bitget_ws.CONTRACT_WS_URL_PUBLIC,
            verbose=True) \
            .error_listener(bitget_ws.handel_error) \
            .build()

        timeframe_map = {
            "1m": "1m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "1h": "1H",
            "4h": "4H",
        }

        channels = [
            bitget_ws.SubscribeReq(
                "USDT-FUTURES",
                f"candle{timeframe_map.get(d['timeframe'], d['timeframe'])}",
                f"{d['symbol']}USDT"
            )
            for d in params
        ]

        # 2) Build your list of channels
        self.lst_channels = [
            {
                "inst_type": "USDT-FUTURES",
                "channel": f"candle{timeframe_map.get(item['timeframe'], item['timeframe'])}",
                "inst_id": f"{item['symbol']}USDT"
            }
            for item in params
        ]

        def on_message(message):
            json_obj = json.loads(message)
            action = str(json_obj.get('action')).replace("\'", "\"")
            if action == "snapshot" or action == 'update':
                arg = json_obj.get('arg')
                if arg['instType'] == 'USDT-FUTURES' \
                        and arg['channel'].startswith("candle"):
                    symbol = arg['instId']
                    timeframe = arg['channel'].replace("candle", "", 1)
                    data = json_obj.get('data')
                    candle_data = json_obj.get("data", [])
                    # Assuming each candle is in the format:
                    # [timestamp, open, high, low, close, volume]
                    df = pd.DataFrame(candle_data,
                                      columns=["timestamp", "open", "high", "low", "close", "volume", "volume_2", "volume_3"])
                    df = df.drop(columns=['volume_2', 'volume_3'])
                    df = df.rename(columns={0: 'timestamp', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'})
                    cols = ["open", "high", "low", "close", "volume"]
                    df[cols] = df[cols].astype(float)
                    if not df.empty:
                        df = df.set_index(df['timestamp'])
                        df.index = df.index.str.replace(r'0{3}$', '', regex=True)
                        df.index = pd.to_datetime(df.index.astype(int), unit='s', utc=True, errors='coerce')
                        self.candle_data.set_value(symbol, timeframe, df)
                else:
                    exit(73492)
            else:
                exit(7392)


        self.client.subscribe(channels, on_message)

        lst_subscribed_candle_channels = self.client.get_subscribed_channels()
        if utils.dict_lists_equal(lst_subscribed_candle_channels,
                                  self.lst_channels):
            self.status = "On"
        else:
            self.status = "Failed"

    def stop(self):
        self.client.close()

    def __del__(self):
        print("destructor")
        self.client.close()

    def request(self, service, params=None):
        # if service == "last":
        #     return self.df
        return None

