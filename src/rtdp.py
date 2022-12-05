from . import utils
import pandas as pd
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from rich import inspect,print

from . import utils,chronos

'''
default_symbols = [
        "BTC/USD",
        "DOGE/USD",
        "MANA/USD",
        "CHZ/USD",
        "AAVE/USD",
        # "BNB/USD",
        "ETH/USD",
        # "MATIC/USD",
        "XRP/USD",
        "SAND/USD",
        # "OMG/USD",
        "CRV/USD",
        "TRX/USD",
        # "FTT/USD",
        "GRT/USD",
        "SRM/USD",
        "FTM/USD",
        "LTC/USD",
        "RUNE/USD",
        # "CRO/USD",
        "UNI/USD",
        "SUSHI/USD",
        "LRC/USD",
        "LINK/USD",
        "BCH/USD",
        "AXS/USD",
        "RAY/USD",
        "SOL/USD",
        "AVAX/USD"
    ]

default_symbols = [
        "BTC/USDT",
    ]
'''
'''
# synthethic datas
default_symbols = [
        "SINGLESINUS1FLAT",
        "SINGLESINUS2FLAT",
        "MIXEDSINUSFLAT",
        "SINGLESINUS2UP",
        "SINGLESINUS1UP"
    ]
'''
default_symbols = [
        "BTC/USDT",
        "DOGE/USDT",
        "MANA/USDT",
        "CHZ/USDT",
        "AAVE/USDT",
        "BNB/USDT",
        "ETH/USDT",
        "MATIC/USDT",
        "XRP/USDT",
        "SAND/USDT",
        # "OMG/USD",
        "CRV/USDT",
        "TRX/USDT",
        # "FTT/USD",
        "GRT/USDT",
        "SRM/USDT",
        "FTM/USDT",
        "LTC/USDT",
        "RUNE/USDT",
        # "CRO/USDT",
        "UNI/USDT",
        "SUSHI/USDT",
        "LRC/USDT",
        "LINK/USDT",
        "BCH/USDT",
        "AXS/USDT",
        "RAY/USDT",
        "SOL/USDT",
        "AVAX/USDT"
    ]


default_features = ["open", "close", "high", "low", "volume"]

class DataDescription():
    def __init__(self):
        self.symbols = default_symbols
        self.features = default_features
        self.interval = "1d"

class IRealTimeDataProvider(metaclass = ABCMeta):
    def __init__(self, params = None):
        pass

    @abstractmethod
    def tick(self):
        pass

    @abstractmethod
    def get_current_data(self, data_description):
        return None

    @abstractmethod
    def get_value(self, symbol):
        pass

    @abstractmethod
    def get_current_datetime(self, format = None):
        pass

    @abstractmethod
    def check_data_description(self, data_description):
        pass

class RealTimeDataProvider(IRealTimeDataProvider):
    def __init__(self, params = None):
        self.start = None
        self.end = None
        self.intervals = None

        if params:
            self.start = params.get("start_date", self.start)
            self.end = params.get("end_date", self.end)
            self.intervals = params.get("intervals", self.intervals)

        if self.start != None and self.end != None:
            self.scheduler = chronos.Chronos(self.start, self.end, self.intervals)
        else:
            self.scheduler = chronos.Chronos()
        pass

    def tick(self):
        if self.start != None and self.end != None:
            self.scheduler.increment_time()
            pass
        else:
            pass

    def get_current_data(self, data_description):

        symbols = ','.join(data_description.symbols)
        symbols = symbols.replace('/','_')

        # adapt the interval according to the frequency given to crag (todo : the interval should be provided by the strategy itself)
        interval = "1d"
        if data_description.interval <= 60 * 60:
            interval = "1h"
        if data_description.interval <= 60:
            interval = "1m"
        
        # adapt the start date
        # start = datetime.now() - timedelta(days=400) # Modif CEDE
        end = self.get_current_datetime()
        if interval == "1d":
            start = end - timedelta(days=400)
        if interval == "1h":
            start = end - timedelta(hours=400)
        elif interval == "1m":
            start = end - timedelta(minutes=400)
        start_timestamp = str(int( 1000 * datetime.timestamp(start)))
        end_timestamp = str(int(1000 * datetime.timestamp(end)))

        params = { "service":"history", "exchange":"binance", "symbol":symbols, "start":start_timestamp, "end":end_timestamp ,"interval": interval, "indicators": data_description.features}
        response_json = utils.fdp_request_post("history", params)

        data = {feature: [] for feature in data_description.features}
        data["symbol"] = []
        
        if response_json["status"] == "ok":
            for symbol in data_description.symbols:
                formatted_symbol = symbol.replace('/','_')
                if response_json["result"][formatted_symbol]["status"] == "ko":
                    print("[RealTimeDataProvider:get_current_data] !!!! no data for ", symbol)
                    continue
                df = pd.read_json(response_json["result"][formatted_symbol]["info"])
                columns = list(df.columns)
                data["symbol"].append(symbol)
                for feature in data_description.features:
                    if feature not in columns:
                        return None
                    data[feature].append(df[feature].iloc[-1])

        df_result = pd.DataFrame(data)
        df_result.set_index("symbol", inplace=True)
        return df_result

    def get_value(self, symbol):
        if self.start != None and self.end != None:
            ds = DataDescription()
            ds.symbols = [
                symbol,
            ]
            ds.features = {"close": None,
                           }
            if self.intervals == "1d":
                ds.interval = 60 * 60 * 24
            elif self.intervals == "1h":
                ds.interval = 60 * 60
            elif self.intervals == "1m":
                ds.interval = 60
            df_price = self.get_current_data(ds)
            return df_price.loc[df_price.index == symbol, 'close'].iloc[0]
        else:
            return None

    def get_current_datetime(self, format = None):
        current_datetime = self.scheduler.get_current_time()
        if isinstance(current_datetime, datetime) and format != None:
            current_datetime = current_datetime.strftime(format)
        return current_datetime

    def check_data_description(self, data_description):
        pass

    def get_final_datetime(self):
        if self.start != None and self.end != None:
            return datetime.strptime(self.end, "%Y-%m-%d")
        else:
            return None

