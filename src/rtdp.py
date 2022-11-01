from . import utils
import pandas as pd
from abc import ABCMeta, abstractmethod
import csv
import json
from datetime import datetime
from . import features # temporary (before using fdp)
from rich import inspect,print

default_symbols = [
        "BTC/USD",
        "DOGE/USD",
        #"MANA/USD",
        "CHZ/USD",
        "AAVE/USD",
        "BNB/USD",
        "ETH/USD",
        "MATIC/USD",
        "XRP/USD",
        "SAND/USD",
        "OMG/USD",
        "CRV/USD",
        "TRX/USD",
        "FTT/USD",
        "GRT/USD",
        "SRM/USD",
        "FTM/USD",
        "LTC/USD",
        #"RUNE/USD",
        "CRO/USD",
        "UNI/USD",
        "SUSHI/USD",
        "LRC/USD",
        "LINK/USD",
        "BCH/USD",
        "AXS/USD",
        "RAY/USD",
        "SOL/USD",
        #"AVAX/USD"
    ]
'''
default_symbols = [
        "BTC/USD",
        "OMG/USD"
    ]
'''

default_features = ["open", "close", "high", "low", "volume"]

class DataDescription():
    def __init__(self):
        self.symbols = default_symbols
        self.features = default_features

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
        pass

    def tick(self):
        pass

    def get_current_data(self, data_description):
        
        # hack : in the case where we want only the close value, we return the current value of the symbol
        if data_description.features == {'close': None}:
            import ccxt
            data = {"symbol":[], "close":[]}
            exchange = ccxt.ftx()
            for symbol in data_description.symbols:
                for _ in range(3):
                    try:
                        ticker = exchange.fetch_ticker(symbol)
                        currentValue = (float(ticker['info']['ask']) + float(ticker['info']['bid'])) / 2
                        data["symbol"].append(symbol)
                        data["close"].append(currentValue)
                        break
                    except BaseException as err:
                        print("[rtdp:get_current_data] can't fetch ticker for {} : {}", symbol, err)

            df_result = pd.DataFrame(data)
            df_result.set_index("symbol", inplace=True)
            print(df_result)
            return df_result


        symbols = ','.join(data_description.symbols)
        symbols = symbols.replace('/','_')
        params = { "service":"history", "exchange":"ftx", "symbol":symbols, "start":"2022-10-20", "interval": "1d", "indicators": data_description.features}
        response_json = utils.fdp_request_post("history", params)

        data = {feature: [] for feature in data_description.features}
        data["symbol"] = []
        
        if response_json["status"] == "ok":
            for symbol in data_description.symbols:
                formatted_symbol = symbol.replace('/','_')
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
        return None

    def get_current_datetime(self, format = None):
        current_datetime = datetime.now()
        if isinstance(current_datetime, datetime) and format != None:
            current_datetime = current_datetime.strftime(format)
        return current_datetime

    def check_data_description(self, data_description):
        pass

    def get_final_datetime(self):
        return None

