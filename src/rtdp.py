from . import utils
import pandas as pd
from abc import ABCMeta, abstractmethod
import csv
import json
from datetime import datetime
from . import features # temporary (before using fdp)

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
        # "BTC/USD"
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
    def get_current_datetime(self):
        pass

class RealTimeDataProvider(IRealTimeDataProvider):
    def __init__(self, params = None):
        pass

    def tick(self):
        pass

    def get_current_data(self, data_description):
        symbols = ','.join(data_description.symbols)
        symbols = symbols.replace('/','_')
        url = "history?exchange=ftx&symbol="+symbols+"&start=2022-06-01"+"&interval=1h"
        response_json = utils.fdp_request(url)

        df_result = pd.DataFrame(columns=['symbol'])
        for symbol in data_description.symbols:
            formatted_symbol = symbol.replace('/','_')
            df = pd.read_json(response_json["result"][formatted_symbol]["info"])
            df = features.add_features(df, data_description.features)
            columns = list(df.columns)

            row = {'symbol':symbol}
            for feature in data_description.features:
                if feature not in columns:
                    return None
                row[feature] = [df[feature].iloc[-1]]

            df_row = pd.DataFrame(data=row)
            df_result = pd.concat((df_result, df_row), axis = 0)

        df_result.set_index("symbol", inplace=True)

        return df_result

    def get_value(self, symbol):
        return None

    def get_current_datetime(self):
        return datetime.now()


