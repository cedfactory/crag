from . import utils
import pandas as pd
from abc import ABCMeta, abstractmethod

default_symbols = [
        "BTC/USD",
        "DOGE/USD",
        "MANA/USD",
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
        "RUNE/USD",
        "CRO/USD",
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
default_symbols = ["BTC/USD"] # TODO waiting for fix in fdp
default_features = ["open", "close", "high", "low", "volume"]

class DataDescription():
    def __init__(self):
        self.symbols = default_symbols
        self.features = default_features

class IRealTimeDataProvider(metaclass = ABCMeta):
    def __init__(self, params = None):
        pass

    @abstractmethod
    def next(self, data_description):
        pass

class RealTimeDataProvider():
    def __init__(self, params = None):
        print(params)

    def next(self, data_description):
        symbols = ','.join(data_description.symbols)
        symbols = symbols.replace('/','_')
        url = "history?exchange=ftx&symbol="+symbols+"&start=01_05_2022"
        response_json = utils.fdp_request(url)
        result = {}
        for symbol in data_description.symbols:
            formatted_symbol = symbol.replace('/','_')
            result[symbol] = pd.read_json(response_json["result"][formatted_symbol]["info"])
        return result

class MyRealTimeDataProvider(IRealTimeDataProvider):
    def __init__(self, params = None):
        print(params)

    def next(self, data_description):
        url = "history?exchange=ftx&symbol=ETH_EUR&start=01_01_2022"
        response_json = utils.fdp_request(url)
        df = pd.read_json(response_json["result"]["ETH_EUR"]["info"])
        return df
