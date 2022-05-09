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
        url = "history?exchange=ftx&symbol=ETH_EUR&start=01_01_2022"
        response_json = utils.fdp_request(url)
        df = pd.read_json(response_json["result"]["ETH_EUR"]["info"])
        return df

class MyRealTimeDataProvider(IRealTimeDataProvider):
    def __init__(self, params = None):
        print(params)

    def next(self, data_description):
        url = "history?exchange=ftx&symbol=ETH_EUR&start=01_01_2022"
        response_json = utils.fdp_request(url)
        df = pd.read_json(response_json["result"]["ETH_EUR"]["info"])
        return df
