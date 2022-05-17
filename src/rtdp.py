from . import utils
import pandas as pd
from abc import ABCMeta, abstractmethod
import csv
import json
import os

from finta import TA # temporary before fdp
import numpy as np
class SuperTrend():
    def __init__(
        self,
        high,
        low,
        close,
        atr_window=10,
        atr_multi=3
    ):
        self.high = high
        self.low = low
        self.close = close
        self.atr_window = atr_window
        self.atr_multi = atr_multi
        self._run()
        
    def _run(self):
        # calculate ATR
        price_diffs = [self.high - self.low, 
                    self.high - self.close.shift(), 
                    self.close.shift() - self.low]
        true_range = pd.concat(price_diffs, axis=1)
        true_range = true_range.abs().max(axis=1)
        # default ATR calculation in supertrend indicator
        atr = true_range.ewm(alpha=1/self.atr_window,min_periods=self.atr_window).mean() 
        # atr = ta.volatility.average_true_range(high, low, close, atr_period)
        # df['atr'] = df['tr'].rolling(atr_period).mean()
        
        # HL2 is simply the average of high and low prices
        hl2 = (self.high + self.low) / 2
        # upperband and lowerband calculation
        # notice that final bands are set to be equal to the respective bands
        final_upperband = upperband = hl2 + (self.atr_multi * atr)
        final_lowerband = lowerband = hl2 - (self.atr_multi * atr)
        
        # initialize Supertrend column to True
        supertrend = [True] * len(self.close)
        
        for i in range(1, len(self.close)):
            curr, prev = i, i-1
            
            # if current close price crosses above upperband
            if self.close[curr] > final_upperband[prev]:
                supertrend[curr] = True
            # if current close price crosses below lowerband
            elif self.close[curr] < final_lowerband[prev]:
                supertrend[curr] = False
            # else, the trend continues
            else:
                supertrend[curr] = supertrend[prev]
                
                # adjustment to the final bands
                if supertrend[curr] == True and final_lowerband[curr] < final_lowerband[prev]:
                    final_lowerband[curr] = final_lowerband[prev]
                if supertrend[curr] == False and final_upperband[curr] > final_upperband[prev]:
                    final_upperband[curr] = final_upperband[prev]

            # to remove bands according to the trend direction
            if supertrend[curr] == True:
                final_upperband[curr] = np.nan
            else:
                final_lowerband[curr] = np.nan
                
        self.st = pd.DataFrame({
            'Supertrend': supertrend,
            'Final Lowerband': final_lowerband,
            'Final Upperband': final_upperband
        })
        
    def super_trend_upper(self):
        return self.st['Final Upperband']
        
    def super_trend_lower(self):
        return self.st['Final Lowerband']
        
    def super_trend_direction(self):
        return self.st['Supertrend']
    

def add_features(df, features):
    df["ema_short"] = TA.EMA(df, period = 5).copy()
    df["ema_long"] = TA.EMA(df, period = 400).copy()
    super_trend = SuperTrend(
            df['high'], 
            df['low'], 
            df['close'], 
            15, # self.st_short_atr_window
            5 # self.st_short_atr_multiplier
        )
        
    df['super_trend_direction'] = super_trend.super_trend_direction()

    return df


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

    @abstractmethod
    def get_value(self, symbol):
        pass

class RealTimeDataProvider():
    def __init__(self, params = None):
        pass

    def next(self, data_description):
        symbols = ','.join(data_description.symbols)
        symbols = symbols.replace('/','_')
        url = "history?exchange=ftx&symbol="+symbols+"&start=01_05_2022"+"&interval=1h"+"&length=500"
        response_json = utils.fdp_request(url)

        columns = ['symbol'].extend(["open", "high", "low", "close", "volume"])
        df_result = pd.DataFrame(columns=columns)
        for symbol in data_description.symbols:
            formatted_symbol = symbol.replace('/','_')
            df = pd.read_json(response_json["result"][formatted_symbol]["info"])
            df = add_features(df, data_description.features)
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


class SimRealTimeDataProvider(IRealTimeDataProvider):
    def __init__(self, params = None):
        self.input = "./data/"
        if params:
            self.input = params.get("input", self.input)
    
        if self.input == None:
            return

        self.data = {}
        files = os.listdir(self.input)
        for file in files:
            strs = file.split('.')
            symbol = strs[0].replace("_", "/")
            if symbol in default_symbols and strs[1] == "csv":
                self.data[symbol] = pd.read_csv(self.input+"/"+file, sep=";")

        self.current_position = -1

    def next(self, data_description):
        offset = 20 # todo : deduce this value from maximal period found in data_description
        self.current_position = self.current_position + 1
        start_in_df = self.current_position
        end_in_df = self.current_position + offset

        columns = ['symbol'].extend(["open", "high", "low", "close", "volume"])
        df_result = pd.DataFrame(columns=['symbol'])
        for symbol in data_description.symbols:
            df_symbol = self.data[symbol]
            df = df_symbol[start_in_df:end_in_df]
            df = df.reset_index()

            df = add_features(df.copy(), data_description.features)
            
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
        return 10

    def record(self, data_description):
        symbols = ','.join(data_description.symbols)
        symbols = symbols.replace('/','_')
        url = "history?exchange=ftx&symbol="+symbols+"&start=01_04_2022"+"&interval=1h"+"&length=400"
        response_json = utils.fdp_request(url)
        for symbol in data_description.symbols:
            formatted_symbol = symbol.replace('/','_')
            df = pd.read_json(response_json["result"][formatted_symbol]["info"])
            df.to_csv("./data/"+formatted_symbol+".csv", sep=";")

