from . import utils
import pandas as pd
from abc import ABCMeta, abstractmethod
import csv
import json
import os
from datetime import datetime

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
    df["ema_short"] = df["ema_short"].shift(1)
    df["ema_long"] = TA.EMA(df, period = 400).copy()
    df["ema_long"] = df["ema_long"].shift(1)
    super_trend = SuperTrend(
            df['high'], 
            df['low'], 
            df['close'], 
            15, # self.st_short_atr_window
            5 # self.st_short_atr_multiplier
        )
        
    df['super_trend_direction'] = super_trend.super_trend_direction()
    df['super_trend_direction'] = df['super_trend_direction'].shift(1)

    df_buy_sell = pd.read_csv('BTC_buy_sell.csv')

    df['open_long_limit'] = df_buy_sell['open_long_limit']
    df['close_long_limit'] = df_buy_sell['close_long_limit']

    return df

default_symbols = [
        "BTC/USD",
        "DOGE/USD",
        # "MANA/USD",
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
        "SOL/USD"
        # "AVAX/USD"
    ]
'''
default_symbols = [
        "BTC/USD"
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
    def next(self, data_description):
        pass

    @abstractmethod
    def get_value(self, symbol):
        pass

    @abstractmethod
    def get_current_datetime(self):
        pass

class RealTimeDataProvider():
    def __init__(self, params = None):
        pass

    def next(self, data_description):
        symbols = ','.join(data_description.symbols)
        symbols = symbols.replace('/','_')
        url = "history?exchange=ftx&symbol="+symbols+"&start=01_05_2022"+"&interval=1h"+"&length=500"
        response_json = utils.fdp_request(url)

        df_result = pd.DataFrame(columns=['symbol'])
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

    def get_current_datetime(self):
        return datetime.now()

class SimRealTimeDataProvider(IRealTimeDataProvider):
    def __init__(self, params = None):
        self.input = "./data/"
        self.scheduler = None
        if params:
            self.input = params.get("input", self.input)
            self.scheduler = params.get("chronos", self.scheduler)
    
        self.data = {}
        # self.current_position = -1 # CEDE Test Debug
        # self.current_position = 400  # CEDE Test Debug Offset
        self.current_position = self.scheduler.get_current_position()

        if self.input == None or not os.path.exists(self.input):
            return
        list_dates = []
        files = os.listdir(self.input)
        for file in files:
            strs = file.split('.')
            symbol = strs[0].replace("_", "/")
            if symbol in default_symbols and strs[1] == "csv":
                df = pd.read_csv(self.input+"/"+file, sep=";")
                df.rename(columns={"Unnamed: 0": "datetime"}, inplace=True)

                df["ema_long"] = df["ema_long"].shift(1)
                df["ema_short"] = df["ema_short"].shift(1)
                df['super_trend_direction'] = df['super_trend_direction'].shift(1)

                # df_buy_sell = pd.read_csv('BTC_buy_sell.csv')
                # df['open_long_limit'] = df_buy_sell['open_long_limit']
                # df['close_long_limit'] = df_buy_sell['close_long_limit']

                #df.set_index('datetime', inplace=True)
                self.data[symbol] = df
                list_dates.extend(self.data[symbol]['timestamp'].to_list())
                list_dates = list(set(list_dates))
        df_timestamp = pd.DataFrame(list_dates, columns=['timestamp'])
        df_timestamp.set_index('timestamp', inplace=True)
        for symbol in self.data:
            self.data[symbol].set_index('timestamp', inplace=True)
            df_backup = self.data[symbol].copy()
            self.data[symbol] = pd.concat([self.data[symbol], df_timestamp], axis=0)
            self.data[symbol].sort_index(ascending=True, inplace=True)
            self.data[symbol] = self.data[symbol].index.drop_duplicates(keep=False)
            if len(self.data[symbol]) == 0:
                self.data[symbol] = df_backup.copy()
            else:
                self.data[symbol] = pd.DataFrame(index=self.data[symbol])
                self.data[symbol] = pd.concat([self.data[symbol], df_backup], axis=0)
                self.data[symbol] = self.data[symbol][~self.data[symbol].index.duplicated(keep='first')]
            self.data[symbol].sort_index(ascending=True, inplace=True)
            self.data[symbol].reset_index(inplace=True)
            self.data[symbol]['datetime'] = self.data[symbol].index
            # symbol_file_name = symbol.replace("/", "_")
            # self.data[symbol].to_csv('./toto/' + symbol_file_name + '.csv')
            print(symbol,'   ',len(self.data[symbol]))

    '''
    def _is_in_dataframe(self):
        if self.current_position < 0:
            return False
        if not bool(self.data):
            return False
        for symbol in self.data:
            if self.current_position >= len(self.data[symbol].index):
                return False
        return True
    '''

    def _is_in_dataframe(self):
        if self.scheduler.get_current_position() < 0:
            return False
        if not bool(self.data):
            return False
        for symbol in self.data:
            if self.scheduler.get_current_position() >= len(self.data[symbol].index):
                return False
        return True

    '''
    def _is_last_in_dataframe(self):
        for symbol in self.data:
            if self.current_position >= len(self.data[symbol].index):
                return True
        return False
    '''

    def _is_last_in_dataframe(self):
        for symbol in self.data:
            if self.scheduler.get_current_position() >= len(self.data[symbol].index):
                return True
        return False

    def next(self, data_description):
        # self.current_position = self.current_position + 1
        self.current_position = self.scheduler.get_current_position()
        if not self._is_in_dataframe():
            return None

        df_result = pd.DataFrame(columns=['symbol'])
        for symbol in data_description.symbols:
            df_symbol = self.data[symbol]
            available_columns = list(df_symbol.columns)
            row = {'symbol':symbol}
            for feature in data_description.features:
                if feature not in available_columns:
                    return None
                # row[feature] = [df_symbol[feature].iloc[self.current_position]]
                row[feature] = [df_symbol[feature].iloc[self.scheduler.get_current_position()]]


            df_row = pd.DataFrame(data=row)
            df_result = pd.concat((df_result, df_row), axis = 0)

        df_result.set_index("symbol", inplace=True)

        return df_result

    def get_value(self, symbol):
        if not self._is_in_dataframe() or not symbol in self.data:
            if self._is_last_in_dataframe():
                df_symbol = self.data[symbol]
                value = df_symbol.iloc[len(self.data[symbol].index)-1]['close']
                return value
            else:
                return -1
        df_symbol = self.data[symbol]
        # value = df_symbol.iloc[self.current_position]['close']
        value = df_symbol.iloc[self.scheduler.get_current_position()]['close']
        return value

    def get_current_datetime(self):
        if not self._is_in_dataframe():
            return None
        first_symbol = list(self.data.keys())[0]
        df_symbol = self.data[first_symbol]
        #value = df_symbol.iloc[self.current_position]['datetime']
        #value = df_symbol.iloc[self.current_position]['timestamp']
        value = self.scheduler.get_current_time()
        return value

    def record(self, data_description, target="./data/"):
        symbols = ','.join(data_description.symbols)
        symbols = symbols.replace('/','_')
        url = "history?exchange=ftx&symbol="+symbols+"&start=01_01_2021"+"&interval=1h"+"&length=9000"
        # url = "history?exchange=ftx&symbol=" + symbols + "&start=01_01_2020" + "&interval=1h" + "&length=400"
        # url = "history?exchange=ftx&symbol=" + symbols + "&start=01_01_2021" + "&interval=1h" + "&end=01_01_2022"
        response_json = utils.fdp_request(url)
        for symbol in data_description.symbols:
            formatted_symbol = symbol.replace('/','_')
            if response_json["result"][formatted_symbol]["status"] == "ko":
                print("no data for ",symbol)
                continue
            df = pd.read_json(response_json["result"][formatted_symbol]["info"])
            df = add_features(df, data_description.features)
            if not os.path.exists(target):
                os.makedirs(target)
            df.to_csv(target+'/'+formatted_symbol+".csv", sep=";")

