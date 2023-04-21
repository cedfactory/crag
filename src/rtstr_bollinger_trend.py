import pandas as pd
import numpy as np
from . import trade
import json
import time
import csv
from datetime import datetime
import datetime

from . import rtdp, rtstr, utils, rtctrl

# Reference: https://crypto-robot.com/blog/bollinger-trend
# Reference: https://github.com/CryptoRobotFr/backtest_tools/blob/main/backtest/single_coin/bol_trend.ipynb

class StrategyBollingerTrend(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        self.zero_print = True

        self.min_bol_spread = 0

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols

        ds.fdp_features = {"close": {},
                           "bollinger_id1": {"indicator": "bollinger", "window_size": 100, "id": "1", "bol_std": 2.25, "output": ["lower_band", "higher_band", "ma_band"]},
                           "rsi": {"indicator": "rsi", "id": "1", "window_size": 14},
                           # "long_ma": {"indicator": "sma", "id": "long_ma", "window_size": 500},
                           "postprocess1": {"indicator": "shift", "window_size": 1, "id": "1", "n": "1", "input": ['lower_band', "higher_band", "ma_band"]},
                           "postprocess2": {"indicator": "shift", "window_size": 1, "n": "1", "input": ["close"]}
                           }

        ds.features = self.get_feature_from_fdp_features(ds.fdp_features)
        print("startegy: ", self.get_info())
        print("strategy features: ", ds.features)

        return ds

    def get_info(self):
        return "StrategyBollingerTrend"

    def condition_for_opening_long_position(self, symbol):
        return (self.df_current_data['n1_close'][symbol] < self.df_current_data['n1_higher_band_1'][symbol])\
               & (self.df_current_data['close'][symbol] > self.df_current_data['higher_band_1'][symbol])\
               & (abs(self.df_current_data['n1_higher_band_1'][symbol] - self.df_current_data['n1_lower_band_1'][symbol]) / self.df_current_data['n1_lower_band_1'][symbol] > self.min_bol_spread) \
               & (self.df_current_data["rsi_1"][symbol] > 52)
               # & (self.df_current_data["close"][symbol] > self.df_current_data["sma_long_ma"][symbol])

    def condition_for_opening_short_position(self, symbol):
        return (self.df_current_data['n1_close'][symbol] > self.df_current_data['n1_lower_band_1'][symbol])\
               & (self.df_current_data['close'][symbol] < self.df_current_data['lower_band_1'][symbol])\
               & (abs(self.df_current_data['n1_higher_band_1'][symbol] - self.df_current_data['n1_lower_band_1'][symbol]) / self.df_current_data['n1_lower_band_1'][symbol] > self.min_bol_spread) \
               & (self.df_current_data["rsi_1"][symbol] < 48)
               # & (self.df_current_data["close"][symbol] < self.df_current_data["sma_long_ma"][symbol])

    def condition_for_closing_long_position(self, symbol):
        return self.df_current_data['close'][symbol] < self.df_current_data['ma_band_1'][symbol]

    def condition_for_closing_short_position(self, symbol):
        return (self.df_current_data['close'][symbol] > self.df_current_data['ma_band_1'][symbol])




















