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

class StrategyBollingerTrend(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        self.zero_print = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols
        """
        ds.features = {"close": None,
                       "n1_close": {"window_size": 1},
                       "bollinger": {"window_size": 100, "bol_std": 2.25, "min_bol_spread": 0},
                       "lower_band": {"window_size": None},
                       "n1_lower_band": {"window_size": None},
                       "higher_band": {"window_size": None},
                       "n1_higher_band": {"window_size": None},
                       "ma_band": {"window_size": None},
                       "n1_ma_band": {"window_size": None},
                       "long_ma": {"window_size": 500}
                       }
        """
        ds.fdp_features = {"close": None,
                           "bollinger1": {"indicator": "bollinger", "id":"1", "window_size": 100, "bol_std": 2.25, "min_bol_spread": 0, "output": ["lower_band", "higher_band", "ma_band"]},
                           "bollinger2": {"indicator": "bollinger", "id":"2", "window_size": 100, "bol_std": 2.25, "min_bol_spread": 0, "output": ["lower_band", "higher_band", "ma_band"]},
                           "sma_short": {"indicator": "sma", "window_size": 10},
                           "sma_long": {"indicator": "sma", "window_size": 20}
                           }
        ds.features = self.get_feature_from_fdp_features(ds.fdp_features)

        return ds

    def get_info(self):
        return "StrategyBollingerTrend"

    def condition_for_opening_long_position(self, symbol):
       return (self.df_current_data['n1_close'][symbol] < self.df_current_data['n1_higher_band'][symbol])\
               & (self.df_current_data['close'][symbol] > self.df_current_data['higher_band'][symbol])\
               & (abs(self.df_current_data['n1_higher_band'][symbol] - self.df_current_data['n1_lower_band'][symbol]) / self.df_current_data['n1_lower_band'][symbol] > self.min_bol_spread)\
               & (self.df_current_data["close"][symbol] > self.df_current_data["long_ma"][symbol])


    def condition_for_opening_short_position(self, symbol):
        return (self.df_current_data['n1_close'][symbol] > self.df_current_data['n1_lower_band'][symbol])\
               & (self.df_current_data['close'][symbol] < self.df_current_data['lower_band'][symbol])\
               & (abs(self.df_current_data['n1_higher_band'][symbol] - self.df_current_data['n1_lower_band'][symbol]) / self.df_current_data['n1_lower_band'][symbol] > self.min_bol_spread)\
               & (self.df_current_data["close"][symbol] < self.df_current_data["long_ma"][symbol])

    def condition_for_closing_long_position(self, symbol):
        return self.df_current_data['close'][symbol] < self.df_current_data['ma_band'][symbol]

    def condition_for_closing_short_position(self, symbol):
        return (self.df_current_data['close'][symbol] > self.df_current_data['ma_band'][symbol])




















