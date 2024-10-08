import pandas as pd
import numpy as np
import json
import time
import csv
from datetime import datetime
import datetime

from . import rtdp, rtstr, utils

# Reference: https://crypto-robot.com/blog/bollinger-trend
# Reference: https://github.com/CryptoRobotFr/backtest_tools/blob/main/backtest/single_coin/bol_trend.ipynb

class StrategyBollingerTrendLong(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.epsilon = 0.1 # CEDE specific for bollinger - to be included to .xml depending on results...

        self.zero_print = True

        self.min_bol_spread = 0

        self.tmp_debug_traces = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols
        ds.candle_stick = self.candle_stick

        ds.fdp_features = {"close": {},
                           # "bollinger_id1": {"indicator": "bollinger", "window_size": 100, "id": "1", "bol_std": 2.25, "output": ["lower_band", "higher_band", "ma_band"]},
                           "bollinger_id1": {"indicator": "bollinger", "window_size": 20, "id": "1", "bol_std": 2, "output": ["lower_band", "higher_band", "ma_band"]},
                           # "rsi": {"indicator": "rsi", "id": "1", "window_size": 14},
                           "long_ma": {"indicator": "sma", "id": "long_ma", "window_size": 100},
                           "postprocess1": {"indicator": "shift", "window_size": 1, "id": "1", "n": "1", "input": ['lower_band', "higher_band", "ma_band"]},
                           "postprocess2": {"indicator": "shift", "window_size": 1, "n": "1", "input": ["close"]}
                           }

        ds.features = self.get_feature_from_fdp_features(ds.fdp_features)
        ds.interval = self.strategy_interval
        print("startegy: ", self.get_info())
        print("strategy features: ", ds.features)

        return ds

    def get_info(self):
        return "StrategyBollingerTrendLong"

    def condition_for_opening_long_position(self, symbol):
        return (self.df_current_data['n1_close'][symbol] < self.df_current_data['n1_higher_band_1'][symbol]) \
               & (self.df_current_data['close'][symbol] > self.df_current_data['higher_band_1'][symbol]) \
               & (self.df_current_data["close"][symbol] > self.df_current_data["sma_long_ma"][symbol])

    def condition_for_closing_long_position(self, symbol):
        ma_band_epsilon = self.df_current_data['ma_band_1'][symbol]
        ma_band_epsilon = ma_band_epsilon + ma_band_epsilon * self.epsilon / 100

        return (self.df_current_data['close'][symbol] < ma_band_epsilon)




















