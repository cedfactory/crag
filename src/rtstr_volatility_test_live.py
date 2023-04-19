import pandas as pd
import numpy as np
from . import trade
import json
import time
import csv
from datetime import datetime
import datetime

from . import rtdp, rtstr, utils, rtctrl

class StrategyVolatilityTest(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        self.zero_print = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols
        ds.fdp_features = { "close": {},
                            "postprocess1": {"indicator": "shift", "window_size": 1, "n": "5", "input": ["close"]},
                            "postprocess2": {"indicator": "shift", "window_size": 1, "n": "10", "input": ["close"]},
                            "postprocess3": {"indicator": "shift", "window_size": 1, "n": "15", "input": ["close"]},
                            "postprocess4": {"indicator": "shift", "window_size": 1, "n": "20", "input": ["close"]}
                            }

        ds.features = self.get_feature_from_fdp_features(ds.fdp_features)
        print("startegy: ", self.get_info())
        print("strategy features: ", ds.features)

        return ds

    def get_info(self):
        return "StrategyVolatilityTest"

    def condition_for_opening_long_position(self, symbol):
        return self.df_current_data['n5_close'][symbol] <= self.df_current_data['close'][symbol]\
               and self.df_current_data['n10_close'][symbol] <= self.df_current_data['n5_close'][symbol]\
               and self.df_current_data['n15_close'][symbol] <= self.df_current_data['n10_close'][symbol]\
               and self.df_current_data['n15_close'][symbol] <= self.df_current_data['n20_close'][symbol]

    def condition_for_opening_short_position(self, symbol):
        return self.df_current_data['n5_close'][symbol] >= self.df_current_data['close'][symbol]\
               and self.df_current_data['n10_close'][symbol] >= self.df_current_data['n5_close'][symbol]\
               and self.df_current_data['n15_close'][symbol] >= self.df_current_data['n10_close'][symbol] \
               and self.df_current_data['n15_close'][symbol] >= self.df_current_data['n20_close'][symbol]

    def condition_for_closing_long_position(self, symbol):
        return False

    def condition_for_closing_short_position(self, symbol):
        return False
