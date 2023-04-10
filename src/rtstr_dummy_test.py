import pandas as pd
import numpy as np
from . import trade
import json
import time
import csv
from datetime import datetime
import datetime

from . import rtdp, rtstr, utils, rtctrl

class StrategyDummyTest(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        self.zero_print = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols
        ds.features = { "close": 1,
                        "close_shift_5": 5,
                        "close_shift_10": 10,
                        "close_shift_15": 15
                      }
        return ds

    def get_info(self):
        return "StrategyDummyTest"

    def condition_for_opening_long_position(self, symbol):
        return False
        return self.df_current_data['close_shift_5'][symbol] <= self.df_current_data['close'][symbol]\
               and self.df_current_data['close_shift_10'][symbol] <= self.df_current_data['close_shift_5'][symbol]\
               and self.df_current_data['close_shift_15'][symbol] <= self.df_current_data['close_shift_10'][symbol]

    def condition_for_opening_short_position(self, symbol):
        return False
        return self.df_current_data['close_shift_5'][symbol] >= self.df_current_data['close'][symbol]\
               and self.df_current_data['close_shift_10'][symbol] >= self.df_current_data['close_shift_5'][symbol]\
               and self.df_current_data['close_shift_15'][symbol] >= self.df_current_data['close_shift_10'][symbol]

    def condition_for_closing_long_position(self, symbol):
        return True
        return self.df_current_data['close_shift_5'][symbol] >= self.df_current_data['close'][symbol] \
               and self.df_current_data['close_shift_10'][symbol] >= self.df_current_data['close_shift_5'][symbol] \
               and self.df_current_data['close_shift_15'][symbol] >= self.df_current_data['close_shift_10'][symbol]

    def condition_for_closing_short_position(self, symbol):
        return False
        return self.df_current_data['close_shift_5'][symbol] <= self.df_current_data['close'][symbol]\
               and self.df_current_data['close_shift_10'][symbol] <= self.df_current_data['close_shift_5'][symbol]\
               and self.df_current_data['close_shift_15'][symbol] <= self.df_current_data['close_shift_10'][symbol]
