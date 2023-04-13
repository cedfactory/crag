import pandas as pd
import numpy as np
from . import trade
import json
import time
import csv
from datetime import datetime
import datetime

from . import rtdp, rtstr, utils, rtctrl

# ref:
# https://www.dutchalgotrading.com/strategies/the-slope-is-dope-trading-bot-strategy-is-it-profitable-or-not/

class StrategySlopeIsDope(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        self.zero_print = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols
        ds.features = { "close": None,
                        "slope_is_dope": None,
                        "n11_close": 11,
                        "rsi": 7,
                        "marketMA": 200,
                        "fastMA": 21,
                        "slowMA": 50,
                        "entryMA": 3,
                        "n11_entryMA": 11,
                        "fast_slope": None,
                        "slow_slope": None,
                        "last_lowest": None
                        }
        return ds

    def get_info(self):
        return "StrategyDummyTest"

    def condition_for_opening_long_position(self, symbol):
        # Only enter when market is bullish (this is a choice)
        # Only trade when the fast slope is above 0
        # Only trade when the slow slope is above 0
        # Only buy when the close price is higher than the 3day average of ten periods ago (this is a choice)
        #       and self.df_current_data['close'][symbol] > self.df_current_data['n11_entryMA'][symbol] \
        # Or only buy when the close price is higher than the close price of 3 days ago (this is a choice)
        # Only enter trades when the RSI is higher than 55
        # Only trade when the fast MA is above the slow MA
        # Or trade when the fast MA crosses above the slow MA (This is a choice)
        #       (qtpylib.crossed_above(dataframe['fastMA'], dataframe['slowMA'])))
        condition = 0
        if self.df_current_data['close'][symbol] > self.df_current_data['marketMA'][symbol]:
            condition += 1
        if self.df_current_data['fast_slope'][symbol] > 0:
            condition += 1
        if self.df_current_data['slow_slope'][symbol] > 0:
            condition += 1
        if self.df_current_data['close'][symbol] > self.df_current_data['n11_close'][symbol]:
            condition += 1
        if self.df_current_data['rsi'][symbol] > 55:
            condition += 1
        if self.df_current_data['fastMA'][symbol] < self.df_current_data['slowMA'][symbol]:
            condition += 1
        print("condition: ", condition)

        return self.df_current_data['close'][symbol] > self.df_current_data['marketMA'][symbol]\
               and self.df_current_data['fast_slope'][symbol] > 0\
               and self.df_current_data['slow_slope'][symbol] > 0\
               and self.df_current_data['close'][symbol] > self.df_current_data['n11_close'][symbol]\
               and self.df_current_data['rsi'][symbol] > 55\
               and self.df_current_data['fastMA'][symbol] > self.df_current_data['slowMA'][symbol]

    def condition_for_opening_short_position(self, symbol):
        return False
        return self.df_current_data['close_shift_5'][symbol] >= self.df_current_data['close'][symbol]\
               and self.df_current_data['close_shift_10'][symbol] >= self.df_current_data['close_shift_5'][symbol]\
               and self.df_current_data['close_shift_15'][symbol] >= self.df_current_data['close_shift_10'][symbol]

    def condition_for_closing_long_position(self, symbol):
        # Close or do not trade when fastMA is below slowMA
        # Or close position when the close price gets below the last lowest candle price configured
        # (AKA candle based (Trailing) stoploss)
        #       and self.df_current_data['close'][symbol] < self.df_current_data['fastMA'][symbol]
        return self.df_current_data['fastMA'][symbol] < self.df_current_data['slowMA'][symbol] \
               or self.df_current_data['close'][symbol] < self.df_current_data['last_lowest'][symbol]

    def condition_for_closing_short_position(self, symbol):
        return False
        return self.df_current_data['close_shift_5'][symbol] <= self.df_current_data['close'][symbol]\
               and self.df_current_data['close_shift_10'][symbol] <= self.df_current_data['close_shift_5'][symbol]\
               and self.df_current_data['close_shift_15'][symbol] <= self.df_current_data['close_shift_10'][symbol]
