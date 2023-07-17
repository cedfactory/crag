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

"""
===========================
sl:  0  stochOverBought:  0.95  stochOverSold:  0.05  offset:  2
nb pair:  34
final_wallet mean:  39149.95676470589
final_wallet max:  911640.2
vs_hold_pct mean:  18.317647058823532
vs_hold_pct max:  239.58
global_win_rate mean:  0.695
global_win_rate max:  0.83
total_trades mean:  84.1470588235294
total_trades max:  187.0
list pairs:  ['XRP', 'BCH', 'DOGE', 'AAVE', 'ATOM', 'SUSHI', 'SHIB', 'EGLD', 'AR', 'PEOPLE', 'IOTA', 'ZIL', 'APE', 'STORJ', '1INCH', 'LUNA2', 'FLOW', 'REEF', 'LUNC', 'MINA', 'ASTR', 'ANKR', 'ACH', 'HBAR', 'CKB', 'RDNT', 'HFT', 'ZEC', 'FLOKI', 'SPELL', 'SUI', 'STMX', 'UMA', 'SLP']
===========================
"""

class StrategyEnvelopeStochRSI(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        # self.envelope = EnvelopeLevelStatus(self.lst_symbols)
        # self.nb_envelope = 3

        # self.stochOverBought = 0.95
        # self.stochOverSold = 0.05

        # self.stochOverBought = 0.8
        # self.stochOverSold = 0.2

        self.stochOverBought = 0.9
        self.stochOverSold = 0.1

        self.zero_print = True

        self.tmp_debug_traces = True
        self.strategy_info_printed = False

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols
        ds.interval = self.strategy_interval
        ds.candle_stick = self.candle_stick

        ds.fdp_features = {"close": {},
                           "ao": {"indicator": "ao", "ao_window_1": 6, "ao_window_2": 22, "window_size": 22},
                           "stoch_rsi": {"indicator": "stoch_rsi", "window_size": 30, "stoch_rsi_window_size":14},
                           "envelope": {"indicator": "envelope", "window_size": 10,
                                        "ma": "sma", "ma_window_size": 5,
                                        # "ma_offset_1": "2", "ma_offset_2": "5", "ma_offset_3": "7",
                                        # "ma_offset_1": "3", "ma_offset_2": "5", "ma_offset_3": "7",
                                        "ma_offset_1": "2", "ma_offset_2": "3", "ma_offset_3": "5",
                                        "output": ["ma_base",
                                                   "envelope_long_1", "envelope_long_2", "envelope_long_3",
                                                   "envelope_short_1", "envelope_short_2", "envelope_short_3"]
                                        }
                           }

        ds.features = self.get_feature_from_fdp_features(ds.fdp_features)

        if not self.strategy_info_printed:
            print("startegy: ", self.get_info())
            print("strategy features: ", ds.features)
            self.strategy_info_printed = True

        # ['close', 'envelope', 'ma_base', 'envelope_long_1', 'envelope_long_2', 'envelope_long_3', 'envelope_short_1', 'envelope_short_2', 'envelope_short_3']
        return ds

    def get_info(self):
        return "StrategyEnvelopeStochRSI"

    def authorize_multi_transaction_for_symbols(self):
        # Multi buy is authorized for this strategy
        return False

    def condition_for_opening_long_position(self, symbol):
        if self.df_current_data['close'][symbol] > self.df_current_data['envelope_long_1'][symbol]:
            return False
        elif self.df_current_data['close'][symbol] < self.df_current_data['envelope_long_1'][symbol]\
                and self.df_current_data['stoch_rsi'][symbol] < self.stochOverSold:
            return True

    def condition_for_opening_short_position(self, symbol):
        if self.df_current_data['close'][symbol] < self.df_current_data['envelope_short_1'][symbol]:
            return False
        elif self.df_current_data['close'][symbol] > self.df_current_data['envelope_short_1'][symbol] \
                and self.df_current_data['stoch_rsi'][symbol] > self.stochOverBought:
            return True

    def condition_for_closing_long_position(self, symbol):
        if self.df_current_data['close'][symbol] >= self.df_current_data['ma_base'][symbol]:
            return True
        else:
            return False

    def condition_for_closing_short_position(self, symbol):
        if self.df_current_data['close'][symbol] <= self.df_current_data['ma_base'][symbol]:
            return True
        else:
            return False

    def sort_list_symbols(self, lst_symbols):
        print("symbol list: ", lst_symbols)
        df = pd.DataFrame(index=lst_symbols, columns=['ao'])
        for symbol in lst_symbols:
            df.at[symbol, 'ao'] = self.df_current_data['ao'][symbol]
        df.sort_values(by=['ao'], inplace=True, ascending=False)
        lst_symbols = df.index.to_list()
        print("sorted symbols with AO: ", lst_symbols)
        return lst_symbols