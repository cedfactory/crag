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

        self.epsilon = 0.1 # CEDE specific for bollinger - to be included to .xml depending on results...

        self.zero_print = True

        self.min_bol_spread = 0

        self.tmp_debug_traces = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols

        ds.fdp_features = {"close": {},
                           # "bollinger_id1": {"indicator": "bollinger", "window_size": 100, "id": "1", "bol_std": 2.25, "output": ["lower_band", "higher_band", "ma_band"]},
                           "bollinger_id1": {"indicator": "bollinger", "window_size": 20, "id": "1", "bol_std": 2, "output": ["lower_band", "higher_band", "ma_band"]},
                           "rsi": {"indicator": "rsi", "id": "1", "window_size": 14},
                           "atr": {"indicator": "atr", "id": "1", "window_size": 14},
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
        return "StrategyBollingerTrend"

    def condition_for_opening_long_position(self, symbol):
        msg = ""
        for col in ['close', 'bollinger_1', 'lower_band_1', 'higher_band_1', 'ma_band_1', 'rsi_1', 'atr_1', 'sma_long_ma', 'n1_lower_band_1', 'n1_higher_band_1', 'n1_ma_band_1', 'n1_close']:
            msg += col + " " + str(self.df_current_data[col][symbol]) + " + "
        print(msg)

        if self.tmp_debug_traces:
            if (self.df_current_data['n1_close'][symbol] < self.df_current_data['n1_higher_band_1'][symbol])\
               & (self.df_current_data['close'][symbol] > self.df_current_data['higher_band_1'][symbol])\
               & (self.df_current_data["close"][symbol] > self.df_current_data["sma_long_ma"][symbol]):
                print("OPENING LONG POSITION: TRUE - ", symbol)
            else:
                print("OPENING LONG POSITION: FALSE - ", symbol)

            print("condition long - ",
                  "symbol: ", symbol,
                  " close: ", self.df_current_data['close'][symbol],
                  " n1_close: ", self.df_current_data['n1_close'][symbol],
                  " higher_band_1: ", self.df_current_data['higher_band_1'][symbol],
                  " n1_higher_band_1: ", self.df_current_data['n1_higher_band_1'][symbol],
                  " lower_band_1: ", self.df_current_data['lower_band_1'][symbol],
                  " n1_lower_band_1: ", self.df_current_data['n1_lower_band_1'][symbol],
                  # " rsi_1: ", self.df_current_data['rsi_1'][symbol]
                  )

            print( "n1_close < n1_higher_band_1 : ", (self.df_current_data['n1_close'][symbol] < self.df_current_data['n1_higher_band_1'][symbol]),
                   " - close > higher_band_1 : ", (self.df_current_data['close'][symbol] > self.df_current_data['higher_band_1'][symbol]),
                   " - close > sma_long_ma : ", (self.df_current_data["close"][symbol] > self.df_current_data["sma_long_ma"][symbol])
                   # " - (abs(n1_higher_band_1 - n1_lower_band_1 / n1_lower_band_1 > min_bol_spread) : ", (abs(self.df_current_data['n1_higher_band_1'][symbol] - self.df_current_data['n1_lower_band_1'][symbol]) / self.df_current_data['n1_lower_band_1'][symbol] > self.min_bol_spread),
                   # " - rsi_1 > 52)", (self.df_current_data["rsi_1"][symbol] > 52)
                   )
            # else:
            #    print("OPENING LONG POSITION: FALSE - ", symbol)

        return (self.df_current_data['n1_close'][symbol] < self.df_current_data['n1_higher_band_1'][symbol])\
               & (self.df_current_data['close'][symbol] > self.df_current_data['higher_band_1'][symbol])\
               & (self.df_current_data["close"][symbol] > self.df_current_data["sma_long_ma"][symbol])
               # & (abs(self.df_current_data['n1_higher_band_1'][symbol] - self.df_current_data['n1_lower_band_1'][symbol]) / self.df_current_data['n1_lower_band_1'][symbol] > self.min_bol_spread) \
               # & (self.df_current_data['rsi_1'][symbol] > 52)

    def condition_for_opening_short_position(self, symbol):
        if self.tmp_debug_traces:
            if (self.df_current_data['n1_close'][symbol] > self.df_current_data['n1_lower_band_1'][symbol])\
                    & (self.df_current_data['close'][symbol] < self.df_current_data['lower_band_1'][symbol])\
                    & (self.df_current_data["close"][symbol] < self.df_current_data["sma_long_ma"][symbol]):
                print("OPENING SHORT POSITION: TRUE - ", symbol)
            else:
                print("OPENING SHORT POSITION: FALSE - ", symbol)
            print("n1_close > n1_lower_band_1 : ",
                  (self.df_current_data['n1_close'][symbol] > self.df_current_data['n1_lower_band_1'][symbol]),
                  " - close < lower_band_1 : ",
                  (self.df_current_data['close'][symbol] < self.df_current_data['lower_band_1'][symbol]),
                  " - close < sma_long_ma : ",
                  (self.df_current_data["close"][symbol] < self.df_current_data["sma_long_ma"][symbol])
                  # " - (abs(n1_higher_band_1 - n1_lower_band_1 / n1_lower_band_1 > min_bol_spread) : ", (abs(self.df_current_data['n1_higher_band_1'][symbol] - self.df_current_data['n1_lower_band_1'][symbol]) / self.df_current_data['n1_lower_band_1'][symbol] > self.min_bol_spread),
                  # " - rsi_1 > 52)", (self.df_current_data["rsi_1"][symbol] < 48)
                  )

            print("condition short - ",
                  "symbol: ", symbol,
                  " close: ", self.df_current_data['close'][symbol],
                  " n1_close: ", self.df_current_data['n1_close'][symbol],
                  " higher_band_1: ", self.df_current_data['higher_band_1'][symbol],
                  " n1_higher_band_1: ", self.df_current_data['n1_higher_band_1'][symbol],
                  " lower_band_1: ", self.df_current_data['lower_band_1'][symbol],
                  " n1_lower_band_1: ", self.df_current_data['n1_lower_band_1'][symbol],
                  # " rsi_1: ", self.df_current_data['rsi_1'][symbol]
                  )
            # else:
            #     print("OPENING SHORT POSITION: FALSE - ", symbol)

        return (self.df_current_data['n1_close'][symbol] > self.df_current_data['n1_lower_band_1'][symbol])\
               & (self.df_current_data['close'][symbol] < self.df_current_data['lower_band_1'][symbol])\
               & (self.df_current_data["close"][symbol] < self.df_current_data["sma_long_ma"][symbol])
               # & (abs(self.df_current_data['n1_higher_band_1'][symbol] - self.df_current_data['n1_lower_band_1'][symbol]) / self.df_current_data['n1_lower_band_1'][symbol] > self.min_bol_spread) \
               # & (self.df_current_data["rsi_1"][symbol] < 48)

    def condition_for_closing_long_position(self, symbol):
        ma_band_epsilon = self.df_current_data['ma_band_1'][symbol]
        ma_band_epsilon = ma_band_epsilon + ma_band_epsilon * self.epsilon / 100
        if self.tmp_debug_traces:
            print("CLOSING LONG POSITION: ", (self.df_current_data['close'][symbol] < self.df_current_data['ma_band_1'][symbol]), " - ", symbol)
            print("symbol: ", symbol,
                  " close: ", self.df_current_data['close'][symbol],
                  " ma_band_1: ", self.df_current_data['ma_band_1'][symbol],
                  " ma_band_1 + epsilon: ", ma_band_epsilon
                  )
        return (self.df_current_data['close'][symbol] < ma_band_epsilon)

    def condition_for_closing_short_position(self, symbol):
        ma_band_epsilon = self.df_current_data['ma_band_1'][symbol]
        ma_band_epsilon = ma_band_epsilon - ma_band_epsilon * self.epsilon / 100
        if self.tmp_debug_traces:
            print("CLOSING SHORT POSITION: ", (self.df_current_data['close'][symbol] > self.df_current_data['ma_band_1'][symbol]), " - ", symbol)
            print("symbol: ", symbol,
                  " close: ", self.df_current_data['close'][symbol],
                  " ma_band_1: ", self.df_current_data['ma_band_1'][symbol],
                  " ma_band_1 - epsilon: ", ma_band_epsilon
                  )
        return (self.df_current_data['close'][symbol] > ma_band_epsilon)

    def sort_list_symbols(self, lst_symbols):
        print("symbol list: ", lst_symbols)
        df = pd.DataFrame(index=lst_symbols, columns=['atr'])
        for symbol in lst_symbols:
            df.at[symbol, 'atr'] = self.df_current_data['atr_1'][symbol]
        df.sort_values(by=['atr'], inplace=True, ascending=False)
        lst_symbols = df.index.to_list()
        print("sorted symbols with atr: ", lst_symbols)
        return lst_symbols




















