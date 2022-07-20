import pandas as pd
import numpy as np
from . import trade
import json
import time
import csv
from datetime import datetime
import datetime

from . import rtdp, rtstr, utils, rtctrl

class StrategyTrix(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)

        self.zero_print = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        #ds.symbols = ds.symbols[:2]
        ds.features = { "TRIX_HISTO" : {"feature": "trix", "period": 21},
                        "STOCH_RSI": {"feature": "stoch_rsi", "period": 14}
                        }
        return ds

    def get_info(self):
        return "trix", self.str_sl, self.str_tp

    def set_current_data(self, current_data):
        self.df_current_data = current_data

    def get_df_buying_symbols(self):
        data = {'symbol':[], 'size':[], 'percent':[]}
        for symbol in self.df_current_data.index.to_list():
            if(
                    self.df_current_data['TRIX_HISTO'][symbol] > 0 and self.df_current_data['STOCH_RSI'][symbol] < 0.8
            ):
                size, percent = self.get_symbol_buying_size(symbol)
                data['symbol'].append(symbol)
                data['size'].append(size)
                data['percent'].append(percent)

        df_result = pd.DataFrame(data)
        df_result.reset_index(inplace=True, drop=True)
        
        df_result = self.get_df_selling_symbols_common(df_result)
        
        return df_result

    def get_df_selling_symbols(self, lst_symbols, df_sl_tp):
        data = {'symbol':[], 'stimulus':[]}
        for symbol in self.df_current_data.index.to_list():
            if(
                    self.df_current_data['TRIX_HISTO'][symbol] < 0 and self.df_current_data['STOCH_RSI'][symbol] > 0.2
            ) or (
                    (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] > self.TP)
                    or (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] < self.SL)
            ):
                data["symbol"].append(symbol)
                data["stimulus"].append("SELL")

                if not self.zero_print:
                    if(isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] > self.TP):
                        print('TAKE PROFIT: ', symbol, ": ", df_sl_tp['roi_sl_tp'][symbol])
                    if(isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] < self.SL):
                        print('STOP LOST: ', symbol, ": ", df_sl_tp['roi_sl_tp'][symbol])

        df_result = pd.DataFrame(data)
        return df_result
