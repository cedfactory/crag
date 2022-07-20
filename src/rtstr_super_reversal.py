import pandas as pd
import numpy as np
from . import trade
import json
import time
import csv
from datetime import datetime
import datetime

from . import rtdp, rtstr, utils, rtctrl

class StrategySuperReversal(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)

        self.zero_print = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        #ds.symbols = ds.symbols[:2]
        ds.features = { "low" : None,
                        "high" : None,
                        "ema_short" : {"feature": "ema", "period": 5},
                        "ema_long" : {"feature": "ema", "period": 400},
                        "super_trend_direction" : {"feature": "super_trend"}
                        }

        return ds

    def get_info(self):
        return "superreversal", self.str_sl, self.str_tp

    def set_current_data(self, current_data):
        self.df_current_data = current_data

    def get_df_buying_symbols(self):
        data = {'symbol':[], 'size':[], 'percent':[]}
        for symbol in self.df_current_data.index.to_list():
            if (
                    self.df_current_data['ema_short'][symbol] >= self.df_current_data['ema_long'][symbol]
                    and self.df_current_data['super_trend_direction'][symbol] == True
                    and self.df_current_data['ema_short'][symbol] > self.df_current_data['low'][symbol]
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
            if (
                    (self.df_current_data['ema_short'][symbol] <= self.df_current_data['ema_long'][symbol]
                     or self.df_current_data['super_trend_direction'][symbol] == False)
                    and self.df_current_data['ema_short'][symbol] < self.df_current_data['high'][symbol]
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

    def update(self, current_datetime, current_trades, broker_cash, prices_symbols, record_info):
        self.rtctrl.update_rtctrl(current_datetime, current_trades, broker_cash, prices_symbols)
        self.rtctrl.display_summary_info(record_info)
