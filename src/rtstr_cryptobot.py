import pandas as pd
import numpy as np
from . import trade
import json
import time
import csv
from datetime import datetime
import datetime

from . import rtdp, rtstr, utils, rtctrl

class StrategyCryptobot(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)

        self.SL = 0             # Stop Loss %
        self.TP = 0             # Take Profit %
        if params:
            self.SL = params.get("sl", self.SL)
            self.TP = params.get("tp", self.TP)
        self.str_sl = "sl" + str(self.SL)
        self.str_tp = "tp" + str(self.TP)

        if self.SL == 0:     # SL == 0 => mean no SL
            self.SL = -1000
        if self.TP == 0:     # TP == 0 => mean no TP
            self.TP = 1000

        self.SPLIT = 5           # Asset Split %
        self.MAX_POSITION = 5    # Asset Overall Percent Size
        self.match_full_position = True

        self.zero_print = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        #ds.symbols = ds.symbols[:2]
        ds.features = { "ema12gtema26co": {"feature": "ema12gtema26co", "period": 26},  # used for buying signal
                        "macdgtsignal":   {"feature": "macdgtsignal", "period": 26},  # used for buying signal
                        "goldencross":    {"feature": "goldencross", "period": 14},  # used for buying signal
                        "obv_pc":         {"feature": "obv_pc", "period": 14},  # used for buying signal
                        "eri_buy":        {"feature": "eri_buy", "period": 14}, # used for buying signal
                        "ema12ltema26co": {"feature": "ema12ltema26co", "period": 26}, # used for selling signal
                        "macdltsignal":   {"feature": "macdltsignal", "period": 26}    # used for selling signal
                        }
        return ds

    def get_info(self):
        return "cryptobot", self.str_sl, self.str_tp

    def set_current_data(self, current_data):
        self.df_current_data = current_data

    def get_df_buying_symbols(self):
        data = {'symbol':[], 'size':[], 'percent':[]}
        for symbol in self.df_current_data.index.to_list():
            if((self.df_current_data["ema12gtema26co"][symbol])
                    and (self.df_current_data["macdgtsignal"][symbol])
                    and (self.df_current_data["goldencross"][symbol])
                    and (self.df_current_data["obv_pc"][symbol] > -5)
                    and (self.df_current_data["eri_buy"][symbol])):

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
                    (self.df_current_data["ema12ltema26co"][symbol])
                    and (self.df_current_data["macdltsignal"][symbol])
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
