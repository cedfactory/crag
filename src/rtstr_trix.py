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

    def update(self, current_datetime, current_trades, broker_cash, prices_symbols, record_info):
        self.rtctrl.update_rtctrl(current_datetime, current_trades, broker_cash, prices_symbols)
        self.rtctrl.display_summary_info(record_info)

    def get_symbol_buying_size(self, symbol):
        if not symbol in self.rtctrl.prices_symbols or self.rtctrl.prices_symbols[symbol] < 0: # first init at -1
            return 0, 0

        available_cash = self.rtctrl.wallet_cash
        if available_cash == 0:
            return 0, 0

        wallet_value = available_cash

        cash_to_buy = wallet_value * self.SPLIT / 100

        if cash_to_buy > available_cash:
            cash_to_buy = available_cash

        size = cash_to_buy / self.rtctrl.prices_symbols[symbol]

        percent =  cash_to_buy * 100 / wallet_value

        return size, percent

    def get_portfolio_value(self):
        return self.rtctrl.df_rtctrl['portfolio_value'].sum()
