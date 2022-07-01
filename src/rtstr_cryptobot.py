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
        df_result = pd.DataFrame(columns = ['symbol', 'size', 'percent'])
        for symbol in self.df_current_data.index.to_list():
            if((self.df_current_data["ema12gtema26co"][symbol] is True)
                    and (self.df_current_data["macdgtsignal"][symbol] is True)
                    and (self.df_current_data["goldencross"][symbol] is True)
                    and (self.df_current_data["obv_pc"][symbol] > -5)
                    and (self.df_current_data["eri_buy"][symbol] is True)):

                size, percent = self.get_symbol_buying_size(symbol)
                df_row = pd.DataFrame(data={"symbol":[symbol], "size":[size], 'percent':[percent]})
                df_result = pd.concat((df_result, df_row), axis = 0)

        # UGGLY CODING to be replaced and included in first main selection test above...
        df_rtctrl = self.rtctrl.df_rtctrl.copy()
        df_result.reset_index(inplace=True, drop=True)
        if len(df_rtctrl) > 0:
            df_rtctrl.set_index('symbol', inplace=True)
            lst_symbols_to_buy = df_result.symbol.to_list()
            for symbol in lst_symbols_to_buy:
                try:
                    df_result_percent = df_result.copy()
                    df_result_percent.set_index('symbol', inplace=True, drop=True)
                    if df_rtctrl["wallet_%"][symbol] >= self.MAX_POSITION:
                        # Symbol to be removed - Over the % limits
                        df_result.drop(df_result[df_result['symbol'] == symbol].index, inplace=True)
                    elif (df_rtctrl["wallet_%"][symbol] + df_result_percent['percent'][symbol]) >= self.MAX_POSITION:
                        if self.match_full_position == True:
                            diff_percent = self.MAX_POSITION - df_rtctrl["wallet_%"][symbol]
                            cash_needed = diff_percent * df_rtctrl["wallet_value"][symbol] / 100
                            size_to_match = cash_needed / self.rtctrl.prices_symbols[symbol]
                            df_result['size'] = np.where(df_result['symbol'] == symbol, size_to_match, df_result['size'])
                            df_result['percent'] = np.where(df_result['symbol'] == symbol, diff_percent, df_result['percent'])
                        else:
                            df_result.drop(df_result[df_result['symbol'] == symbol].index, inplace=True)
                except:
                    # Stay in list
                    pass
        df_result.reset_index(inplace=True, drop=True)
        return df_result

    def get_df_selling_symbols(self, lst_symbols, df_sl_tp):
        df_result = pd.DataFrame(columns = ['symbol', 'stimulus'])
        for symbol in self.df_current_data.index.to_list():
            if(
                    (self.df_current_data["ema12ltema26co"][symbol] is True)
                    and (self.df_current_data["macdltsignal"][symbol] is True)
            ) or (
                    (df_sl_tp['roi_sl_tp'][symbol] > self.TP)
                    or (df_sl_tp['roi_sl_tp'][symbol] < self.SL)
            ):
                df_row = pd.DataFrame(data={"symbol":[symbol], "stimulus":["SELL"]})
                df_result = pd.concat((df_result, df_row), axis = 0)

                if(df_sl_tp['roi_sl_tp'][symbol] > self.TP):
                    print('=========================== TAKE PROFIT ==========================')
                    print('=========================== ', symbol,' ==========================')
                    print('=========================== ', df_sl_tp['roi_sl_tp'][symbol], ' ==========================')
                if(df_sl_tp['roi_sl_tp'][symbol] < self.SL):
                    print('=========================== STOP LOST ==========================')
                    print('=========================== ', symbol,' ==========================')
                    print('=========================== ', df_sl_tp['roi_sl_tp'][symbol], ' ==========================')

        return df_result

    # get_df_selling_symbols and get_df_forced_exit_selling_symbols
    # could be merged in one...
    def get_df_forced_exit_selling_symbols(self, lst_symbols):
        df_result = pd.DataFrame(columns = ['symbol', 'stimulus'])
        if hasattr(self, 'df_current_data'):
            for symbol in self.df_current_data.index.to_list():
                df_row = pd.DataFrame(data={"symbol":[symbol], "stimulus":["SELL"]})
                df_result = pd.concat((df_result, df_row), axis = 0)

        return df_result

    def update(self, current_datetime, current_trades, broker_cash, prices_symbols, record_info):
        self.rtctrl.update_rtctrl(current_datetime, current_trades, broker_cash, prices_symbols)
        self.rtctrl.display_summary_info(record_info)

    def get_symbol_buying_size(self, symbol):
        if self.rtctrl.prices_symbols[symbol] < 0: # first init at -1
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