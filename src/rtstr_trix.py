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
        df_result = pd.DataFrame(columns = ['symbol', 'size', 'percent'])
        for symbol in self.df_current_data.index.to_list():
            if(
                    self.df_current_data['TRIX_HISTO'][symbol] > 0 and self.df_current_data['STOCH_RSI'][symbol] < 0.8
            ):
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
                    self.df_current_data['TRIX_HISTO'][symbol] < 0 and self.df_current_data['STOCH_RSI'][symbol] > 0.2
            ) or (
                    (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] > self.TP)
                    or (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] < self.SL)
            ):
                df_row = pd.DataFrame(data={"symbol":[symbol], "stimulus":["SELL"]})
                df_result = pd.concat((df_result, df_row), axis = 0)

                if not self.zero_print:
                    if(isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] > self.TP):
                        print('TAKE PROFIT: ', symbol, ": ", df_sl_tp['roi_sl_tp'][symbol])
                    if(isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] < self.SL):
                        print('STOP LOST: ', symbol, ": ", df_sl_tp['roi_sl_tp'][symbol])

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