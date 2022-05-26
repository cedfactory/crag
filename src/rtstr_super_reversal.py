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

        self.rtctrl = rtctrl.rtctrl()

        self.SL = -0.2           # Stop Loss %
        self.TP = 0.2            # Take Profit %
        self.SPLIT = 1          # Asset Split
        self.MAX_POSITION = 100   # Asset Overall Percent Size

    def get_data_description(self):
        ds = rtdp.DataDescription()
        #ds.symbols = ds.symbols[:2]
        ds.features = { "low" : None,
                        "high" : None,
                        "ema_short" : {"feature": "ema", "period": 5},
                        "ema_long" : {"feature": "ema", "period": 400},
                        "super_trend_direction" : {"feature": "super_trend"},
                        "open_long_limit": None,
                        "close_long_limit": None
                        }

        return ds

    def set_current_data(self, current_data):
        self.df_current_data = current_data

    def get_df_buying_symbols(self):
        df_result = pd.DataFrame(columns = ['symbol', 'size'])
        for symbol in self.df_current_data.index.to_list():
            if (self.df_current_data['ema_short'][symbol] >= self.df_current_data['ema_long'][symbol]
                and self.df_current_data['super_trend_direction'][symbol] == True
                and self.df_current_data['ema_short'][symbol] > self.df_current_data['low'][symbol]):
                # DEBUG For test purposes.....
                # pass
                #
            # if(self.df_current_data['open_long_limit'][symbol] == True):
                size = self.get_symbol_buying_size(symbol)
                df_row = pd.DataFrame(data={"symbol":[symbol], "size":[size]})
                df_result = pd.concat((df_result, df_row), axis = 0)

        # UGGLY CODING to be replaced and included in first main selection test above...
        df_rtctrl = self.rtctrl.df_rtctrl.copy()
        df_result.reset_index(inplace=True, drop=True)
        if len(df_rtctrl) > 0:
            df_rtctrl.set_index('symbol', inplace=True)
            lst_symbols_to_buy = df_result.symbol.to_list()
            for symbol in lst_symbols_to_buy:
                try:
                    if df_rtctrl["wallet_%"][symbol] >= self.MAX_POSITION:
                        # Symbol to be removed - Over the % limits
                        df_result.drop(df_result[df_result['symbol'] == symbol].index, inplace=True)
                except:
                    # Stay in list
                    pass
        df_result.reset_index(inplace=True, drop=True)
        return df_result

    def get_df_selling_symbols(self, lst_symbols):
        df_result = pd.DataFrame(columns = ['symbol', 'stimulus'])
        for symbol in self.df_current_data.index.to_list():
            if ((self.df_current_data['ema_short'][symbol] <= self.df_current_data['ema_long'][symbol]
                or self.df_current_data['super_trend_direction'][symbol] == False)
                and self.df_current_data['ema_short'][symbol] < self.df_current_data['high'][symbol]):
                # DEBUG For test purposes.....
                #pass
                #
            #if (self.df_current_data['close_long_limit'][symbol] == True):
                df_row = pd.DataFrame(data={"symbol":[symbol], "stimulus":["SELL"]})
                df_result = pd.concat((df_result, df_row), axis = 0)

        return df_result

    # get_df_selling_symbols and get_df_forced_exit_selling_symbols
    # could be merged in one...
    def get_df_forced_exit_selling_symbols(self, lst_symbols):
        df_result = pd.DataFrame(columns = ['symbol', 'stimulus'])
        for symbol in self.df_current_data.index.to_list():
            df_row = pd.DataFrame(data={"symbol":[symbol], "stimulus":["SELL"]})
            df_result = pd.concat((df_result, df_row), axis = 0)

        return df_result

    def update(self, current_datetime, current_trades, broker_cash, prices_symbols):
        self.rtctrl.update_rtctrl(current_datetime, current_trades, broker_cash, prices_symbols)
        self.rtctrl.display_summary_info()

    def get_symbol_buying_size(self, symbol):
        if self.rtctrl.prices_symbols[symbol] < 0: # first init at -1
            return 0
        # size = 1 / self.rtctrl.prices_symbols[symbol] # by default : buy for 1 eur
        initial_cash = self.rtctrl.init_cash_value
        available_cash = self.rtctrl.wallet_cash
        cash_to_buy = available_cash / self.SPLIT

        size = cash_to_buy / self.rtctrl.prices_symbols[symbol]

        return size

    def get_portfolio_value(self):
        return self.rtctrl.df_rtctrl['portfolio_value'].sum()