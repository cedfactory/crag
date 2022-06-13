import pandas as pd
import numpy as np
from . import trade
import json
import time
import csv
from datetime import datetime
import datetime

from . import rtdp, rtstr, utils, rtctrl

class StrategyVMC(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)

        self.positive_Threshold = 0
        self.chop_Threshold = 50
        self.AO_Threshold = 0
        self.stochOverSold = 0.2
        self.willOverBought = -10

        self.SL = -0.2           # Stop Loss %
        self.TP = 0.2            # Take Profit %
        self.SPLIT = 5           # Asset Split %
        self.MAX_POSITION = 5    # Asset Overall Percent Size
        self.match_full_position = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        #ds.symbols = ds.symbols[:2]
        ds.features = { "low" : None,
                        "high" : None,
                        "close": None,
                        "AO" : {"feature": "AO", "period": 22},
                        "previous_AO": {"feature": "previous_AO", "period": 22},
                        "STOCH_RSI": {"feature": "STOCH_RSI", "period": 14},
                        "ema_short_vmc": {"feature": "ema_short_vmc", "period": 100},
                        "ema_long_vmc": {"feature": "ema_long_vmc", "period": 100},
                        "MONEY_FLOW": {"feature": "MONEY_FLOW", "period": 100},
                        "VMC_WAVE1": {"feature": "VMC_WAVE1", "period": 100},
                        "VMC_WAVE2": {"feature": "VMC_WAVE2", "period": 100},
                        "n1_VMC_WAVE1": {"feature": "n1_VMC_WAVE1", "period": 100},
                        "n1_VMC_WAVE2": {"feature": "n1_VMC_WAVE2", "period": 100},
                        "CHOP": {"feature": "CHOP", "period": 100},
                        "WILLR" : {"feature": "WILLR", "period": 14}
                        }
        return ds

    def set_current_data(self, current_data):
        self.df_current_data = current_data

    def get_df_buying_symbols(self):
        df_result = pd.DataFrame(columns = ['symbol', 'size', 'percent'])
        for symbol in self.df_current_data.index.to_list():
            if (
                    (self.df_current_data['ema_short_vmc'][symbol] > self.df_current_data['ema_long_vmc'][symbol])
                    & (self.df_current_data['close'][symbol] > self.df_current_data['ema_short_vmc'][symbol])
                    & (self.df_current_data['MONEY_FLOW'][symbol] > self.positive_Threshold)
                    & (self.df_current_data['VMC_WAVE1'][symbol] < self.positive_Threshold)
                    & (self.df_current_data['VMC_WAVE2'][symbol] < self.positive_Threshold)
                    & (self.df_current_data['VMC_WAVE1'][symbol] > self.df_current_data['VMC_WAVE2'][symbol])
                    & (self.df_current_data['n1_VMC_WAVE1'][symbol] < self.df_current_data['n1_VMC_WAVE2'][symbol])
                    & (self.df_current_data['CHOP'][symbol] < self.chop_Threshold)
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

    def get_df_selling_symbols(self, lst_symbols):
        df_result = pd.DataFrame(columns = ['symbol', 'stimulus'])
        for symbol in self.df_current_data.index.to_list():
            if (
                    (self.df_current_data['AO'][symbol] < self.AO_Threshold
                     and self.df_current_data['STOCH_RSI'][symbol] >  self.stochOverSold)
                    or self.df_current_data['WILLR'][symbol] > self.willOverBought
            ):
                df_row = pd.DataFrame(data={"symbol":[symbol], "stimulus":["SELL"]})
                df_result = pd.concat((df_result, df_row), axis = 0)

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

    def update(self, current_datetime, current_trades, broker_cash, prices_symbols):
        self.rtctrl.update_rtctrl(current_datetime, current_trades, broker_cash, prices_symbols)
        self.rtctrl.display_summary_info()

    def get_symbol_buying_size(self, symbol):
        if self.rtctrl.prices_symbols[symbol] < 0: # first init at -1
            return 0

        available_cash = self.rtctrl.wallet_cash
        if available_cash == 0:
            return 0

        wallet_value = available_cash

        cash_to_buy = wallet_value * self.SPLIT / 100

        if cash_to_buy > available_cash:
            cash_to_buy = available_cash

        size = cash_to_buy / self.rtctrl.prices_symbols[symbol]

        percent =  cash_to_buy * 100 / wallet_value

        return size, percent

    def get_portfolio_value(self):
        return self.rtctrl.df_rtctrl['portfolio_value'].sum()