import pandas as pd
import numpy as np
from . import trade
import json
import time
import csv
from datetime import datetime
import datetime

from . import rtstr, utils, rtctrl

class RTStrTradingView(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl()

        self.SL = -0.2    # Stop Loss %
        self.TP = 0.2   # Take Profit %
        self.TimerSL = self.SL/2    # Stop Loss %
        self.TimerTP = self.TP/2    # Take Profit %
        self.Timer = 12
        self.SPLIT = 10  # Asset Split Overall Percent Size

    def get_crypto_buying_list(self, current_data):
        # INPUT: df data received from fdp
        # OUTPUT: buying list

        df_portfolio = pd.read_json(current_data["portfolio"])

        # remove unused columns
        unused_columns = [column for column in df_portfolio.columns if column.startswith('RECOMMENDATION_')]
        unused_columns.extend(["rank_change1h", "rank_change24h"])
        df_portfolio.drop(unused_columns, axis=1, inplace=True)

        # sum all the buy, neutral & sell values
        for action in ["buy", "neutral", "sell"]:
            columns = [column for column in df_portfolio.columns if column.startswith(action + "_")]
            df_portfolio["sum_" + action] = df_portfolio.loc[:, columns].sum(axis=1)

        # CEDE for Debug
        FOR_RAPID_TEST = False
        if FOR_RAPID_TEST:
            out_condition = 1
        else:
            out_condition = -1

        # compute a score based on TDView and 24h & 1h trends
        # df_portfolio['score'] = np.where( (df_portfolio['change24h'] <= 0) | (df_portfolio['change1h'] <=0),
        df_portfolio['score'] = np.where((df_portfolio['change24h'] <= 0),
                                          out_condition,
                                          df_portfolio['sum_buy'] + 1 * df_portfolio['change24h'] + 1 * df_portfolio['change1h'] - 2 * df_portfolio['sum_sell'] - df_portfolio['sum_neutral'])

        df_portfolio.sort_values(by=['score'], ascending=False, inplace=True)

        # get rid of lower scores
        df_portfolio.drop(df_portfolio[df_portfolio.score <= 0].index, inplace=True)

        lst_symbols_to_buy = df_portfolio['symbol'].tolist()

        df_rtctrl = self.rtctrl.df_rtctrl.copy()
        if len(df_rtctrl) > 0:
            lst_symbol = []
            df_rtctrl.set_index('symbol', inplace=True)
            for symbol in lst_symbols_to_buy:
                try:
                    if df_rtctrl["wallet_%"][symbol] < self.SPLIT:
                        lst_symbol.append(symbol)
                except:
                    lst_symbol.append(symbol)
            lst_symbols_to_buy = lst_symbol

        return lst_symbols_to_buy


    def get_crypto_selling_list(self, sell_trade):
        if len(df_rtctrl) > 0:
            symbol = sell_trade.symbol
        
            df_rtctrl = self.rtctrl.df_rtctrl.copy()
            df_rtctrl.set_index('symbol', inplace=True)
            sell_trade.roi = df_rtctrl["roi_%"][symbol]
            if df_rtctrl["roi_%"][symbol] >= self.TP:
                sell_trade.stimulus = "GET_PROFIT"
            elif df_rtctrl["roi_%"][symbol] <= self.SL:
                sell_trade.stimulus = "STOP_LOSS"
            elif df_rtctrl["recommendation"][symbol] == 'STRONG_SELL':
                sell_trade.stimulus = "RECOMMENDATION_STRONG_SELL"
            elif df_rtctrl["recommendation"][symbol] == 'SELL':
                sell_trade.stimulus = "RECOMMENDATION_SELL"

        return sell_trade


    def update(self, current_trades, broker_cash):
        self.rtctrl.update_rtctrl(current_trades, broker_cash)
        self.rtctrl.display_summary_info()
