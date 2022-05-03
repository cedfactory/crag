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

        self.SL = - 3 * 0.07    # Stop Loss %
        self.TP = -0.07 + 0.07 * 3   # Take Profit %
        self.SPLIT = 10  # Asset Split Overall Percent Size

    def get_crypto_buying_list(self, current_data, df_rtctrl):
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
        FOR_RAPID_TEST = True
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


    def get_crypto_selling_list(self, current_trade, sell_trade, lst_symbols_to_buy, df_rtctrl):

        delta_duration = sell_trade.time - current_trade.time
        holding_hours = int(delta_duration / datetime.timedelta(hours=1))

        if holding_hours >= 6:
            holding_hours_coef = 4
        if holding_hours >= 3:
            holding_hours_coef = 2
        if holding_hours <= 1:
            holding_hours_coef = 1

        # sell_trade.roi = sell_trade.net_price - current_trade.gross_price
        sell_trade.roi = sell_trade.net_price - current_trade.gross_price - sell_trade.selling_fee

        if len(df_rtctrl) > 0:
            df_rtctrl.set_index('symbol', inplace=True)
            symbol = sell_trade.symbol
            if df_rtctrl["roi_%"][symbol] >= self.TP:
                sell_trade.stimulus = "GET_PROFIT"
            elif df_rtctrl["roi_%"][symbol] <= self.SL:
                sell_trade.stimulus = "STOP_LOSS"
            elif (sell_trade.roi > 0.2) & (holding_hours > 8):
                sell_trade.stimulus = "GET_PROFIT_TIMER"


        return sell_trade
