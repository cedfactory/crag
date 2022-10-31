import pandas as pd
import numpy as np
from . import trade
import json
import time
import csv
from datetime import datetime
import datetime

from . import rtdp, rtstr, utils, rtctrl

class StrategyTvRecommendationMid(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)

        self.zero_print = True

        self.MAX_POSITION = 10
        self.TP = 10
        self.SL = -2

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.features = { "tv_1h" : None,
                        "tv_2h" : None,
                        "tv_4h" : None,
                        "tv_1d" : None
                        }

        return ds

    def get_info(self):
        return "StrategyTvRecommendationMid", self.str_sl, self.str_tp

    def condition_for_buying(self, symbol):
        lst_recom = [
            self.df_current_data['tv_1h'][symbol],
            self.df_current_data['tv_2h'][symbol],
            self.df_current_data['tv_4h'][symbol],
            self.df_current_data['tv_1d'][symbol]
        ]

        strong_buy_count = lst_recom.count('STRONG_BUY')
        # buy_count = lst_recom.count('BUY')

        if ('SELL' in lst_recom or 'STRONG_SELL' in lst_recom or 'NEUTRAL' in lst_recom):
            # SELL signal
            buying_signal = False
        else:
            if (strong_buy_count >= 2):
                # BUY signal
                print('BUY: ', symbol, " ", lst_recom)
                buying_signal = True
            else:
                # HOLD signal
                buying_signal = False

        return buying_signal

    def condition_for_selling(self, symbol, df_sl_tp):
        lst_recom = [
            self.df_current_data['tv_1h'][symbol],
            self.df_current_data['tv_2h'][symbol],
            self.df_current_data['tv_4h'][symbol],
            self.df_current_data['tv_1d'][symbol]
        ]

        strong_buy_count = lst_recom.count('STRONG_BUY')
        # buy_count = lst_recom.count('BUY')

        if ('SELL' in lst_recom or 'STRONG_SELL' in lst_recom or 'NEUTRAL' in lst_recom):
            # SELL signal
            print('SELL: ', symbol, " ", lst_recom)
            selling_signal = True
        else:
            if (strong_buy_count >= 1):
                # BUY signal
                selling_signal = False
            else:
                # HOLD signal
                selling_signal = False
                print('HOLD: ', symbol, " ", lst_recom)


        return (
                   selling_signal
            ) or (
                    (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] > self.TP)
                    or (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] < self.SL)
            )
