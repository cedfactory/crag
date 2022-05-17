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

        self.SL = -0.2    # Stop Loss %
        self.TP = 0.2   # Take Profit %
        self.TimerSL = self.SL/2    # Stop Loss %
        self.TimerTP = self.TP/2    # Take Profit %
        self.Timer = 12
        self.SPLIT = 10  # Asset Split Overall Percent Size

    def get_data_description(self):
        ds = rtdp.DataDescription()
        #ds.symbols = ds.symbols[:2]
        ds.features = { "low" : None,
                        "high" : None,
                        "ema_short" : {"feature": "ema", "period": 5},
                        "ema_long" : {"feature": "ema", "period": 400},
                        "super_trend_direction" : {"feature": "super_trend"}}

        return ds

    def set_current_data(self, current_data):
        self.df_current_data = current_data

    def get_df_buying_symbols(self):
        df_result = pd.DataFrame(columns = ['symbol', 'size'])
        for symbol in self.df_current_data.index.to_list():
            if (self.df_current_data['ema_short'][symbol] >= self.df_current_data['ema_long'][symbol]
                and self.df_current_data['super_trend_direction'][symbol] == True
                and self.df_current_data['ema_short'][symbol] > self.df_current_data['low'][symbol]):
                    df_row = pd.DataFrame(data={"symbol":[symbol], "size":[-1]})
                    df_result = pd.concat((df_result, df_row), axis = 0)

        return df_result

    def get_df_selling_symbols(self, lst_symbols):
        df_result = pd.DataFrame(columns = ['symbol', 'stimulus'])
        for symbol in self.df_current_data.index.to_list():
            if ((self.df_current_data['ema_short'][symbol] <= self.df_current_data['ema_long'][symbol]
                or self.df_current_data['super_trend_direction'][symbol] == False)
                and self.df_current_data['ema_short'][symbol] < self.df_current_data['high'][symbol]):
                    df_row = pd.DataFrame(data={"symbol":[symbol], "stimulus":["SELL"]})
                    df_result = pd.concat((df_result, df_row), axis = 0)

        return df_result

    def update(self, current_trades, broker_cash, prices_symbols):
        self.rtctrl.update_rtctrl(current_trades, broker_cash, prices_symbols)
        self.rtctrl.display_summary_info()
