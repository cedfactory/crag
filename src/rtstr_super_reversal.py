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
        ds.features.append("sma10")

        return ds

    def set_current_data(self, current_data):
        self.df_current_data = current_data

    def get_df_buying_symbols(self):
        df = pd.DataFrame(columns = ['symbol', 'size'])
        for symbol in self.df_current_data.index.values.tolist():
            df_row = pd.DataFrame(data={"symbol":[symbol], "size":[1]})
            df = pd.concat((df, df_row), axis = 0)
        return df

    def get_df_selling_symbols(self, lst_symbols):
        df = pd.DataFrame(columns = ['symbol', 'stimulus'])
        return df

    def update(self, current_trades, broker_cash, prices_symbols):
        self.rtctrl.update_rtctrl(current_trades, broker_cash, prices_symbols)
        self.rtctrl.display_summary_info()

    def get_price_symbol(self, symbol):
        return self.df_current_data.loc[symbol]["close"]
        