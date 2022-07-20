import pandas as pd
import numpy as np
from . import trade
import json
import time
import csv
from datetime import datetime
import datetime

from . import rtdp, rtstr, utils, rtctrl

class StrategyBigWill(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)

        self.willOverSold = -85
        self.willOverBought = -10
        self.AO_Threshold = 0
        self.stochOverBought = 0.8
        self.stochOverSold = 0.2

    def get_data_description(self):
        ds = rtdp.DataDescription()
        #ds.symbols = ds.symbols[:2]
        ds.features = { "low" : None,
                        "high" : None,
                        "AO" : {"feature": "AO", "period": 22},
                        "previous_AO": {"feature": "previous_AO", "period": 22},
                        "EMA100" : {"feature": "EMA100", "period": 100},
                        "EMA200": {"feature": "EMA200", "period": 200},
                        "STOCH_RSI": {"feature": "STOCH_RSI", "period": 14},
                        "WILLR" : {"feature": "WILLR", "period": 14}
                        }

        return ds

    def condition_for_buying(self, symbol):
        return self.df_current_data['AO'][symbol] >= self.AO_Threshold and \
            self.df_current_data['previous_AO'][symbol] > self.df_current_data['AO'][symbol] and \
            self.df_current_data['WILLR'][symbol] < self.willOverSold and \
            self.df_current_data['EMA100'][symbol] > self.df_current_data['EMA200'][symbol]

    def condition_for_selling(self, symbol, df_sl_tp):
        return ((self.df_current_data['AO'][symbol] < self.AO_Threshold and \
                self.df_current_data['STOCH_RSI'][symbol] >  self.stochOverSold) \
                or self.df_current_data['WILLR'][symbol] > self.willOverBought \
            ) or ( \
                (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] > self.TP) \
                or (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] < self.SL))



