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

        self.rtctrl = rtctrl.rtctrl(params=params)

        self.zero_print = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        #ds.symbols = ds.symbols[:2]
        ds.features = { "low" : None,
                        "high" : None,
                        "ema_short" : {"feature": "ema", "period": 5},
                        "ema_long" : {"feature": "ema", "period": 400},
                        "super_trend_direction" : {"feature": "super_trend"}
                        }

        return ds

    def get_info(self):
        return "superreversal", self.str_sl, self.str_tp

    def condition_for_buying(self, symbol):
        return self.df_current_data['ema_short'][symbol] >= self.df_current_data['ema_long'][symbol] \
                    and self.df_current_data['super_trend_direction'][symbol] == True \
                    and self.df_current_data['ema_short'][symbol] > self.df_current_data['low'][symbol]

    def condition_for_selling(self, symbol, df_sl_tp):
        return (
                    (self.df_current_data['ema_short'][symbol] <= self.df_current_data['ema_long'][symbol]
                     or self.df_current_data['super_trend_direction'][symbol] == False)
                    and self.df_current_data['ema_short'][symbol] < self.df_current_data['high'][symbol]
            ) or (
                    (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] > self.TP)
                    or (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] < self.SL)
            )
