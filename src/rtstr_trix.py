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

        self.zero_print = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        #ds.symbols = ds.symbols[:2]
        ds.features = { "TRIX_HISTO" : {"feature": "trix", "period": 21},
                        "STOCH_RSI": {"feature": "stoch_rsi", "period": 14}
                        }
        return ds

    def get_info(self):
        return "StrategyTrix", self.str_sl, self.str_tp

    def condition_for_buying(self, symbol):
        return self.df_current_data['TRIX_HISTO'][symbol] > 0 and self.df_current_data['STOCH_RSI'][symbol] < 0.8

    def condition_for_selling(self, symbol, df_sl_tp):
        return (
                    self.df_current_data['TRIX_HISTO'][symbol] < 0 and self.df_current_data['STOCH_RSI'][symbol] > 0.2
            ) or (
                    (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] > self.TP)
                    or (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] < self.SL)
            )
