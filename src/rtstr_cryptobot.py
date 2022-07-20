import pandas as pd
import numpy as np
from . import trade
import json
import time
import csv
from datetime import datetime
import datetime

from . import rtdp, rtstr, utils, rtctrl

class StrategyCryptobot(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)

        self.zero_print = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        #ds.symbols = ds.symbols[:2]
        ds.features = { "ema12gtema26co": {"feature": "ema12gtema26co", "period": 26},  # used for buying signal
                        "macdgtsignal":   {"feature": "macdgtsignal", "period": 26},  # used for buying signal
                        "goldencross":    {"feature": "goldencross", "period": 14},  # used for buying signal
                        "obv_pc":         {"feature": "obv_pc", "period": 14},  # used for buying signal
                        "eri_buy":        {"feature": "eri_buy", "period": 14}, # used for buying signal
                        "ema12ltema26co": {"feature": "ema12ltema26co", "period": 26}, # used for selling signal
                        "macdltsignal":   {"feature": "macdltsignal", "period": 26}    # used for selling signal
                        }
        return ds

    def get_info(self):
        return "cryptobot", self.str_sl, self.str_tp

    def condition_for_buying(self, symbol):
        return self.df_current_data["ema12gtema26co"][symbol] \
            and self.df_current_data["macdgtsignal"][symbol] \
            and self.df_current_data["goldencross"][symbol] \
            and self.df_current_data["obv_pc"][symbol] > -5 \
            and self.df_current_data["eri_buy"][symbol]

    def condition_for_selling(self, symbol, df_sl_tp):
        return (
                    (self.df_current_data["ema12ltema26co"][symbol])
                    and (self.df_current_data["macdltsignal"][symbol])
            ) or (
                    (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] > self.TP)
                    or (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] < self.SL)
            )
