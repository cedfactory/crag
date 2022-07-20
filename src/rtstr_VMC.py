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

    def condition_for_buying(self, symbol):
        return (
                    (self.df_current_data['ema_short_vmc'][symbol] > self.df_current_data['ema_long_vmc'][symbol])
                    & (self.df_current_data['close'][symbol] > self.df_current_data['ema_short_vmc'][symbol])
                    & (self.df_current_data['MONEY_FLOW'][symbol] > self.positive_Threshold)
                    & (self.df_current_data['VMC_WAVE1'][symbol] < self.positive_Threshold)
                    & (self.df_current_data['VMC_WAVE2'][symbol] < self.positive_Threshold)
                    & (self.df_current_data['VMC_WAVE1'][symbol] > self.df_current_data['VMC_WAVE2'][symbol])
                    & (self.df_current_data['n1_VMC_WAVE1'][symbol] < self.df_current_data['n1_VMC_WAVE2'][symbol])
                    & (self.df_current_data['CHOP'][symbol] < self.chop_Threshold)
            )

    def condition_for_selling(self, symbol, df_sl_tp):
        return (
                (self.df_current_data['AO'][symbol] < self.AO_Threshold
                    and self.df_current_data['STOCH_RSI'][symbol] >  self.stochOverSold)
                or self.df_current_data['WILLR'][symbol] > self.willOverBought
            ) or (
                (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] > self.TP)
                or (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] < self.SL)
            )
