import pandas as pd
from . import rtdp, rtstr

class StrategyCryptobot(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.zero_print = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols
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
        return "StrategyCryptobot"

    def condition_for_opening_long_position(self, symbol):
        return self.df_current_data["ema12gtema26co"][symbol] \
            and self.df_current_data["macdgtsignal"][symbol] \
            and self.df_current_data["goldencross"][symbol] \
            and self.df_current_data["obv_pc"][symbol] > -5 \
            and self.df_current_data["eri_buy"][symbol]

    def condition_for_closing_long_position(self, symbol):
        return self.df_current_data["ema12ltema26co"][symbol] \
               and self.df_current_data["macdltsignal"][symbol]