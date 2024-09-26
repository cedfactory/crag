from . import rtdp, rtstr
import pandas as pd

class StrategySLTPOnly(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.zero_print = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols

        ds.fdp_features = { "ema10" : {"indicator": "ema", "id": "10", "window_size": 10}
                            }

        ds.features = self.get_feature_from_fdp_features(ds.fdp_features)
        ds.interval = self.strategy_interval
        print("startegy: ", self.get_info())
        print("strategy features: ", ds.features)

        return ds

    def get_info(self):
        return "StrategySLTPOnly"

    def condition_for_opening_long_position(self, symbol):
        return False

    def condition_for_opening_short_position(self, symbol):
        return False

    def condition_for_closing_long_position(self, symbol):
        return False

    def condition_for_closing_short_position(self, symbol):
        return False

    def get_strategy_type(self):
        return "INTERVAL"