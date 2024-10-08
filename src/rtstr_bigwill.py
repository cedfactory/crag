from . import rtdp, rtstr
import pandas as pd

class StrategyBigWill(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.zero_print = True

        self.default_AO_Threshold = 0
        self.default_stochOverBought = 0.8
        self.default_stochOverSold = 0.2
        self.default_willOverSold = -80
        self.default_willOverBought = -10

        self.AO_Threshold = self.default_AO_Threshold
        self.stochOverBought = self.default_stochOverBought
        self.stochOverSold = self.default_stochOverSold
        self.willOverSold = self.default_willOverSold
        self.willOverBought = self.default_willOverBought

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols

        ds.fdp_features = { "ao" : {"indicator": "ao", "ao_window_1": 6, "ao_window_2": 22, "window_size": 22},
                            "ema100" : {"indicator": "ema", "id": "100", "window_size": 100},
                            "ema200": {"indicator": "ema", "id": "200", "window_size": 200},
                            "stoch_rsi": {"indicator": "stoch_rsi", "window_size": 30, "stoch_rsi_window_size":14},
                            "willr" : {"indicator": "willr", "window_size": 14},
                            "postprocess1": {"indicator": "shift", "window_size": 1, "n": "1", "input": ["ao"]}
                            }

        ds.features = self.get_feature_from_fdp_features(ds.fdp_features)
        ds.interval = self.strategy_interval
        print("startegy: ", self.get_info())
        print("strategy features: ", ds.features)

        return ds

    def get_info(self):
        return "StrategyBigWill"

    #    row['AO'] >= 0
    #    and previousRow['AO'] > row['AO']
    #    and row['WillR'] < willOverSold
    #    and row['EMA100'] > row['EMA200']
    def condition_for_opening_long_position(self, symbol):
        try:
            self.willOverSold = int(self.df_symbol_param.at[symbol, 'willOverSold'])
        except:
            self.willOverSold = self.default_willOverSold

        return self.df_current_data['ao'][symbol] > self.AO_Threshold and \
               self.df_current_data['n1_ao'][symbol] > self.df_current_data['ao'][symbol] and \
               self.df_current_data['willr'][symbol] < self.willOverSold and \
               self.df_current_data['ema_100'][symbol] > self.df_current_data['ema_200'][symbol]

    def condition_for_opening_short_position(self, symbol):
        try:
            self.willOverBought = int(self.df_symbol_param.at[symbol, 'willOverBought'])
        except:
            self.willOverBought = self.default_willOverBought

        return self.df_current_data['ao'][symbol] < self.AO_Threshold and \
               self.df_current_data['n1_ao'][symbol] < self.df_current_data['ao'][symbol] and \
               self.df_current_data['willr'][symbol] > self.willOverBought and \
               self.df_current_data['ema_100'][symbol] < self.df_current_data['ema_200'][symbol]

    #   (row['AO'] < 0
    #       and row['STOCH_RSI'] > stochOverSold)
    #   or row['WillR'] > willOverBought
    def condition_for_closing_long_position(self, symbol):
        try:
            self.stochOverSold = int(self.df_symbol_param.at[symbol, 'stochOverSold'])
        except:
            self.stochOverSold = self.default_stochOverSold
        try:
            self.willOverBought = int(self.df_symbol_param.at[symbol, 'willOverBought'])
        except:
            self.willOverBought = self.default_willOverBought

        return (self.df_current_data['ao'][symbol] < self.AO_Threshold
                and self.df_current_data['stoch_rsi'][symbol] > self.stochOverSold) \
               or self.df_current_data['willr'][symbol] > self.willOverBought

    def condition_for_closing_short_position(self, symbol):
        try:
            self.stochOverBought = int(self.df_symbol_param.at[symbol, 'stochOverBought'])
        except:
            self.stochOverBought = self.default_stochOverBought
        try:
            self.willOverSold = int(self.df_symbol_param.at[symbol, 'willOverSold'])
        except:
            self.willOverSold = self.default_willOverSold

        return (self.df_current_data['ao'][symbol] > self.AO_Threshold
                and self.df_current_data['stoch_rsi'][symbol] < self.stochOverBought) \
               or self.df_current_data['willr'][symbol] < self.willOverSold