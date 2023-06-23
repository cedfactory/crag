from . import rtdp, rtstr, rtctrl
import pandas as pd

class StrategyBigWill(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        self.zero_print = True

        self.willOverSold = -85
        self.willOverBought = -10
        self.AO_Threshold = 0
        self.stochOverBought = 0.8
        self.stochOverSold = 0.2

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols

        ds.fdp_features = { "ao" : {"indicator": "ao", "ao_window_1": 6, "ao_window_2": 22, "window_size": 22},
                            "ema100" : {"indicator": "ema", "id": "100", "window_size": 100},
                            "ema200": {"indicator": "ema", "id": "200", "window_size": 200},
                            "stoch_rsi": {"indicator": "stoch_rsi", "window_size": 14},
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

    def condition_for_opening_long_position(self, symbol):
        return self.df_current_data['ao'][symbol] >= self.AO_Threshold and \
               self.df_current_data['n1_ao'][symbol] > self.df_current_data['ao'][symbol] and \
               self.df_current_data['willr'][symbol] < self.willOverSold and \
               self.df_current_data['ema_100'][symbol] > self.df_current_data['ema_200'][symbol]

    def condition_for_opening_short_position(self, symbol):
        return self.df_current_data['ao'][symbol] <= self.AO_Threshold and \
               self.df_current_data['n1_ao'][symbol] < self.df_current_data['ao'][symbol] and \
               self.df_current_data['willr'][symbol] > self.willOverSold and \
               self.df_current_data['ema_100'][symbol] < self.df_current_data['ema_200'][symbol]

    def condition_for_closing_long_position(self, symbol):
        return (self.df_current_data['ao'][symbol] < self.AO_Threshold
                and self.df_current_data['stoch_rsi'][symbol] > self.stochOverSold) \
               or self.df_current_data['willr'][symbol] > self.willOverBought

    def condition_for_closing_short_position(self, symbol):
        return (self.df_current_data['ao'][symbol] > self.AO_Threshold
                and self.df_current_data['stoch_rsi'][symbol] < self.stochOverBought) \
               or self.df_current_data['willr'][symbol] < self.willOverSold

    def sort_list_symbols(self, lst_symbols):
        print("symbol list: ", lst_symbols)
        df = pd.DataFrame(index=lst_symbols, columns=['ao'])
        for symbol in lst_symbols:
            df.at[symbol, 'ao'] = self.df_current_data['ao'][symbol]
        df.sort_values(by=['ao'], inplace=True, ascending=False)
        lst_symbols = df.index.to_list()
        print("sorted symbols with AO: ", lst_symbols)
        return lst_symbols