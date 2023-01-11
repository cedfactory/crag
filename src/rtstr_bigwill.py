from . import rtdp, rtstr, rtctrl

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

    def condition_for_opening_long_position(self, symbol):
        return self.df_current_data['AO'][symbol] >= self.AO_Threshold and \
               self.df_current_data['previous_AO'][symbol] > self.df_current_data['AO'][symbol] and \
               self.df_current_data['WILLR'][symbol] < self.willOverSold and \
               self.df_current_data['EMA100'][symbol] > self.df_current_data['EMA200'][symbol]

    def condition_for_closing_long_position(self, symbol):
        return (self.df_current_data['AO'][symbol] < self.AO_Threshold
                and self.df_current_data['STOCH_RSI'][symbol] > self.stochOverSold) \
               or self.df_current_data['WILLR'][symbol] > self.willOverBought


