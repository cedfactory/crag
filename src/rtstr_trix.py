from . import rtdp, rtstr, rtctrl

class StrategyTrix(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        self.zero_print = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols
        ds.features = { "TRIX_HISTO" : {"feature": "trix", "period": 21},
                        "STOCH_RSI": {"feature": "stoch_rsi", "period": 14}
                        }
        return ds

    def get_info(self):
        return "StrategyTrix"

    def condition_for_opening_long_position(self, symbol):
        return self.df_current_data['TRIX_HISTO'][symbol] > 0 and self.df_current_data['STOCH_RSI'][symbol] < 0.8

    def condition_for_closing_long_position(self, symbol):
        return self.df_current_data['TRIX_HISTO'][symbol] < 0 and self.df_current_data['STOCH_RSI'][symbol] > 0.2