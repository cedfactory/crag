from . import rtdp, rtstr, rtctrl

class StrategySuperReversal(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        self.zero_print = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols
        ds.features = { "low" : None,
                        "high" : None,
                        "ema_5" : None,
                        "ema_400" : None,
                        "super_trend_direction" : {"feature": "super_trend"}
                        }
        return ds

    def get_info(self):
        return "StrategySuperReversal"

    def condition_for_opening_long_position(self, symbol):
        return self.df_current_data['ema_5'][symbol] >= self.df_current_data['ema_400'][symbol] \
               and self.df_current_data['super_trend_direction'][symbol] == True \
               and self.df_current_data['ema_5'][symbol] > self.df_current_data['low'][symbol]

    def condition_for_closing_long_position(self, symbol):
        return (self.df_current_data['ema_5'][symbol] <= self.df_current_data['ema_400'][symbol]
                or self.df_current_data['super_trend_direction'][symbol] == False)\
               and self.df_current_data['ema_5'][symbol] < self.df_current_data['high'][symbol]