import pandas as pd
from . import rtdp, rtstr, rtctrl

class StrategyTvRecommendationMid(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        self.zero_print = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols
        ds.features = { "tv_1h" : None,
                        "tv_2h" : None,
                        "tv_4h" : None,
                        "tv_1d" : None
                        }
        return ds

    def get_info(self):
        return "StrategyTvRecommendationMid"

    def condition_for_opening_long_position(self, symbol):
        lst_recom = [
            self.df_current_data['tv_1h'][symbol],
            self.df_current_data['tv_2h'][symbol],
            self.df_current_data['tv_4h'][symbol],
            self.df_current_data['tv_1d'][symbol]
        ]

        strong_buy_count = lst_recom.count('STRONG_BUY')
        # buy_count = lst_recom.count('BUY')

        if ('SELL' in lst_recom or 'STRONG_SELL' in lst_recom or 'NEUTRAL' in lst_recom):
            # SELL signal
            buying_signal = False
        else:
            if (strong_buy_count >= 2):
                # BUY signal
                print('OPEN LONG POSITION: ', symbol, " ", lst_recom)
                buying_signal = True
            else:
                # HOLD signal
                buying_signal = False

        return buying_signal

    def condition_for_closing_long_position(self, symbol):
        lst_recom = [
            self.df_current_data['tv_1h'][symbol],
            self.df_current_data['tv_2h'][symbol],
            self.df_current_data['tv_4h'][symbol],
            self.df_current_data['tv_1d'][symbol]
        ]

        strong_buy_count = lst_recom.count('STRONG_BUY')
        # buy_count = lst_recom.count('BUY')

        if ('SELL' in lst_recom or 'STRONG_SELL' in lst_recom or 'NEUTRAL' in lst_recom):
            # SELL signal
            print('CLOSE LONG POSITION: ', symbol, " ", lst_recom)
            selling_signal = True
        else:
            if (strong_buy_count >= 1):
                # BUY signal
                selling_signal = False
            else:
                # HOLD signal
                selling_signal = False
                print('HOLD POSITION: ', symbol, " ", lst_recom)

        return selling_signal