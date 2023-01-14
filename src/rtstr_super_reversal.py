from . import rtdp, rtstr, rtctrl

# Ref:
# https://github.com/CryptoRobotFr/backtest_tools/blob/main/backtest/single_coin/super_reversal.ipynb
# https://fr.tradingview.com/script/EDMXVEIV/
# https://www.youtube.com/watch?v=nj_1IiVQ28Q
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
        ds.features = { "super_reversal": 400,
                        "superreversal": 400,
                        "close": None,
                        "low": None,
                        "high": None,
                        "ema_short": 5,
                        "ema_long": 400,
                        "super_trend_direction": 15,
                        "n1_ema_short": 5,
                        "n1_ema_long": 400,
                        "n1_super_trend_direction": 15
                        }
        return ds

    def get_info(self):
        return "StrategySuperReversal"

    def condition_for_opening_long_position(self, symbol):
        if self.tradingview_condition:
            # open_long = dir < 0 and sma > lma and close > sma
            result = (self.df_current_data['ema_short'][symbol] > self.df_current_data['ema_long'][symbol])\
                     & (self.df_current_data['super_trend_direction'][symbol] == True)\
                     & (self.df_current_data['close'][symbol] > self.df_current_data['ema_short'][symbol])
        else:
            # df['n1_ema_short'] >= df['n1_ema_long']) & (df['n1_super_trend_direction'] == True) & (df['n1_ema_short'] > df['low'])
            result = (self.df_current_data['n1_ema_short'][symbol] >= self.df_current_data['n1_ema_long'][symbol])\
                     & (self.df_current_data['n1_super_trend_direction'][symbol] == True)\
                     & (self.df_current_data['n1_ema_short'][symbol] > self.df_current_data['low'][symbol])
        if result:
            print('toto')
        return result

    def condition_for_closing_long_position(self, symbol):
        if self.tradingview_condition:
            # close_long = (dir > 0 or sma < lma) and close < sma
            result = ((self.df_current_data['ema_short'][symbol] < self.df_current_data['n1_ema_long'][symbol])
                      | (self.df_current_data['super_trend_direction'][symbol] == False)) \
                     & (self.df_current_data['close'][symbol] < self.df_current_data['ema_short'][symbol])
        else:
            # ((df['n1_ema_short'] <= df['n1_ema_long']) | (df['n1_super_trend_direction'] == False)) & (df['n1_ema_short'] < df['high'])
            result = ((self.df_current_data['n1_ema_short'][symbol] <= self.df_current_data['n1_ema_long'][symbol])
                      | (self.df_current_data['n1_super_trend_direction'][symbol] == False)) \
                     & (self.df_current_data['n1_ema_short'][symbol] < self.df_current_data['high'][symbol])
        if result:
            print('toto')
        return result

    def condition_for_opening_short_position(self, symbol):
        if self.short_and_long:
            if self.tradingview_condition:
                # open_short = dir > 0 and sma < lma and close < sma
                result = (self.df_current_data['ema_short'][symbol] < self.df_current_data['ema_long'][symbol]) \
                         & (self.df_current_data['super_trend_direction'][symbol] == False)\
                         & (self.df_current_data['close'][symbol] < self.df_current_data['ema_short'][symbol])
            else:
                result = (self.df_current_data['n1_ema_short'][symbol] <= self.df_current_data['n1_ema_long'][symbol]) \
                         & (self.df_current_data['n1_super_trend_direction'][symbol] == False)\
                         & (self.df_current_data['n1_ema_short'][symbol] < self.df_current_data['high'][symbol])
        else:
            result = False
        return result

    def condition_for_closing_short_position(self, symbol):
        if self.short_and_long:
            if self.tradingview_condition:
                # close_short = (dir < 0 or sma > lma) and close > sma
                result = ((self.df_current_data['ema_short'][symbol] >= self.df_current_data['ema_long'][symbol])
                          | (self.df_current_data['super_trend_direction'][symbol] == True)) \
                         & (self.df_current_data['close'][symbol] > self.df_current_data['ema_short'][symbol])
            else:
                result = ((self.df_current_data['n1_ema_short'][symbol] >= self.df_current_data['n1_ema_long'][symbol])
                          | (self.df_current_data['n1_super_trend_direction'][symbol] == True)) \
                         & (self.df_current_data['n1_ema_short'][symbol] > self.df_current_data['low'][symbol])
        else:
            result = False
        return result