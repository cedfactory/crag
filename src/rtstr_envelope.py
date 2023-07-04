import pandas as pd
import numpy as np
from . import trade
import json
import time
import csv
from datetime import datetime
import datetime

from . import rtdp, rtstr, utils, rtctrl

# Reference: https://crypto-robot.com/blog/bollinger-trend
# Reference: https://github.com/CryptoRobotFr/backtest_tools/blob/main/backtest/single_coin/bol_trend.ipynb

class StrategyEnvelope(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        self.envelope = EnvelopeLevelStatus(self.lst_symbols)
        self.nb_envelope = 3

        self.zero_print = True

        self.tmp_debug_traces = True
        self.strategy_info_printed = False

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols
        ds.interval = self.strategy_interval
        ds.candle_stick = self.candle_stick

        ds.fdp_features = {"close": {},
                           "stoch_rsi": {"indicator": "stoch_rsi", "window_size": 14},
                           "envelope": {"indicator": "envelope", "window_size": 10,
                                        "ma": "sma", "ma_window_size": 5,
                                        # "ma_offset_1": "2", "ma_offset_2": "5", "ma_offset_3": "7",
                                        # "ma_offset_1": "3", "ma_offset_2": "5", "ma_offset_3": "7",
                                        "ma_offset_1": "2", "ma_offset_2": "3", "ma_offset_3": "5",
                                        "output": ["ma_base",
                                                   "envelope_long_1", "envelope_long_2", "envelope_long_3",
                                                   "envelope_short_1", "envelope_short_2", "envelope_short_3"]
                                        }
                           }

        ds.features = self.get_feature_from_fdp_features(ds.fdp_features)

        if not self.strategy_info_printed:
            print("startegy: ", self.get_info())
            print("strategy features: ", ds.features)
            self.strategy_info_printed = True

        # ['close', 'envelope', 'ma_base', 'envelope_long_1', 'envelope_long_2', 'envelope_long_3', 'envelope_short_1', 'envelope_short_2', 'envelope_short_3']
        return ds

    def get_info(self):
        return "StrategyEnvelop"

    def authorize_multi_transaction_for_symbols(self):
        # Multi buy is authorized for this strategy
        return True

    def condition_for_opening_long_position(self, symbol):
        return self.envelope.get_open_long_position_condition(self.df_current_data, symbol)

    def condition_for_opening_short_position(self, symbol):
        return self.envelope.get_open_short_position_condition(self.df_current_data, symbol)

    def condition_for_closing_long_position(self, symbol):
        return self.envelope.get_close_long_position_condition(self.df_current_data, symbol)

    def condition_for_closing_short_position(self, symbol):
        return self.envelope.get_close_short_position_condition(self.df_current_data, symbol)

    def envelope_strategy_on(self):
        return True

    def get_nb_envelope_to_purchase(self, symbol):
        nb_to_purchase = max(self.envelope.get_nb_long_position_to_purchase(symbol),
                             self.envelope.get_nb_short_position_to_purchase(symbol))

        if nb_to_purchase == 0:
            return 0
        elif self.envelope.get_nb_short_position_to_purchase(symbol) == 0 \
                and self.envelope.get_nb_long_position_to_purchase(symbol) > 0:
            # OPEN LONG
            nb_to_already_purchased = self.envelope.get_nb_long_position_already_purchased(symbol)
            if nb_to_already_purchased == self.nb_envelope:
                self.envelope.reset_nb_long_position_to_purchase(symbol)
                return 0
            else:
                self.envelope.set_nb_long_position_already_purchased(nb_to_purchase, symbol)
                self.envelope.reset_nb_long_position_to_purchase(symbol)
                return nb_to_purchase / (self.nb_envelope - nb_to_already_purchased)
        elif self.envelope.get_nb_long_position_to_purchase(symbol) == 0 \
                and self.envelope.get_nb_short_position_to_purchase(symbol) > 0:
            # OPEN SHORT
            nb_to_already_purchased = self.envelope.get_nb_short_position_already_purchased(symbol)
            if nb_to_already_purchased == self.nb_envelope:
                self.envelope.reset_nb_short_position_to_purchase(symbol)
                return 0
            else:
                self.envelope.set_nb_short_position_already_purchased(nb_to_purchase, symbol)
                self.envelope.reset_nb_short_position_to_purchase(symbol)
                return nb_to_purchase / (self.nb_envelope - nb_to_already_purchased)

    def get_symbol_buying_size(self, symbol):
        if not symbol in self.rtctrl.prices_symbols or self.rtctrl.prices_symbols[symbol] < 0:  # first init at -1
            return 0, 0, 0

        if self.rtctrl.init_cash_value == 0:  # CEDE DEBUG for resume
            print("init_cash_value: ", self.rtctrl.init_cash_value, " wallet_cash: ", self.rtctrl.wallet_cash)
            self.rtctrl.init_cash_value = self.rtctrl.wallet_cash

        if round(self.rtctrl.wallet_cash, 2) != round(self.rtctrl.cash_available, 2):
            print("traces get_symbol_buying_size - wallet cash: ", round(self.rtctrl.wallet_cash, 2),
                  " cash available: ", round(self.rtctrl.cash_available, 2))

        total_postion_engaged = self.get_nb_position_engaged()
        print("DEBUG - nb  position engaged: ", total_postion_engaged, " max position: ", self.MAX_POSITION)
        if total_postion_engaged > self.MAX_POSITION:
            print("max position reached: ", self.MAX_POSITION)
            print("symbol: ", symbol, "size: 0")
            return 0, 0, 0
        else:
            if self.MAX_POSITION == total_postion_engaged:
                self.buying_size = self.rtctrl.cash_available
            else:
                self.buying_size = self.rtctrl.cash_available * self.get_nb_envelope_to_purchase(symbol)
                print(symbol," get_nb_envelope_to_purchase: ", self.get_nb_envelope_to_purchase(symbol), " buying_size: ", self.buying_size)
            self.set_nb_increase_symbol_position_engaged(1, symbol)
            total_postion_engaged = self.get_nb_position_engaged()
            print("DEBUG - nb  position engaged increased to: ", total_postion_engaged, " max position: ", self.MAX_POSITION)


        available_cash = self.rtctrl.cash_available
        if available_cash == 0:
            print("symbol: ", symbol, "size: 0 cash: 0")
            return 0, 0, 0

        cash_to_buy = self.buying_size

        if cash_to_buy > available_cash:
            cash_to_buy = available_cash

        size = cash_to_buy / self.rtctrl.prices_symbols[symbol]

        percent = cash_to_buy * 100 / self.rtctrl.init_cash_value

        gridzone = -1

        # DEBUG CEDE:
        print("symbol: ", symbol,
              " size: ", size,
              " cash_to_buy: ", cash_to_buy,
              " available cash: ", available_cash,
              " price symbol: ", self.rtctrl.prices_symbols[symbol],
              " init_cash_value: ", self.rtctrl.init_cash_value,
              " wallet_cash: ", self.rtctrl.wallet_cash,
              " available cash: ", self.rtctrl.cash_available
              )

        return size, percent, gridzone

    def get_nb_envelope(self):
        return 3

class EnvelopeLevelStatus():
    def __init__(self, lst_symbols, params=None):
        self.nb_position_engaged_long = 0
        self.nb_position_engaged_short = 0
        self.df_position_status = pd.DataFrame(columns=["nb_position_engaged_long", "nb_long_already_purchased", "nb_position_long_to_purchase",
                                                        "nb_position_engaged_short", "nb_short_already_purchased", "nb_position_short_to_purchase"],
                                               index=lst_symbols)
        self.df_position_status["nb_position_engaged_long"] = 0
        self.df_position_status["nb_position_engaged_short"] = 0
        self.df_position_status["nb_long_already_purchased"] = 0
        self.df_position_status["nb_short_already_purchased"] = 0
        self.df_position_status["nb_position_long_to_purchase"] = 0
        self.df_position_status["nb_position_short_to_purchase"] = 0

    def reset_nb_position_engaged(self, symbol):
        self.reset_position_engaged_long(symbol)
        self.reset_position_engaged_short(symbol)

    def reset_position_engaged_long(self, symbol):
        self.reset_nb_long_position_engaged(symbol)
        self.reset_nb_long_position_already_purchased(symbol)
        self.reset_nb_long_position_to_purchase(symbol)

    def reset_position_engaged_short(self, symbol):
        self.reset_nb_short_position_engaged(symbol)
        self.reset_nb_short_position_already_purchased(symbol)
        self.reset_nb_short_position_to_purchase(symbol)

    ####### RESET #######
    def reset_nb_long_position_engaged(self, symbol):
        self.df_position_status.at[symbol, "nb_position_engaged_long"] = 0

    def reset_nb_short_position_engaged(self, symbol):
        self.df_position_status.at[symbol, "nb_position_engaged_short"] = 0

    def reset_nb_long_position_already_purchased(self, symbol):
        self.df_position_status.at[symbol, "nb_long_already_purchased"] = 0

    def reset_nb_short_position_already_purchased(self, symbol):
        self.df_position_status.at[symbol, "nb_short_already_purchased"] = 0

    def reset_nb_long_position_to_purchase(self, symbol):
        self.df_position_status.at[symbol, "nb_position_long_to_purchase"] = 0

    def reset_nb_short_position_to_purchase(self, symbol):
        self.df_position_status.at[symbol, "nb_position_short_to_purchase"] = 0

    ####### GET #######
    def get_nb_long_position_engaged(self, symbol):
        return self.df_position_status.at[symbol, "nb_position_engaged_long"]

    def get_nb_short_position_engaged(self, symbol):
        return self.df_position_status.at[symbol, "nb_position_engaged_short"]

    def get_nb_long_position_already_purchased(self, symbol):
        return self.df_position_status.at[symbol, "nb_long_already_purchased"]

    def get_nb_short_position_already_purchased(self, symbol):
        return self.df_position_status.at[symbol, "nb_short_already_purchased"]

    def get_nb_long_position_to_purchase(self, symbol):
        return self.df_position_status.at[symbol, "nb_position_long_to_purchase"]

    def get_nb_short_position_to_purchase(self, symbol):
        return self.df_position_status.at[symbol, "nb_position_short_to_purchase"]

    ####### SET #######
    def set_position_long(self, pos, symbol):
        nb_position = pos - self.get_nb_long_position_engaged(symbol)
        if nb_position > 0:
            self.df_position_status.at[symbol, "nb_position_engaged_long"] = pos
            self.df_position_status.at[symbol, "nb_position_long_to_purchase"] = nb_position

    def set_position_short(self, pos, symbol):
        nb_position = pos - self.get_nb_short_position_engaged(symbol)
        if nb_position > 0:
            self.df_position_status.at[symbol, "nb_position_engaged_short"] = pos
            self.df_position_status.at[symbol, "nb_position_short_to_purchase"] = nb_position

    def confirm_open_long(self, symbol, pos):
        return (pos - self.get_nb_long_position_engaged(symbol)) > 0

    def confirm_open_short(self, symbol, pos):
        return (pos - self.get_nb_short_position_engaged(symbol)) > 0

    def set_nb_long_position_already_purchased(self, pos, symbol):
        self.df_position_status.at[symbol, "nb_long_already_purchased"] += pos

    def set_nb_short_position_already_purchased(self, pos, symbol):
        self.df_position_status.at[symbol, "nb_short_already_purchased"] += pos

    ####### CONDITION #######
    def get_open_long_position_condition(self, df_current_data, symbol):
        if False:
            print("get_open_long_position_condition - symbol: ", symbol,
                  " envelope_long_1 ", df_current_data['envelope_long_1'][symbol],
                  " close: ", df_current_data['close'][symbol],
                  " test 1: ", df_current_data['close'][symbol] > df_current_data['envelope_long_1'][symbol],
                  " test 2: ", df_current_data['close'][symbol] < df_current_data['envelope_long_1'][symbol] \
                    and df_current_data['close'][symbol] > df_current_data['envelope_long_2'][symbol]\
                    and self.confirm_open_long(symbol, 1),
                  " self.confirm_open_long(symbol, 1): ", self.confirm_open_long(symbol, 1),
                  " test 3: ", df_current_data['close'][symbol] < df_current_data['envelope_long_2'][symbol] \
                    and df_current_data['close'][symbol] > df_current_data['envelope_long_3'][symbol] \
                    and self.confirm_open_long(symbol, 2)
                  )
        if df_current_data['close'][symbol] > df_current_data['envelope_long_1'][symbol]:
            return False
        elif df_current_data['close'][symbol] < df_current_data['envelope_long_1'][symbol] \
                and df_current_data['close'][symbol] > df_current_data['envelope_long_2'][symbol]\
                and self.confirm_open_long(symbol, 1):
            self.set_position_long(1, symbol)
            return True
        elif df_current_data['close'][symbol] < df_current_data['envelope_long_2'][symbol] \
                and df_current_data['close'][symbol] > df_current_data['envelope_long_3'][symbol] \
                and self.confirm_open_long(symbol, 2):
            self.set_position_long(2, symbol)
            return True
        elif df_current_data['close'][symbol] < df_current_data['envelope_long_3'][symbol] \
                and self.confirm_open_long(symbol, 3):
            self.set_position_long(3, symbol)
            return True
        else:
            return False

    def get_open_short_position_condition(self, df_current_data, symbol):
        if False:
            print("get_open_short_position_condition - symbol: ", symbol,
                  " envelope_short_1 ", df_current_data['envelope_short_1'][symbol],
                  " close: ", df_current_data['close'][symbol],
                  " test 1: ", df_current_data['close'][symbol] < df_current_data['envelope_short_1'][symbol],
                  " test 2: ", df_current_data['close'][symbol] > df_current_data['envelope_short_1'][symbol] \
                    and df_current_data['close'][symbol] < df_current_data['envelope_short_2'][symbol] \
                    and self.confirm_open_short(symbol, 1),
                  " self.confirm_open_short(symbol, 1): ", self.confirm_open_short(symbol, 1),
                  " test 3: ", df_current_data['close'][symbol] > df_current_data['envelope_short_2'][symbol] \
                    and df_current_data['close'][symbol] < df_current_data['envelope_short_3'][symbol] \
                    and self.confirm_open_short(symbol, 2)
                  )
        if df_current_data['close'][symbol] < df_current_data['envelope_short_1'][symbol]:
            return False
        elif df_current_data['close'][symbol] > df_current_data['envelope_short_1'][symbol] \
                and df_current_data['close'][symbol] < df_current_data['envelope_short_2'][symbol] \
                and self.confirm_open_short(symbol, 1):
            self.set_position_short(1, symbol)
            return True
        elif df_current_data['close'][symbol] > df_current_data['envelope_short_2'][symbol] \
                and df_current_data['close'][symbol] < df_current_data['envelope_short_3'][symbol] \
                and self.confirm_open_short(symbol, 2):
            self.set_position_short(2, symbol)
            return True
        elif df_current_data['close'][symbol] > df_current_data['envelope_short_3'][symbol] \
                and self.confirm_open_short(symbol, 3):
            self.set_position_short(3, symbol)
            return True
        else:
            return False

    def get_close_long_position_condition(self, df_current_data, symbol):
        if df_current_data['close'][symbol] >= df_current_data['ma_base'][symbol]:
            self.reset_position_engaged_long(symbol)
            return True
        else:
            return False

    def get_close_short_position_condition(self, df_current_data, symbol):
        if df_current_data['close'][symbol] <= df_current_data['ma_base'][symbol]:
            self.reset_position_engaged_short(symbol)
            return True
        else:
            return False






















