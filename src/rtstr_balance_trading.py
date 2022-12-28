import pandas as pd
import os
from . import rtdp, rtstr, rtctrl

class StrategyBalanceTrading(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)

        self.zero_print = True

        # self.grid = GridLevelPosition(params=params)
        # self.previous_zone_position = 0
        # self.zone_position = 0

        # self.share_size = 10
        self.global_tp = 10000
        if params:
            # self.share_size = params.get("share_size", self.share_size)
            self.global_tp = params.get("global_tp", self.global_tp)
            if isinstance(self.global_tp, str):
                self.global_tp = int(self.global_tp)

        if self.global_tp == 0:
            self.global_tp = 10000
        self.net_size = 0.0
        self.global_tp_net = -1000
        self.tp_sl_abort = False

        # Strategy Specefics
        self.symbol_pct = 0.5          #BTC%
        self.USD_pct = 0.5             #USD%
        self.rebalance_pct = 0.01      #rebalance%

        self.price_symbol = 0
        self.balance_symbol = 0
        self.symbol_current_pct = 0

        self.usd_balance = 0
        self.usd_current_pct = 0

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = ["BTC/USD"]
        ds.features = { "close" : None }
        return ds

    def get_info(self):
        return "StrategyBalanceTradingMulti"

    def condition_for_buying(self, symbol):
        if self.tp_sl_abort:
            return False

        self.price_symbol = self.rtctrl.prices_symbols[symbol]
        try:
            self.balance_symbol = self.rtctrl.df_rtctrl.loc[self.rtctrl.df_rtctrl['symbol'] == symbol, 'size'][0]
        except:
            self.balance_symbol = 0
        self.usd_balance = self.rtctrl.wallet_cash

        if self.price_symbol * self.balance_symbol + self.usd_balance != 0:
            # price("BTC/USD")*balance("BTC")/(price("BTC/USD")*balance("BTC")+balance("USD"))
            self.symbol_current_pct = self.price_symbol * self.balance_symbol / (self.price_symbol * self.balance_symbol + self.usd_balance)
            # balance("USD")/(price("BTC/USD")*balance("BTC")+balance("USD"))
            self.usd_current_pct = self.usd_balance / (self.price_symbol * self.balance_symbol + self.usd_balance)
        else:
            self.symbol_current_pct = 0
            self.usd_current_pct = 0

        # if( get_variable("BTC_Current%") < get_variable("BTC%") - get_variable("rebalance%") )
        if self.symbol_current_pct < self.symbol_pct - self.rebalance_pct:
            buying_signal = True
        else:
            buying_signal = False

        return buying_signal

    def condition_for_selling(self, symbol, df_sl_tp):
        if self.tp_sl_abort:
            return True

        self.price_symbol = self.rtctrl.prices_symbols[symbol]
        try:
            self.balance_symbol = self.rtctrl.df_rtctrl.loc[self.rtctrl.df_rtctrl['symbol'] == symbol, 'size'][0]
        except:
            self.balance_symbol = 0
        self.usd_balance = self.rtctrl.wallet_cash

        if self.price_symbol * self.balance_symbol + self.usd_balance != 0:
            # price("BTC/USD")*balance("BTC")/(price("BTC/USD")*balance("BTC")+balance("USD"))
            self.symbol_current_pct = self.price_symbol * self.balance_symbol / (self.price_symbol * self.balance_symbol + self.usd_balance)
            # balance("USD")/(price("BTC/USD")*balance("BTC")+balance("USD"))
            self.usd_current_pct = self.usd_balance / (self.price_symbol * self.balance_symbol + self.usd_balance)
        else:
            self.symbol_current_pct = 0
            self.usd_current_pct = 0

        # get_variable("BTC_Current%")>get_variable("BTC%")+get_variable("rebalance%")
        if ((self.symbol_current_pct > self.symbol_pct + self.rebalance_pct)
                or ((isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] > self.TP)
                    or (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] < self.SL))):
            selling_signal = True
        else:
            selling_signal = False

        if self.rtctrl.wallet_value >= self.rtctrl.init_cash_value + self.rtctrl.init_cash_value * self.global_tp / 100:
            self.global_tp = (self.rtctrl.wallet_value - self.rtctrl.init_cash_value) * 100 / self.rtctrl.init_cash_value
            self.global_tp_net = self.global_tp - self.net_size
            print("global_tp: ", round(self.global_tp, 2), " net_tp: ", round(self.global_tp_net, 2), "protfolio: $", self.rtctrl.wallet_value)

        if self.rtctrl.wallet_value <= self.rtctrl.init_cash_value + self.rtctrl.init_cash_value * self.global_tp_net / 100:
            self.tp_sl_abort = True
            selling_signal = True
            print("abort: $", self.rtctrl.wallet_value)

        return selling_signal

    def get_symbol_buying_size(self, symbol):
        if not symbol in self.rtctrl.prices_symbols or self.rtctrl.prices_symbols[symbol] < 0: # first init at -1
            return 0, 0, 0

        available_cash = self.rtctrl.wallet_cash
        if available_cash == 0:
            return 0, 0, 0

        # balance("USD")/price("BTC/USD")/100
        size = available_cash / self.rtctrl.prices_symbols[symbol] / 100

        wallet_value = available_cash
        cash_to_buy = wallet_value

        # size = cash_to_buy / self.rtctrl.prices_symbols[symbol]

        # cash_to_buy => 100
        # size * self.rtctrl.prices_symbols[symbol] => percent
        # percent = 100 * size * self.rtctrl.prices_symbols[symbol] / cash_to_buy
        percent = 100 * size * self.rtctrl.prices_symbols[symbol] / cash_to_buy

        gridzone = -1
        return size, percent, gridzone

    def get_symbol_selling_size(self, symbol):
        if not symbol in self.rtctrl.prices_symbols or self.rtctrl.prices_symbols[symbol] < 0: # first init at -1
            return 0, 0, 0

        available_cash = self.rtctrl.wallet_cash
        if available_cash == 0:
            return 0, 0, 0

        # balance("BTC") / 100
        size = self.balance_symbol / 100

        wallet_value = available_cash
        cash_to_buy = wallet_value

        if cash_to_buy > available_cash:
            cash_to_buy = available_cash

        # size = cash_to_buy / self.rtctrl.prices_symbols[symbol]
        percent = 100 * size * self.rtctrl.prices_symbols[symbol] / cash_to_buy

        gridzone = -1
        return size, percent, gridzone

    def get_selling_limit(self, symbol):
        return 1

