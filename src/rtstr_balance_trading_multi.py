import pandas as pd
import os
from . import rtdp, rtstr, rtctrl

class StrategyBalanceTradingMulti(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.MAX_POSITION = 100    # Asset Overall Percent Size

        self.rtctrl = rtctrl.rtctrl(params=params)

        self.zero_print = True

        self.global_tp = 10000
        if params:
            self.global_tp = params.get("global_tp", self.global_tp)

        if self.global_tp == 0:
            self.global_tp = 10000
        self.net_size = 0.0
        self.global_tp_net = -1000
        self.tp_sl_abort = False

        self.limit_sell = 1

        # Strategy Specifics
        self.list_symbols = []
        self.symbol_pct = 0.5          #BTC%
        # CEDE Comment: Merge the 3 df into one unique
        self.df_symbol_pct = pd.DataFrame(columns=['symbol', 'symbol_pct'])
        self.df_balance_symbol = pd.DataFrame(columns=['symbol', 'balance_symbol'])
        self.df_symbol_current_pct = pd.DataFrame(columns=['symbol', 'balance_symbol'])

        self.df_selling_limits = pd.DataFrame(columns=['symbol', 'selling_limits'])

        self.usd_pct = 0.5             #USD%
        self.rebalance_pct = 0.01      #rebalance%

        self.price_symbol = 0

        self.balance_usd = 0

        self.symbol_current_pct = 0
        self.usd_current_pct = 0

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = [
            "BTC/USD",
            "ETH/USD",
            "XRP/USD",
            "BNB/USD",
            "FTT/USD"
        ]
        ds.features = { "close" : None }
        self.list_symbols = ds.symbols
        return ds

    def get_info(self):
        return "StrategyBalanceTradingMulti", self.str_sl, self.str_tp

    def condition_for_buying(self, symbol):
        if self.tp_sl_abort:
            return False

        if len(self.df_selling_limits) == 0:
            self.set_df_multi()

        self.price_symbol = self.rtctrl.prices_symbols[symbol]
        try:
            self.df_balance_symbol.loc[self.df_balance_symbol['symbol'] == symbol, "balance_symbol"] = self.rtctrl.df_rtctrl.loc[self.rtctrl.df_rtctrl['symbol'] == symbol, 'size'].iloc[0]
        except:
            self.df_balance_symbol.loc[self.df_balance_symbol['symbol'] == symbol, "balance_symbol"] = 0
        self.balance_usd = self.rtctrl.wallet_cash

        self.balance_symbol = self.df_balance_symbol.loc[self.df_balance_symbol['symbol'] == symbol, "balance_symbol"].iloc[0]

        if self.price_symbol * self.balance_symbol + self.balance_usd != 0:
            # price("BTC/USD")*balance("BTC")/(price("BTC/USD")*balance("BTC")+balance("USD"))
            self.df_symbol_current_pct.loc[self.df_symbol_current_pct['symbol'] == symbol, "symbol_current_pct"] = self.price_symbol * self.balance_symbol / (self.price_symbol * self.balance_symbol + self.balance_usd)
            # balance("USD")/(price("BTC/USD")*balance("BTC")+balance("USD"))
            self.usd_current_pct = self.balance_usd / (self.price_symbol * self.balance_symbol + self.balance_usd)
        else:
            self.symbol_current_pct = 0
            self.usd_current_pct = 0

        self.symbol_current_pct = self.df_symbol_current_pct.loc[self.df_symbol_current_pct['symbol'] == symbol, "symbol_current_pct"].iloc[0]
        self.symbol_pct = self.df_symbol_pct.loc[self.df_symbol_pct['symbol'] == symbol, "symbol_pct"].iloc[0]
        # if( get_variable("BTC_Current%") < get_variable("BTC%") - get_variable("rebalance%") )
        if self.symbol_current_pct < self.symbol_pct - self.rebalance_pct:
            buying_signal = True
        else:
            buying_signal = False

        return buying_signal

    def condition_for_selling(self, symbol, df_sl_tp):
        if self.tp_sl_abort:
            return True

        if len(self.df_selling_limits) == 0:
            self.set_df_multi()

        self.price_symbol = self.rtctrl.prices_symbols[symbol]
        try:
            self.df_balance_symbol.loc[self.df_balance_symbol['symbol'] == symbol, "balance_symbol"] = self.rtctrl.df_rtctrl.loc[self.rtctrl.df_rtctrl['symbol'] == symbol, 'size'].iloc[0]
        except:
            self.df_balance_symbol.loc[self.df_balance_symbol['symbol'] == symbol, "balance_symbol"] = 0

        self.usd_balance = self.rtctrl.wallet_cash
        self.balance_symbol = self.df_balance_symbol.loc[self.df_balance_symbol['symbol'] == symbol, "balance_symbol"].iloc[0]

        if self.price_symbol * self.balance_symbol + self.usd_balance != 0:
            # price("BTC/USD")*balance("BTC")/(price("BTC/USD")*balance("BTC")+balance("USD"))
            self.df_symbol_current_pct.loc[self.df_symbol_current_pct['symbol'] == symbol, "symbol_current_pct"] = self.price_symbol * self.balance_symbol / (self.price_symbol * self.balance_symbol + self.usd_balance)
            # balance("USD")/(price("BTC/USD")*balance("BTC")+balance("USD"))
            self.usd_current_pct = self.usd_balance / (self.price_symbol * self.balance_symbol + self.usd_balance)
        else:
            self.symbol_current_pct = 0
            self.usd_current_pct = 0

        self.symbol_current_pct = self.df_symbol_current_pct.loc[self.df_symbol_current_pct['symbol'] == symbol, "symbol_current_pct"].iloc[0]
        self.symbol_pct = self.df_symbol_pct.loc[self.df_symbol_pct['symbol'] == symbol, "symbol_pct"].iloc[0]
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
            print("global_tp: ", round(self.global_tp, 2), " net_tp: ", round(self.global_tp_net, 2), "protfolio: $",
                  self.rtctrl.wallet_value)

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

        self.balance_symbol = self.df_balance_symbol.loc[self.df_balance_symbol['symbol'] == symbol, "balance_symbol"].iloc[0]
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

    def reset_selling_limits(self):
        self.df_selling_limits["selling_limits"] = self.limit_sell

    def set_selling_limits(self, df_selling_symbols):
        for symbol in df_selling_symbols.symbol.tolist():
            self.df_selling_limits.loc[self.df_selling_limits['symbol'] == symbol, "selling_limits"] = 0

    def force_selling_limits(self):
        self.df_selling_limits["selling_limits"] = self.limit_sell + 1

    def count_selling_limits(self, symbol):
        self.df_selling_limits.loc[self.df_selling_limits['symbol'] == symbol, "selling_limits"] = \
            self.df_selling_limits.loc[self.df_selling_limits['symbol'] == symbol, "selling_limits"].iloc[0] + 1

    def get_selling_limit(self, symbol):
        if self.df_selling_limits.loc[self.df_selling_limits['symbol'] == symbol, "selling_limits"].iloc[0] < 1:
            return True
        else:
            return False

    def set_df_multi(self):
        self.df_balance_symbol['symbol'] = self.list_symbols
        self.df_balance_symbol['balance_symbol'] = 0

        self.df_selling_limits['symbol'] = self.list_symbols
        self.df_selling_limits['selling_limits'] = 0

        self.df_symbol_pct['symbol'] = self.list_symbols
        val_symbol_pct = (1 - self.usd_pct) / len(self.list_symbols)
        for symbol in self.list_symbols:
            self.df_symbol_pct.loc[self.df_symbol_pct['symbol'] == symbol, "symbol_pct"] = val_symbol_pct
        self.df_symbol_current_pct['symbol'] = self.list_symbols
        self.df_symbol_current_pct['symbol_current_pct'] = 0



