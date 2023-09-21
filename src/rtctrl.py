import pandas as pd
import numpy as np
import os
import requests
from datetime import datetime

# Class to real time control the strategy behaviours
class rtctrl():
    def __init__(self, current_datetime=None, params=None):
        self.df_rtctrl = pd.DataFrame(columns=self.get_df_header())
        self.df_rtctrl_tracking = pd.DataFrame(columns=self.get_df_header_tracking())
        self.symbols = []
        self.prices_symbols = {}
        self.time = current_datetime
        self.init_cash_value = 0
        self.actual_price = 0
        self.actual_net_price = 0
        self.global_roi_value = 0
        self.global_roi_percent = 0
        self.portfolio_value = 0
        self.wallet_cash = 0
        self.wallet_value = 0
        self.wallet_percent = 0
        self.screener_type = "crypto"
        self.exchange = "binance"
        self.verbose = False
        self.suffix = ""
        self.working_directory = ""
        self.export_filename = ""
        if params:
            self.verbose = params.get("rtctrl_verbose", self.verbose)
            self.suffix = params.get("suffix", self.suffix)
            self.working_directory = params.get("working_directory", self.working_directory)
            if self.working_directory == "":
                self.working_directory = './output/'  # CEDE NOTE: output dir as default value
            self.export_filename = os.path.join("./" + self.working_directory, "wallet_tracking_records" + self.suffix + ".csv")
        self.record_tracking = False

    def get_df_header(self):
        return ["symbol", "time", "actual_price", "size", "fees", "buying_gross_price", "actual_net_price", "roi_$", "roi_%", "portfolio_value", "cash", 'cash_borrowed',"wallet_value", "wallet_%", "unrealizedPL", "wallet_unrealizedPL"]

    def get_df_header_tracking(self):
        return ["time", "roi%", "cash", "portfolio", "wallet", "asset%"]

    def get_list_of_traded_symbols(self, list_of_current_trades):
        list_symbols = [trade.symbol for trade in list_of_current_trades if trade.type in self.lst_opening_type]
        return list(set(list_symbols))

    def get_list_of_asset_size(self, list_of_current_trades):
        return [sum(current_trade.net_size for current_trade in list_of_current_trades if current_trade.type in self.lst_opening_type and current_trade.symbol == symbol) for symbol in self.symbols]

    def get_list_of_asset_fees(self, list_of_current_trades):
        return [sum(current_trade.buying_fee for current_trade in list_of_current_trades if current_trade.type in self.lst_opening_type and current_trade.symbol == symbol) for symbol in self.symbols]

    def get_asset_fees(self, list_of_current_trades, symbol):
        return sum(current_trade.buying_fee for current_trade in list_of_current_trades if current_trade.type in self.lst_opening_type and current_trade.symbol == symbol)

    def get_list_of_asset_gross_price(self, list_of_current_trades):
        return [sum(current_trade.gross_price for current_trade in list_of_current_trades if current_trade.type in self.lst_opening_type and current_trade.symbol == symbol) for symbol in self.symbols]

    def get_asset_gross_price(self, list_of_current_trades, symbol):
        return sum(current_trade.gross_price for current_trade in list_of_current_trades if current_trade.type in self.lst_opening_type and current_trade.symbol == symbol)

    def update_rtctrl(self, current_datetime, list_of_current_trades, wallet_cash, wallet_cash_borrowed, prices_symbols, final_date, df_balance):
        NoneType = type(None)
        if isinstance(df_balance, NoneType):
            self.update_rtctrl_backtest(current_datetime, list_of_current_trades, wallet_cash, wallet_cash_borrowed, prices_symbols, final_date)
        else:
            self.update_rtctrl_live(current_datetime, list_of_current_trades, wallet_cash, prices_symbols, df_balance)

    def update_rtctrl_live(self, current_datetime, list_of_current_trades, wallet_cash, prices_symbols, df_balance):
        self.time = current_datetime
        self.wallet_cash_borrowed = 0

        self.prices_symbols = prices_symbols

        self.cash_available = float(df_balance.loc[df_balance['symbol'] == 'USDT', 'available'].values[0])

        df_balance_assets = df_balance.copy()
        indexUSDT = df_balance_assets[(df_balance_assets['symbol'] == 'USDT')].index
        df_balance_assets.drop(indexUSDT, inplace=True)
        df_balance_assets.reset_index(inplace=True, drop=True)

        self.df_rtctrl = self.df_rtctrl[0: 0] # self.df_rtctrl = self.df_rtctr.head(0)
        self.df_roi_sl_tp = self.df_rtctrl.copy()

        if len(list_of_current_trades) == 0:
            if self.init_cash_value == 0:
                self.init_cash_value = wallet_cash
                self.wallet_cash = wallet_cash
                self.wallet_value = df_balance['usdtEquity'].sum()
            else:
                self.wallet_cash = wallet_cash
                self.wallet_value = df_balance['usdtEquity'].sum()
            self.df_roi_sl_tp.set_index('symbol', inplace=True)
            self.df_roi_sl_tp.rename(columns={'roi_%': 'roi_sl_tp'}, inplace=True)
            return

        if len(list_of_current_trades) != 0 and self.init_cash_value == 0:
            self.wallet_value = df_balance.at[indexUSDT[0], 'usdtEquity']
            self.init_cash_value = self.wallet_value
            self.wallet_cash = wallet_cash

        self.symbols = self.get_list_of_traded_symbols(list_of_current_trades)
        lst_symbols_from_balance = df_balance_assets['symbol'].tolist()   # self.symbols = df_balances['baseCoin'].tolist() ???
        lst_symbols_from_current_trade = self.symbols
        lst_symbols_from_current_trade.sort()
        lst_symbols_from_balance.sort()
        if lst_symbols_from_current_trade != lst_symbols_from_balance:
            print('================================================================')
            print('error list of symbols/current_trades not matching')
            print('list from current_trade: ', lst_symbols_from_current_trade)
            print('list from account balance: ', lst_symbols_from_balance)
            print('================================================================')

        # actual_prices = [prices_symbols[symbol] for symbol in self.symbols]
        actual_prices = [prices_symbols[symbol] for symbol in df_balance_assets['symbol'].tolist()]
        self.actual_price = actual_prices
        self.wallet_cash = wallet_cash

        self.df_rtctrl['symbol'] = df_balance_assets['symbol'].tolist()
        self.df_rtctrl['time'] = self.time
        self.df_rtctrl['actual_price'] = df_balance_assets['actualPrice'].tolist()
        self.df_rtctrl['size'] = df_balance_assets['size'].tolist()
        df_balance_assets['price * size'] = df_balance_assets['size'] * df_balance_assets['averageOpenPrice']
        self.df_rtctrl['buying_gross_price'] = df_balance_assets['price * size'].tolist()
        self.df_rtctrl['actual_net_price'] = self.df_rtctrl['size'] * self.df_rtctrl['actual_price']

        for symbol in self.df_rtctrl['symbol'].tolist():
            idx = df_balance_assets[(df_balance_assets['symbol'] == symbol)].index
            self.df_rtctrl.at[idx[0], 'fees'] = self.get_asset_fees(list_of_current_trades, symbol)

        self.df_rtctrl['roi_$'] = df_balance_assets['unrealizedPL']
        self.df_rtctrl['roi_%'] = self.df_rtctrl['roi_$'] / self.df_rtctrl['buying_gross_price'] * 100

        self.df_rtctrl['portfolio_value'] = df_balance_assets['usdtEquity']   # CEDE ???????????? could be net_price
        self.df_rtctrl['cash'] = self.wallet_cash
        self.df_rtctrl['cash_borrowed'] = self.wallet_cash_borrowed
        # self.df_rtctrl['wallet_value'] = df_balance['usdtEquity'].sum()
        self.df_rtctrl['wallet_value'] = df_balance.loc[df_balance['symbol'] == 'USDT', 'usdtEquity'].values[0]

        self.df_roi_sl_tp = self.df_rtctrl.copy()
        self.df_roi_sl_tp.set_index('symbol', inplace=True)
        self.df_roi_sl_tp.rename(columns={'roi_%' : 'roi_sl_tp'}, inplace=True)

    def update_rtctrl_backtest(self, current_datetime, list_of_current_trades, wallet_cash, wallet_cash_borrowed, prices_symbols, final_date):
        self.time = current_datetime

        if current_datetime == final_date:
            final_step = True
        else:
            final_step = False

        self.df_rtctrl = self.df_rtctrl[0: 0] # self.df_rtctrl = self.df_rtctr.head(0)
        self.df_roi_sl_tp = self.df_rtctrl.copy()

        if len(list_of_current_trades) == 0 and (not final_step):
            if self.init_cash_value == 0:
                self.init_cash_value = wallet_cash
                self.wallet_cash = wallet_cash
                self.wallet_value = wallet_cash
                self.wallet_cash_borrowed = wallet_cash_borrowed
            else:
                self.wallet_cash = wallet_cash
                self.wallet_value = wallet_cash
                self.wallet_cash_borrowed = wallet_cash_borrowed
                self.df_roi_sl_tp.set_index('symbol', inplace=True)
                self.df_roi_sl_tp.rename(columns={'roi_%': 'roi_sl_tp'}, inplace=True)
            return

        self.symbols = self.get_list_of_traded_symbols(list_of_current_trades)
        actual_prices = [prices_symbols[symbol] for symbol in self.symbols]
        self.actual_price = actual_prices
        self.wallet_cash = wallet_cash
        self.wallet_cash_borrowed = wallet_cash_borrowed

        self.df_rtctrl['symbol'] = self.symbols
        self.df_rtctrl['time'] = self.time
        self.df_rtctrl['actual_price'] = self.actual_price
        self.df_rtctrl['size'] = self.get_list_of_asset_size(list_of_current_trades)
        self.df_rtctrl['fees'] = self.get_list_of_asset_fees(list_of_current_trades)
        self.df_rtctrl['buying_gross_price'] = self.get_list_of_asset_gross_price(list_of_current_trades)
        self.df_rtctrl['actual_net_price'] = self.df_rtctrl['size'] * self.df_rtctrl['actual_price']
        self.df_rtctrl['roi_$'] = self.df_rtctrl['actual_net_price'] - self.df_rtctrl['buying_gross_price']
        self.df_rtctrl['roi_%'] = self.df_rtctrl['roi_$'] / self.df_rtctrl['buying_gross_price']    # * 100 ????
        self.df_rtctrl['portfolio_value'] = self.df_rtctrl['actual_net_price']
        self.df_rtctrl['cash'] = self.wallet_cash
        self.df_rtctrl['cash_borrowed'] = self.wallet_cash_borrowed
        self.df_rtctrl['wallet_value'] = self.df_rtctrl['portfolio_value'].sum() + self.df_rtctrl['cash']
        if len(self.df_rtctrl) > 0:
            self.wallet_value = self.df_rtctrl['wallet_value'][0]
            self.portfolio_value = self.df_rtctrl['portfolio_value'].sum()
        else:
            self.wallet_value = self.wallet_cash
            self.portfolio_value = 0
        self.df_rtctrl['wallet_%'] = np.where(self.df_rtctrl['wallet_value'] != 0, 100 * self.df_rtctrl['portfolio_value'] / self.df_rtctrl['wallet_value'], 0.)

        self.global_roi_value = self.wallet_value - self.init_cash_value
        self.global_roi_percent = self.global_roi_value / self.init_cash_value * 100

        self.df_roi_sl_tp = self.df_rtctrl.copy()
        self.df_roi_sl_tp.set_index('symbol', inplace=True)
        self.df_roi_sl_tp.rename(columns={'roi_%' : 'roi_sl_tp'}, inplace=True)

    def display_summary_info(self, record_info=None):
        if not (self.record_tracking or self.verbose):
            return

        wallet_cash = self.wallet_cash
        portfolio = self.df_rtctrl['actual_net_price'].sum()
        wallet_value = self.wallet_value
        if self.init_cash_value != 0:
            roi_percent = (self.wallet_value - self.init_cash_value) * 100 / self.init_cash_value
        else:
            roi_percent = 0
        asset_percent = self.df_rtctrl['wallet_%'].sum()

        summary = "{} roi: {:.2f}% cash: $ {:.2f} portfolio: {:.2f} wallet: $ {:.2f} asset: {:.2f}%".format(self.time, roi_percent, wallet_cash, portfolio, wallet_value, asset_percent)
        if self.verbose:
            print(self.df_rtctrl)
            print(summary)

        if self.record_tracking:
            df_new_line = pd.DataFrame([[self.time, roi_percent, wallet_cash, portfolio, wallet_value, asset_percent]], columns=self.get_df_header_tracking())

            self.df_rtctrl_tracking = pd.concat([self.df_rtctrl_tracking, df_new_line])
            self.df_rtctrl_tracking.reset_index(inplace=True, drop=True)

            if record_info and self.export_filename != None and self.export_filename != "":
                if self.df_rtctrl_tracking['time'][1] and self.df_rtctrl_tracking['time'][0]:
                    interval = self.df_rtctrl_tracking['time'][1] - self.df_rtctrl_tracking['time'][0]
                    self.df_rtctrl_tracking['time'][len(self.df_rtctrl_tracking)-1] = self.df_rtctrl_tracking['time'][len(self.df_rtctrl_tracking)-1] + interval

                    self.df_rtctrl_tracking.drop(index=self.df_rtctrl_tracking.index[-2], axis=0, inplace=True)
                    self.df_rtctrl_tracking.reset_index(drop=True, inplace=True)

                    self.df_rtctrl_tracking.to_csv(self.export_filename)
                else:
                    print("!!! [rtctrl] self.df_rtctrl_tracking expecting values in time column")
                    print(self.df_rtctrl_tracking)

    def set_list_open_position_type(self, lst_opening_type):
        self.lst_opening_type = lst_opening_type

    def set_list_close_position_type(self, lst_closing_type):
        self.lst_closing_type = lst_closing_type

    def get_rtctrl_wallet_value(self):
        # Cash + Assets values
        return self.wallet_value

    def get_rtctrl_portfolio_value(self):
        # Assets values
        return self.portfolio_value

    def get_rtctrl_nb_symbols(self):
        return len(self.df_rtctrl.index)

    def get_rtctrl_lst_symbols(self):
        return self.df_rtctrl['symbol'].to_list()

    def get_rtctrl_lst_values(self):
        return self.df_rtctrl['actual_net_price'].to_list()

    def get_rtctrl_lst_roi_percent(self):
        return self.df_rtctrl['roi_%'].to_list()

    def get_rtctrl_lst_roi_dol(self):
        return self.df_rtctrl['roi_$'].to_list()

    def get_rtctrl_df_roi_sl_tp(self):
        return self.df_roi_sl_tp