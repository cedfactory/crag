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
        self.roi_value = 0
        self.roi_percent = 0
        self.portfolio_value = 0
        self.wallet_cash = 0
        self.wallet_value = 0
        self.wallet_percent = 0
        self.screener_type = "crypto"
        self.exchange = "ftx"
        self.verbose = False
        self.suffix = ""
        self.working_directory = ""
        self.export_filename = ""
        if params:
            self.verbose = params.get("rtctrl_verbose", self.verbose)
            self.suffix = params.get("suffix", self.suffix)
            self.working_directory = params.get("working_directory", self.working_directory)
            self.export_filename = os.path.join(self.working_directory, "./" + "wallet_tracking_records" + self.suffix + ".csv")
        self.record_tracking = True

    def get_df_header(self):
        return ["symbol", "time", "actual_price", "size", "fees", "buying_gross_price", "actual_net_price", "roi_$", "roi_%", "portfolio_value", "cash","wallet_value", "wallet_%"]

    def get_df_header_tracking(self):
        return ["time", "roi%", "cash", "portfolio", "wallet", "asset%"]

    def get_list_of_traded_symbols(self, list_of_current_trades):
        list_symbols = [trade.symbol for trade in list_of_current_trades if trade.type == "BUY"]
        return list(set(list_symbols))

    def get_list_of_asset_size(self, list_of_current_trades):
        return [sum(current_trade.net_size for current_trade in list_of_current_trades if current_trade.type == "BUY" and current_trade.symbol == symbol) for symbol in self.symbols]

    def get_list_of_asset_fees(self, list_of_current_trades):
        return [sum(current_trade.buying_fee for current_trade in list_of_current_trades if current_trade.type == "BUY" and current_trade.symbol == symbol) for symbol in self.symbols]

    def get_list_of_asset_gross_price(self, list_of_current_trades):
        return [sum(current_trade.gross_price for current_trade in list_of_current_trades if current_trade.type == "BUY" and current_trade.symbol == symbol) for symbol in self.symbols]

    def update_rtctrl(self, current_datetime, list_of_current_trades, wallet_cash, prices_symbols, final_date):
        self.prices_symbols = prices_symbols
        self.time = current_datetime

        if current_datetime == final_date:
            final_step = True
        else:
            final_step = False

        if len(list_of_current_trades) == 0 and (not final_step):
            if self.init_cash_value == 0:
                self.init_cash_value = wallet_cash
                self.wallet_cash = wallet_cash
                self.wallet_value = wallet_cash
            return

        self.df_rtctrl = pd.DataFrame(columns=self.get_df_header())
        self.symbols = self.get_list_of_traded_symbols(list_of_current_trades)
        actual_prices = [prices_symbols[symbol] for symbol in self.symbols]
        self.actual_price = actual_prices
        self.wallet_cash = wallet_cash

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
        self.df_rtctrl['wallet_value'] = self.df_rtctrl['portfolio_value'].sum() + self.df_rtctrl['cash']
        if len(self.df_rtctrl) > 0:
            self.wallet_value = self.df_rtctrl['wallet_value'][0]
        else:
            self.wallet_value = self.wallet_cash
        self.df_rtctrl['wallet_%'] = np.where(self.df_rtctrl['wallet_value'] != 0, 100 * self.df_rtctrl['portfolio_value'] / self.df_rtctrl['wallet_value'], 0.)

    def display_summary_info(self, record_info=None):
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
                interval = self.df_rtctrl_tracking['time'][1] - self.df_rtctrl_tracking['time'][0]
                self.df_rtctrl_tracking['time'][len(self.df_rtctrl_tracking)-1] = self.df_rtctrl_tracking['time'][len(self.df_rtctrl_tracking)-1] + interval

                self.df_rtctrl_tracking.drop(index=self.df_rtctrl_tracking.index[-2], axis=0, inplace=True)
                self.df_rtctrl_tracking.reset_index(drop=True, inplace=True)

                self.df_rtctrl_tracking.to_csv(self.export_filename)
        
        return summary
