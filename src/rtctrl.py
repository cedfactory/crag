import pandas as pd
import requests
from datetime import datetime
from . import trade

# Class to real time control the strategy behaviours
class rtctrl():
    def __init__(self):
        self.df_rtctrl = pd.DataFrame(columns=self.get_df_header())
        self.df_rtctrl_tracking = pd.DataFrame(columns=self.get_df_header_tracking())
        self.df_rtctrl_symbol_price = pd.DataFrame(columns=self.get_df_header_symbol_price())
        self.symbol = []
        self.time = 0
        self.actual_price = 0
        self.size = 0
        self.fees = 0
        self.buying_gross_price = 0
        self.actual_net_price = 0
        self.roi_value = 0
        self.roi_percent = 0
        self.portfolio_value = 0
        self.wallet_cash = 0
        self.wallet_value = 0
        self.wallet_percent = 0
        self.screener_type = "crypto"
        self.exchange = "ftx"
        self.print_tracking = True
        self.record_tracking = True

    def get_df_header(self):
        return ["symbol", "time", "actual_price", "size", "fees", "buying_gross_price", "actual_net_price", "roi_$", "roi_%", "portfolio_value", "cash","wallet_value", "wallet_%"]

    def get_df_header_tracking(self):
        return ["time", " roi%", "cash", "portfolio", "wallet", "asset%"]

    def get_df_header_symbol_price(self):
        return ["symbol", "price"]

    def get_price_Direct_FTX(self, symbol):
        endpoint_url = 'https://ftx.com/api/markets'

        request_url = f'{endpoint_url}/{symbol}'
        df = pd.DataFrame(requests.get(request_url).json())

        return df['result']['price']

    def get_list_of_traded_symbols(self, list_of_current_trades):
        list_symbols = [trade.symbol for trade in list_of_current_trades if trade.type == "BUY"]
        return list(set(list_symbols))

    def get_list_of_actual_prices(self):
        list_actual_prices = []
        for symbol in self.symbol:
            list_actual_prices.append(self.get_price_Direct_FTX(symbol))
        return list_actual_prices

    def get_list_of_asset_size(self, list_of_current_trades):
        return [sum(current_trade.size for current_trade in list_of_current_trades if current_trade.type == "BUY" and current_trade.symbol == symbol) for symbol in self.symbol]

    def get_list_of_asset_fees(self, list_of_current_trades):
        return [sum(current_trade.buying_fee for current_trade in list_of_current_trades if current_trade.type == "BUY" and current_trade.symbol == symbol) for symbol in self.symbol]

    def get_list_of_asset_gross_price(self, list_of_current_trades):
        return [sum(current_trade.gross_price for current_trade in list_of_current_trades if current_trade.type == "BUY" and current_trade.symbol == symbol) for symbol in self.symbol]

    def update_rtctrl_price(self, list_symbols):
        self.df_rtctrl_symbol_price['symbol'] = list_symbols
        self.df_rtctrl_symbol_price.set_index('symbol', inplace=True)
        for symbol in list_symbols:
            self.df_rtctrl_symbol_price['price'][symbol] = self.get_price_Direct_FTX(symbol)

    def update_rtctrl(self, list_of_current_trades, wallet_cash):
        if len(list_of_current_trades) == 0:
            return

        self.df_rtctrl = pd.DataFrame(columns=self.get_df_header())

        self.symbol = self.get_list_of_traded_symbols(list_of_current_trades)
        self.time = datetime.now()
        self.actual_price = self.get_list_of_actual_prices()
        self.size = self.get_list_of_asset_size(list_of_current_trades)
        self.fees = self.get_list_of_asset_fees(list_of_current_trades)
        self.buying_gross_price = self.get_list_of_asset_gross_price(list_of_current_trades)
        self.wallet_cash = wallet_cash

        self.df_rtctrl['symbol'] = self.symbol
        self.df_rtctrl['time'] = self.time
        self.df_rtctrl['actual_price'] = self.actual_price
        self.df_rtctrl['size'] = self.size
        self.df_rtctrl['fees'] = self.fees
        self.df_rtctrl['buying_gross_price'] = self.buying_gross_price

        self.df_rtctrl['actual_net_price'] = self.df_rtctrl['size'] * self.df_rtctrl['actual_price']
        self.df_rtctrl['roi_$'] = self.df_rtctrl['actual_net_price'] - self.df_rtctrl['buying_gross_price']
        self.df_rtctrl['roi_%'] = self.df_rtctrl['roi_$'] / self.df_rtctrl['buying_gross_price']    # * 100 ????
        self.df_rtctrl['portfolio_value'] = self.df_rtctrl['actual_net_price']
        self.df_rtctrl['cash'] = self.wallet_cash
        self.df_rtctrl['wallet_value'] = self.df_rtctrl['portfolio_value'].sum() + self.df_rtctrl['cash']
        self.wallet_value = self.df_rtctrl['wallet_value'][0]
        self.df_rtctrl['wallet_%'] = 100 * self.df_rtctrl['portfolio_value'] / self.df_rtctrl['wallet_value']

        return


    def display_summary_info(self):
        if self.print_tracking:
            try:
                print(self.time,
                      " roi%: ", self.df_rtctrl['roi_$'].sum() / self.df_rtctrl['buying_gross_price'].sum(),
                      " cash: ", self.wallet_cash,
                      " portfolio: ", self.df_rtctrl['actual_net_price'].sum(),
                      " wallet: ", self.wallet_value,
                      " asset%: ", self.df_rtctrl['wallet_%'].sum())
            except:
                pass

        if self.record_tracking:
            try:
                df_new_line = pd.DataFrame([[self.time,
                                             self.df_rtctrl['roi_$'].sum() / self.df_rtctrl['buying_gross_price'].sum(),
                                             self.wallet_cash,
                                             self.df_rtctrl['actual_net_price'].sum(),
                                             self.wallet_value,
                                             self.df_rtctrl['wallet_%'].sum()]], columns=self.get_df_header_tracking())

                self.df_rtctrl_tracking = pd.concat([self.df_rtctrl_tracking, df_new_line])
                self.df_rtctrl_tracking.reset_index(inplace=True, drop=True)
                self.df_rtctrl_tracking.to_csv("wallet_tracking_records.csv")
            except:
                pass