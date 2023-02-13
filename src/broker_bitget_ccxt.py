from .bitget.mix import ccxt_bitget as prep

from . import broker_bitget
from dotenv import load_dotenv
import os
import pandas as pd

class BrokerBitGetCcxt(broker_bitget.BrokerBitGet):
    def __init__(self, params = None):
        super().__init__(params)

    def _authentification(self):
        load_dotenv()
        exchange_api_key = os.getenv(self.api_key)
        exchange_api_secret = os.getenv(self.api_secret)
        exchange_api_password = os.getenv(self.api_password)

        self.ccxtApi = prep.PerpBitget(exchange_api_key, exchange_api_secret,exchange_api_password)

        return self.ccxtApi != None

    def authentication_required(fn):
        """decoration for methods that require authentification"""
        def wrapped(self, *args, **kwargs):
            if not self._authentification():
                print("You must be authenticated to use this method {}".format(fn))
                return None
            else:
                return fn(self, *args, **kwargs)
        return wrapped

    def _get_symbol(self, coin, base = "USDT"):
        return coin+"/"+base+":"+base

    @authentication_required
    def get_open_position(self, symbol=""):
        open_position = self.ccxtApi.get_open_position(symbol)
        lst_open_position = [data for data in open_position if float(data["total"]) != 0.]
        return self._build_df_open_positions(lst_open_position)

    @authentication_required
    def _place_market_order(self, symbol, side, amount, reduce=False):
        symbol = self._get_symbol(symbol, "USDT")
        return self.ccxtApi.place_market_order(symbol, side, amount, reduce)

    @authentication_required
    def open_long_position(self, symbol, amount):
        return self._place_market_order(symbol, "buy", amount, reduce=False)

    @authentication_required
    def close_long_position(self, symbol, amount):
        return self._place_market_order(symbol, "sell", amount, reduce=True)

    @authentication_required
    def open_short_position(self, symbol, amount):
        return self._place_market_order(symbol, "sell", amount, reduce=False)

    @authentication_required
    def close_short_position(self, symbol, amount):
        return self._place_market_order(symbol, "buy", amount, reduce=True)

    @authentication_required
    def get_orders(self, symbol=None):
        return self.ccxtApi.get_orders(symbol)

    @authentication_required
    def get_usdt_equity(self):
        return self.ccxtApi.get_usdt_equity()

    @authentication_required
    def get_cash(self, baseCoin='USDT'):
        return self.get_usdt_equity()

    @authentication_required
    def get_balance(self):
        balance = self.ccxtApi.get_all_balance()
        #print(balance)
        info = balance["info"]
        df_balance = pd.DataFrame(columns=["symbol", "usdValue", "size"])
        for i in range(len(info)):
            data = info[i]
            df_balance.loc[i] = pd.Series({"symbol": data["marginCoin"], "usdValue": float(data["usdtEquity"]), "size": float(data["equity"])})
        return df_balance

    @authentication_required
    def get_order_history(self, symbol, startTime, endTime, pageSize):
        print("get_order_history : TODO")
        return None
        
    @authentication_required
    def get_value(self, symbol):
        value = self.ccxtApi.get_bid_ask_price(symbol)
        return value["ask"]

    @authentication_required
    def get_symbol_min_max_leverage(self, symbol):
        symbol = self._get_symbol(symbol, "USDT")
        leverage = self.ccxtApi._session.fetch_leverage(symbol)
        return leverage['data']['minLeverage'], leverage['data']['maxLeverage']

    @authentication_required
    def set_account_symbol_leverage(self, symbol, leverage):
        symbol = self._get_symbol(symbol, "USDT")
        leverage = self.ccxtApi._session.set_leverage(leverage, symbol)
        return leverage['data']['crossMarginLeverage'], leverage['data']['longLeverage'], leverage['data']['shortLeverage']

    @authentication_required
    def convert_amount_to_precision(self, symbol, amount):
        return self.ccxtApi.convert_amount_to_precision(symbol, amount)

    @authentication_required
    def convert_price_to_precision(self, symbol, price):
        return self.ccxtApi.convert_price_to_precision(symbol, price)

    @authentication_required
    def get_min_order_amount(self, symbol):
        return self.ccxtApi.get_min_order_amount(symbol)

    @authentication_required
    def export_history(self, target=None):
        return self.ccxtApi.export_history(target)

    @authentication_required
    def get_portfolio_value(self):
        return self.ccxtApi.get_portfolio_value()
