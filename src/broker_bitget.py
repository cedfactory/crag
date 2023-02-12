from .bitget.mix import market_api as market
from .bitget.mix import account_api as account
from .bitget.mix import position_api as position
from .bitget.mix import order_api as order
from .bitget.mix import ccxt_bitget as prep

from . import broker,rtdp,utils
from dotenv import load_dotenv
import os
import pandas as pd

class BrokerBitGet(broker.Broker):
    def __init__(self, params = None):
        self.rtdp = rtdp.RealTimeDataProvider(params)
        self.trades = []
        self.simulation = False
        self.account = ""
        self.leverage = 0
        self.name = ""
        self.exchange_name = "bitget"
        self.api_key = "BITGET_API_KEY"
        self.api_secret = "BITGET_API_SECRET"
        self.api_password = "BITGET_API_PASSWORD"
        self.chase_limit = False
        if params:
            self.simulation = params.get("simulation", self.simulation)
            if self.simulation == 0 or self.simulation == "0":
                self.simulation = False
            if self.simulation == 1 or self.simulation == "1":
                self.simulation = True
            self.account = params.get("account", self.account)
            self.leverage = params.get("leverage", self.leverage)
            if isinstance(self.leverage, str):
                self.leverage = int(self.leverage)
        if not self._authentification():
            print("[BrokerBitGet] : Problem encountered during authentification")

    def authentication_required(fn):
        """decoration for methods that require authentification"""
        def wrapped(self, *args, **kwargs):
            if not self._authentification():
                print("You must be authenticated to use this method {}".format(fn))
                return None
            else:
                return fn(self, *args, **kwargs)
        return wrapped

    def ready(self):
        return self.marketApi != None and self.accountApi != None

    def log_info(self):
        info = ""
        info += "{}".format(type(self).__name__)
        info += "\nCash : $ {}".format(utils.KeepNDecimals(self.get_cash(), 2))
        info += "\nLeverage : {}".format(self.leverage)
        return info



    @authentication_required
    def get_commission(self, symbol):
        pass

    @authentication_required
    def execute_trade(self, trade):
        print("!!!!!!! EXECUTE THE TRADE !!!!!!!")
        if self.exchange:
            symbol = trade.symbol
            amount = trade.gross_size
            # type = "market" # CEDE NOT USED
            if trade.type == "OPEN_LONG":
                side = "buy"
                reduce = False
                if amount > self.get_min_order_amount(symbol):
                    trade.orderId = self.place_market_order_ccxt(symbol, side, trade.gross_size, reduce)
                    trade.tradeId, trade.symbol_price, trade.net_size, trade.buying_fee = self.get_order_fill_detail(symbol, trade.orderId)
                    trade.net_price = trade.net_size * trade.symbol_price
                    # CEDE: Option 1&2 should have the same value - Keep the more accurate
                    trade.bought_gross_price = trade.gross_size * trade.symbol_price # CEDE: Option 1
                    trade.bought_gross_price = trade.net_price + trade.buying_fee # CEDE: Option 2
                    self.cash = self.get_cash()
                else:
                    return False
            elif trade.type == "OPEN_SHORT":
                side = "sell"
                reduce = False
                if amount > self.get_min_order_amount(symbol):
                    trade.orderId = self.place_market_order_ccxt(symbol, side, trade.gross_size, reduce)
                    trade.tradeId, trade.symbol_price, trade.net_size, trade.buying_fee = self.get_order_fill_detail(symbol, trade.orderId)
                    trade.net_price = trade.net_size * trade.symbol_price
                    # CEDE: Option 1&2 should have the same value - Keep the more accurate
                    trade.bought_gross_price = trade.gross_size * trade.symbol_price # CEDE: Option 1
                    trade.bought_gross_price = trade.net_price + trade.buying_fee # CEDE: Option 2
                    self.cash = self.get_cash()
                    self.cash_borrowed = self.cash_borrowed + abs(trade.net_price) # CEDE: cash_borrowed to be investigated
                else:
                    return False
            elif trade.type == "CLOSE_LONG":
                result, orderId = self.cancel_order(symbol, 'USDT', trade.orderId) # Possibility to use place_market_order_ccxt instead
                if result:
                    tradeId, trade.symbol_price, trade.net_size, order_fee = self.get_order_fill_detail(symbol, orderId) # Is it possible to get info from a canceled order? to be tested
                    trade.net_price = trade.net_size * trade.symbol_price
                    # CEDE: Solution 1
                    trade.gross_price = trade.gross_size * trade.symbol_price
                    trade.selling_fee = trade.gross_price - trade.net_price
                    # CEDE: Solution 2
                    trade.selling_fee = order_fee # CEDE: to be verified if retruned order_fee include buying_fee + selling_fee (cancelation) or just the selling_fee
                    trade.gross_price = trade.net_size + trade.selling_fee
                    trade.roi = 100 * (trade.net_price - trade.bought_gross_price) / trade.bought_gross_price
                    self.cash = self.get_cash()
                else:
                    return False
            elif trade.type == "CLOSE_SHORT":
                result, orderId = self.cancel_order(symbol, 'USDT', trade.orderId) # Possibility to use place_market_order_ccxt instead
                if result:
                    tradeId, trade.symbol_price, trade.net_size, order_fee = self.get_order_fill_detail(symbol, orderId) # Is it possible to get info from a canceled order? to be tested
                    trade.net_price = trade.net_size * trade.symbol_price
                    # CEDE: Solution 1
                    trade.gross_price = trade.gross_size * trade.symbol_price
                    trade.selling_fee = trade.gross_price - trade.net_price
                    # CEDE: Solution 2
                    trade.selling_fee = order_fee # CEDE: to be verified if retruned order_fee include buying_fee + selling_fee (cancelation) or just the selling_fee
                    trade.gross_price = trade.net_size + trade.selling_fee
                    trade.roi = 100 * (trade.bought_gross_price - trade.net_price) / trade.bought_gross_price
                    self.cash_borrowed = self.cash_borrowed - abs(trade.cash_borrowed) # CEDE: cash_borrowed to be investigated
                else:
                    return False
            self.trades.append(trade)
            return True

            # CEDE: PREVIOUS CODE FROM broker_ccxt
            # INCLUDING _chase_limit NOT IMPLEMENTED FOR BITGET
            order = None
            if self.simulation:
                return order

            print("!!!!!!! EXECUTE THE TRADE !!!!!!!")
            if self.exchange:
                side = ""
                type = self.orders
                if trade.type == "SELL":
                    type = "market"
                    side = "sell"
                elif trade.type == "BUY":
                    type = "market"
                    side = "buy"

                # limit orders :
                # https://github.com/ccxt/ccxt/wiki/Manual
                elif trade.type == "LIMIT_SELL":
                    type = "limit"
                    side = "sell"
                elif trade.type == "LIMIT_BUY":
                    type = "limit"
                    side = "buy"
                print("side : ", side)
                print("type : ", type)
                if side == "":
                    return order

                symbol = trade.symbol
                amount = trade.net_price / trade.symbol_price

                if type == "market" and self.chase_limit:
                    return self._chase_limit(symbol, amount)

                try:
                    order = self.exchange.create_order(symbol, type, side, amount)
                except BaseException as err:
                    print("[BrokerCCXT::execute_trade] An error occured : {}".format(err))
                    print("[BrokerCCXT::execute_trade]   -> symbol : {}".format(symbol))
                    print("[BrokerCCXT::execute_trade]   -> type :   {}".format(type))
                    print("[BrokerCCXT::execute_trade]   -> side :   {}".format(side))
                    print("[BrokerCCXT::execute_trade]   -> amount : {}".format(amount))

            return order

    @authentication_required
    def export_history(self, target):
        pass

    '''
    def get_symbol(self, coin, base):
        self.get_future_market()
        symbol = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == base), "symbol"].values[0]
        return symbol
    '''

    @authentication_required
    def get_account_asset(self):
        result = self.accountApi.accountAssets(productType='umcbl')
        return result

    @authentication_required
    def get_order_current(self, symbol):
        current = self.orderApi.current(symbol)
        return current

    @authentication_required
    def cancel_order(self, symbol, marginCoin, orderId):
        result = self.orderApi.cancel_orders(symbol, marginCoin, orderId)
        if result['msg'] == "success":
            return True, result['data']['orderId']
        else:
            return False , False

    def get_order_fill_detail(self, symbol, order_id):
        transaction_id, transaction_price,  transaction_size,  transaction_fee = self.orderApi.get_order_fill_detail(symbol, order_id)
        return transaction_id, transaction_price,  transaction_size,  transaction_fee

    @authentication_required
    def get_symbol_min_max_leverage(self, symbol):
        leverage = self.marketApi.get_symbol_leverage(symbol)
        return leverage['data']['minLeverage'], leverage['data']['maxLeverage']

    @authentication_required
    def get_account_symbol_leverage(self, symbol, marginCoin='USDT'):
        dct_account = self.accountApi.account(symbol, marginCoin)
        return dct_account['data']['crossMarginLeverage'], dct_account['data']['fixedLongLeverage'], dct_account['data']['fixedShortLeverage']

    @authentication_required
    def set_account_symbol_leverage(self, symbol, leverage):
        dct_account = self.accountApi.leverage(symbol, 'USDT', leverage)
        return dct_account['data']['crossMarginLeverage'], dct_account['data']['longLeverage'], dct_account['data']['shortLeverage']

    def _build_df_open_positions(self, open_positions):
        df_open_positions = pd.DataFrame(columns=["symbol", "holdSide", "leverage", "marginCoin", "available", "total", "marketPrice"])
        for i in range(len(open_positions)):
            data = open_positions[i]
            df_open_positions.loc[i] = pd.Series({"symbol": data["symbol"], "holdSide": data["holdSide"], "leverage": data["leverage"], "marginCoin": data["marginCoin"],"available": float(data["available"]),"total": float(data["total"]),"marketPrice": data["marketPrice"]})
        return df_open_positions

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
