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
        if not self.authentification():
            print("[BrokerBitGet] : Problem encountered during authentification")

        self.df_market = self.get_future_market()
         
    def authentification(self):
        authentificated = False
        load_dotenv()
        exchange_api_key = os.getenv(self.api_key)
        exchange_api_secret = os.getenv(self.api_secret)
        exchange_api_password = os.getenv(self.api_password)

        self.marketApi = market.MarketApi(exchange_api_key, exchange_api_secret, exchange_api_password, use_server_time=False, first=False)
        self.accountApi = account.AccountApi(exchange_api_key, exchange_api_secret, exchange_api_password, use_server_time=False, first=False)
        self.positionApi = position.PositionApi(exchange_api_key, exchange_api_secret, exchange_api_password, use_server_time=False, first=False)
        self.orderApi = order.OrderApi(exchange_api_key, exchange_api_secret, exchange_api_password, use_server_time=False, first=False)
        self.ccxtApi = prep.PerpBitget(exchange_api_key, exchange_api_secret,exchange_api_password)

        return True

    def authentication_required(fn):
        """decoration for methods that require authentification"""
        def wrapped(self, *args, **kwargs):
            if not self.authentification():
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
    def get_value(self, symbol):
        return float(self.marketApi.market_price(symbol)["data"]["markPrice"])

    @authentication_required
    def get_commission(self, symbol):
        pass

    @authentication_required
    def execute_trade(self, trade):
        pass

    @authentication_required
    def export_history(self, target):
        pass

    def get_market_info_header(self):
        return ['symbol', 'quoteCoin', 'baseCoin', 'symbolType', 'makerFeeRate', 'takerFeeRate', 'minTradeNum']

    def get_account_info_header(self):
        return ['symbol', 'marginCoin', 'available', 'equity', 'usdtEquity', 'locked', 'btcEquity',
                       "size", "actualPrice", 'quoteCoin', 'baseCoin', 'symbolType',
                       'makerFeeRate', 'takerFeeRate', 'minTradeNum']

    def market_results_to_df(self, markets):
        lst_columns = self.get_market_info_header()
        df = pd.DataFrame(columns=lst_columns)
        for market in markets['data']:
            lst_info_symbol = [ market['symbol'],
                                market['quoteCoin'],
                                market['baseCoin'],
                                market['symbolType'],
                                float(market['makerFeeRate']),
                                float(market['takerFeeRate']),
                                float(market['minTradeNum'])
                                ]
            df.loc[len(df)] = lst_info_symbol
        return df

    def account_results_to_df(self, markets):
        lst_columns = self.get_account_info_header()
        df = pd.DataFrame(columns=lst_columns)
        for market in markets['data']:
            if float(market['equity']) > 0 \
                    and float(market['usdtEquity']) > 0 \
                    and float(market['btcEquity']) > 0:
                lst_info_symbol = [ "",
                                    market['marginCoin'],
                                    float(market['available']),
                                    float(market['equity']),
                                    float(market['usdtEquity']),
                                    float(market['locked']),
                                    float(market['btcEquity']),
                                    0, 0, "", "", "", 0, 0, 0]
                df.loc[len(df)] = lst_info_symbol
        return df.copy()

    def get_future_market(self):
        """
        productType
            umcbl USDT perpetual contract
            dmcbl Universal margin perpetual contract
            cmcbl USDC perpetual contract
            sumcbl USDT simulation perpetual contract
            sdmcbl Universal margin simulation perpetual contract
            scmcbl USDC simulation perpetual contract
        """
        dct_market = self.marketApi.contracts('umcbl')
        df_market_umcbl = self.market_results_to_df(dct_market)
        dct_market = self.marketApi.contracts('dmcbl')
        df_market_dmcbl = self.market_results_to_df(dct_market)
        dct_market = self.marketApi.contracts('cmcbl')
        df_market_cmcbl = self.market_results_to_df(dct_market)
        return pd.concat([df_market_umcbl, df_market_dmcbl, df_market_cmcbl]).reset_index()

    def _get_df_account(self):
        # update market
        dct_account = self.accountApi.accounts('umcbl')
        df_account_umcbl = self.account_results_to_df(dct_account)
        dct_account = self.accountApi.accounts('dmcbl')
        df_account_dmcbl =  self.account_results_to_df(dct_account)
        dct_account = self.accountApi.accounts('cmcbl')
        df_account_cmcbl = self.account_results_to_df(dct_account)
        self.df_account_assets = pd.concat([df_account_umcbl, df_account_dmcbl, df_account_cmcbl])
        self.df_account_assets.reset_index(inplace=True, drop=True)

    def _fill_df_account_from_market(self):
        for idx in self.df_account_assets.index.tolist():
            coin = self.df_account_assets.at[idx, 'marginCoin']
            if coin != "USDT" and coin != "USDC" and coin != "USD":
                self.df_account_assets.at[idx, 'symbol'] = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == 'USDT'), "symbol"].values[0]
                self.df_account_assets.at[idx, 'quoteCoin'] = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == 'USDT'), "quoteCoin"].values[0]
                self.df_account_assets.at[idx, 'baseCoin'] = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == 'USDT'), "baseCoin"].values[0]
                self.df_account_assets.at[idx, 'symbolType'] = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == 'USDT'), "symbolType"].values[0]
                self.df_account_assets.at[idx, 'makerFeeRate'] = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == 'USDT'), "makerFeeRate"].values[0]
                self.df_account_assets.at[idx, 'takerFeeRate'] = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == 'USDT'), "takerFeeRate"].values[0]
                self.df_account_assets.at[idx, 'minTradeNum'] = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == 'USDT'), "minTradeNum"].values[0]
            else:
                self.df_account_assets.at[idx, 'symbol'] = coin
                self.df_account_assets.at[idx, 'quoteCoin'] = coin
                self.df_account_assets.at[idx, 'baseCoin'] = coin
                self.df_account_assets.at[idx, 'size'] = self.df_account_assets.loc[self.df_account_assets["symbol"] == coin, "usdtEquity"].values[0]
                self.df_account_assets.at[idx, 'actualPrice'] = 1

    def _fill_price_and_size_from_bitget(self):
        for symbol in self.df_account_assets['symbol'].tolist():
            if self.df_account_assets.loc[self.df_account_assets['symbol'] == symbol, "symbolType"].values[0] == "perpetual":
                self.df_account_assets.loc[self.df_account_assets['symbol'] == symbol, "actualPrice"] = self.get_value(symbol)
                self.df_account_assets.loc[self.df_account_assets['symbol'] == symbol, "size"] = self.df_account_assets.loc[self.df_account_assets['symbol'] == symbol, "available"].values[0]
            else:
                pass

    def print_account_assets(self):
        print(self.df_account_assets)

    def get_list_of_account_assets(self):
        self._get_df_account()
        self._fill_df_account_from_market()
        self._fill_price_and_size_from_bitget()

    def get_symbol(self, coin, base):
        self.get_future_market()
        symbol = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == base), "symbol"].values[0]
        return symbol

    def get_symbol_ccxt(self, coin, base):
        return coin+"/"+base+":"+base

    @authentication_required
    def get_balance(self):
        self.get_list_of_account_assets()
        df_balance = pd.DataFrame(columns=['symbol', 'usdValue'])
        df_balance['symbol'] = self.df_account_assets['symbol']
        df_balance['usdValue'] = self.df_account_assets['usdtEquity']
        df_balance['size'] = self.df_account_assets['available']
        return df_balance

    @authentication_required
    def get_cash(self, baseCoin='USDT'):
        self.get_list_of_account_assets()
        return self.df_account_assets.loc[self.df_account_assets['symbol'] == baseCoin, "usdtEquity"].values[0]

    @authentication_required
    def get_usdt_equity(self):
        self.get_list_of_account_assets()
        return self.df_account_assets['usdtEquity'].sum()

    @authentication_required
    def get_usdt_equity_ccxt(self):
        return self.ccxtApi.get_usdt_equity()

    @authentication_required
    def get_account_asset(self):
        result = self.accountApi.accountAssets(productType='umcbl')
        return result

    '''
    marginCoin: Deposit currency
    size: It is quantity when the price is limited. The market price is the limit. The sales is the quantity
    side \in {open_long, open_short, close_long, close_short}
    orderType \in {limit(fixed price), market(market price)}

    !!!!!!! doesn't work. TODO : check why ?
    symbol = my_broker.get_symbol('ETH', 'USDT')
    marginCoin = 'USDT'
    order = my_broker.place_order_api(symbol, marginCoin=marginCoin, size=0.01, side='close_long', orderType='market')
    print(order)
    cancelStatus = my_broker.cancel_order(symbol, marginCoin, orderId)
    '''
    @authentication_required
    def place_order_api(self, symbol, marginCoin, size, side, orderType):
        order = self.orderApi.place_order(symbol, marginCoin, size, side, orderType,
                                         price='',
                                         clientOrderId='', timeInForceValue='normal',
                                         presetTakeProfitPrice='', presetStopLossPrice='')
        if order['msg'] == 'success':
            return order['data']['orderId'], order['data']['clientOid'], order['requestTime']
        else:
            return order['msg']

    @authentication_required
    def get_order_current(self, symbol):
        current = self.orderApi.current(symbol)
        return current

    @authentication_required
    def get_order_history(self, symbol, startTime, endTime, pageSize):
        history = self.orderApi.history(symbol, startTime, endTime, pageSize, lastEndId='', isPre=False)
        df_history = pd.DataFrame(columns=["symbol", "size", "side", "orderId", "filledQty", "leverage", "fee", "orderType", "marginCoin", "totalProfits", "cTime", "uTime"])
        orderList = history["data"]["orderList"]
        for i in range(len(orderList)):
            data = orderList[i]
            df_history.loc[i] = pd.Series({"symbol": data["symbol"], "size": data["size"], "side": data["side"], "orderId": data["orderId"], "filledQty": data["filledQty"], "leverage": int(data["leverage"]),"fee": float(data["fee"]),"orderType": data["orderType"],"marginCoin": data["marginCoin"],"totalProfits": float(data["totalProfits"]),"cTime": utils.convert_ms_to_datetime(data["cTime"]),"uTime": utils.convert_ms_to_datetime(data["uTime"])})
        return df_history

        return history

    @authentication_required
    def cancel_order(self, symbol, marginCoin, orderId):
        result = self.orderApi.cancel_orders(symbol, marginCoin, orderId)
        return result

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
    def get_open_position_ccxt(self, symbol=''):
        open_position = self.ccxtApi.get_open_position(symbol)
        lst_open_position = [data for data in open_position if float(data["total"]) != 0.]
        return self._build_df_open_positions(lst_open_position)

    @authentication_required
    def get_open_position(self):
        all_positions = self.positionApi.all_position(productType='umcbl',marginCoin='USDT')
        lst_all_positions = [data for data in all_positions["data"] if float(data["total"]) != 0.]
        return self._build_df_open_positions(lst_all_positions)
    
    '''
    @authentication_required
    def get_positions(self):
        return self.ccxtApi.get_positions()
    '''

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
    def place_market_order_ccxt(self, symbol, side, amount, reduce=False):
        return self.ccxtApi.place_market_order(symbol, side, amount, reduce)

    @authentication_required
    def open_long_position(self, symbol, amount):
        return self.ccxtApi.place_market_order(symbol, "buy", amount, reduce=False)

    @authentication_required
    def close_long_position(self, symbol, amount):
        return self.ccxtApi.place_market_order(symbol, "sell", amount, reduce=True)

    @authentication_required
    def open_short_position(self, symbol, amount):
        return self.ccxtApi.place_market_order(symbol, "sell", amount, reduce=False)

    @authentication_required
    def close_short_position(self, symbol, amount):
        return self.ccxtApi.place_market_order(symbol, "buy", amount, reduce=True)

    @authentication_required
    def get_orders_ccxt(self, symbol=None):
        return self.ccxtApi.get_orders(symbol)

    @authentication_required
    def export_history(self, target=None):
        return self.ccxtApi.export_history(target)

    @authentication_required
    def get_portfolio_value(self):
        return self.ccxtApi.get_portfolio_value()