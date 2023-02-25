from .bitget.mix import market_api as market
from .bitget.mix import account_api as account
from .bitget.mix import position_api as position
from .bitget.mix import order_api as order
from .bitget.spot import public_api as public

from . import broker_bitget
from . import utils
from dotenv import load_dotenv
import os
import pandas as pd

class BrokerBitGetApi(broker_bitget.BrokerBitGet):
    def __init__(self, params = None):
        super().__init__(params)
        
        self.df_market = self.get_future_market()

    def _authentification(self):
        load_dotenv()
        exchange_api_key = os.getenv(self.api_key)
        exchange_api_secret = os.getenv(self.api_secret)
        exchange_api_password = os.getenv(self.api_password)

        self.marketApi = market.MarketApi(exchange_api_key, exchange_api_secret, exchange_api_password, use_server_time=False, first=False)
        self.accountApi = account.AccountApi(exchange_api_key, exchange_api_secret, exchange_api_password, use_server_time=False, first=False)
        self.positionApi = position.PositionApi(exchange_api_key, exchange_api_secret, exchange_api_password, use_server_time=False, first=False)
        self.orderApi = order.OrderApi(exchange_api_key, exchange_api_secret, exchange_api_password, use_server_time=False, first=False)
        self.publicApi = public.PublicApi(exchange_api_key, exchange_api_secret, exchange_api_password, use_server_time=False, first=False)

        return self.marketApi != None

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
        # self.get_future_market()
        symbol = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == base), "symbol"].values[0]
        return symbol

    def _get_symbol_min_trade_amount(self, coin, base = "USDT"):
        # self.get_future_market()
        minTradeNum = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == base), "minTradeNum"].values[0]
        return float(minTradeNum)

    def single_position(self, symbol, marginCoin = "USDT"):
        single_position = self.positionApi.single_position(symbol, marginCoin='USDT')
        return single_position


    #@authentication_required
    def get_open_position(self):
        all_positions = self.positionApi.all_position(productType='umcbl',marginCoin='USDT')
        lst_all_positions = [data for data in all_positions["data"] if float(data["total"]) != 0.]
        return self._build_df_open_positions(lst_all_positions)

    #@authentication_required
    def get_open_position_unrealizedPL(self, symbol):
        all_positions = self.positionApi.all_position(productType='umcbl',marginCoin='USDT')
        for data in all_positions["data"]:
            if data["symbol"] == symbol:
                if float(data["total"]) != 0.:
                    return float(data["unrealizedPL"])
                else:
                    return 0.0

    '''
    marginCoin: Deposit currency
    size: It is quantity when the price is limited. The market price is the limit. The sales is the quantity
    side \in {open_long, open_short, close_long, close_short}
    orderType \in {limit(fixed price), market(market price)}
    returns :
    - transaction_id
    - transaction_price
    - transaction_size
    - transaction_fee
    '''
    @authentication_required
    def _place_order_api(self, symbol, marginCoin, size, side, orderType):
        result = {}
        order = self.orderApi.place_order(symbol, marginCoin, size, side, orderType,
                                         price='',
                                         clientOrderId='', timeInForceValue='normal',
                                         presetTakeProfitPrice='', presetStopLossPrice='')
        # order structure contains order['data']['orderId'], order['data']['clientOid'] & order['requestTime']
        return order
        if order['msg'] == 'success':
            orderId = order['data']['orderId']
            result["transaction_id"], result["transaction_price"], result["transaction_size"], result["transaction_fee"] = self.get_order_fill_detail(symbol, orderId)
            result["order_id"] = orderId
            result["success"] = True
        else:
            result["success"] = False
            result["msg"] = order["msg"]
        return result

    @authentication_required
    def _open_long_position(self, symbol, amount):
        return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='open_long', orderType='market')

    @authentication_required
    def _close_long_position(self, symbol, amount):
        return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='close_long', orderType='market')

    @authentication_required
    def _open_short_position(self, symbol, amount):
        return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='open_short', orderType='market')

    @authentication_required
    def _close_short_position(self, symbol, amount):
        return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='close_short', orderType='market')

    @authentication_required
    def get_usdt_equity(self):
        self.get_list_of_account_assets()
        return self.df_account_assets['usdtEquity'].sum()

    @authentication_required
    def get_cash(self, baseCoin="USDT"):
        self.get_list_of_account_assets()
        return self.df_account_assets.loc[self.df_account_assets["symbol"] == baseCoin, "usdtEquity"].values[0]

    @authentication_required
    def get_balance(self):
        self.get_list_of_account_assets()
        df_balance = pd.DataFrame(columns=['symbol', 'usdValue'])
        df_balance['symbol'] = self.df_account_assets['symbol']
        df_balance['usdValue'] = self.df_account_assets['usdtEquity']
        df_balance['size'] = self.df_account_assets['available']
        return df_balance

    @authentication_required
    def get_order_history(self, symbol, startTime, endTime, pageSize):
        history = self.orderApi.history(symbol, startTime, endTime, pageSize, lastEndId='', isPre=False)
        df_history = pd.DataFrame(columns=["symbol", "size", "side", "orderId", "filledQty", "leverage", "fee", "orderType", "marginCoin", "totalProfits", "cTime", "uTime"])
        orderList = history["data"]["orderList"]
        if orderList:
            for i in range(len(orderList)):
                data = orderList[i]
                df_history.loc[i] = pd.Series({"symbol": data["symbol"], "size": data["size"], "side": data["side"], "orderId": data["orderId"], "filledQty": data["filledQty"], "leverage": int(data["leverage"]),"fee": float(data["fee"]),"orderType": data["orderType"],"marginCoin": data["marginCoin"],"totalProfits": float(data["totalProfits"]),"cTime": utils.convert_ms_to_datetime(data["cTime"]),"uTime": utils.convert_ms_to_datetime(data["uTime"])})
        return df_history

    @authentication_required
    def get_value(self, symbol):
        return float(self.marketApi.market_price(symbol)["data"]["markPrice"])



    def _market_results_to_df(self, markets):
        lst_columns = ['symbol', 'quoteCoin', 'baseCoin', 'symbolType', 'makerFeeRate', 'takerFeeRate', 'minTradeNum']
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

    def _account_results_to_df(self, markets):
        lst_columns = ['symbol', 'marginCoin', 'available', 'equity', 'usdtEquity', 'locked', 'btcEquity',
                       "size", "actualPrice", 'quoteCoin', 'baseCoin', 'symbolType',
                       'makerFeeRate', 'takerFeeRate', 'minTradeNum']
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
        df_market_umcbl = self._market_results_to_df(dct_market)
        dct_market = self.marketApi.contracts('dmcbl')
        df_market_dmcbl = self._market_results_to_df(dct_market)
        dct_market = self.marketApi.contracts('cmcbl')
        df_market_cmcbl = self._market_results_to_df(dct_market)
        return pd.concat([df_market_umcbl, df_market_dmcbl, df_market_cmcbl]).reset_index()

    def _get_df_account(self):
        # update market
        dct_account = self.accountApi.accounts('umcbl')
        df_account_umcbl = self._account_results_to_df(dct_account)
        dct_account = self.accountApi.accounts('dmcbl')
        df_account_dmcbl =  self._account_results_to_df(dct_account)
        dct_account = self.accountApi.accounts('cmcbl')
        df_account_cmcbl = self._account_results_to_df(dct_account)
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
        trade_id = price = fillAmount = sizeQty = fee = None
        response = self.orderApi.fills(symbol, order_id)
        if len(response["data"]) > 0:
            trade_id = response["data"][0]["tradeId"]
            price = response["data"][0]["price"]
            sizeQty = response["data"][0]["sizeQty"]
            fee = response["data"][0]["fee"]
            fillAmount = response["data"][0]["fillAmount"]
        return trade_id, float(price), float(fillAmount), float(sizeQty), float(fee)

    @authentication_required
    def get_symbol_min_max_leverage(self, symbol):
        symbol = self._get_symbol(symbol)
        leverage = self.marketApi.get_symbol_leverage(symbol)
        return leverage['data']['minLeverage'], leverage['data']['maxLeverage']

    @authentication_required
    def get_account_symbol_leverage(self, symbol, marginCoin="USDT"):
        symbol = self._get_symbol(symbol, marginCoin)
        dct_account = self.accountApi.account(symbol, marginCoin)
        return dct_account['data']['crossMarginLeverage'], dct_account['data']['fixedLongLeverage'], dct_account['data']['fixedShortLeverage']

    @authentication_required
    def set_account_symbol_leverage(self, symbol, leverage):
        symbol = self._get_symbol(symbol)
        dct_account = self.accountApi.leverage(symbol, "USDT", leverage)
        return dct_account['data']['crossMarginLeverage'], dct_account['data']['longLeverage'], dct_account['data']['shortLeverage']

    def get_min_order_amount(self, symbol):
        if "_" in symbol:
            symbol = symbol[:3]+"USDT_SPBL"
        else:
            symbol += "USDT_SPBL"
        product = self.publicApi.product(symbol)
        return float(product["data"]["minTradeAmount"])
