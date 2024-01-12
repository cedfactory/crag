import math

from .bitget.mix import market_api as market
from .bitget.mix import account_api as account
from .bitget.mix import position_api as position
from .bitget.mix import order_api as order
from .bitget.spot import public_api as public

from . import broker_bitget
from . import utils
from datetime import datetime
import time
import os, shutil
import pandas as pd

class BrokerBitGetApi(broker_bitget.BrokerBitGet):
    def __init__(self, params = None):
        super().__init__(params)

        self.marketApi = None
        self.accountApi = None
        self.positionApi = None
        self.orderApi = None
        self.publicApi = None

        self.df_market = self.get_future_market()
        if isinstance(self.df_market, pd.DataFrame):
            self.df_market.drop( self.df_market[self.df_market['quoteCoin'] != 'USDT'].index, inplace=True)
            self.df_market.reset_index(drop=True)
            print('list symbols perpetual/USDT: ', self.df_market["baseCoin"].tolist())

        self.failure = 0
        self.success = 0

        self.boot_status = ""
        self.broker_dir_path = "./broker_data"
        self.broker_dir_path_filename = self.broker_dir_path + "./broker_init_data.csv"
        if self.reset_account_orders:
            self.cancel_all_orders(["XRP", "BTC", "ETH"])
        if self.reset_account:
            print('reset account requested')
            self.execute_reset_account()
            self.clear_broker_reset_data()
            self.set_boot_status_to_reseted()
        else:
            print('reset account not requested')
            print('resume strategy')
            self.set_boot_status_to_resumed()

    def _authentification(self):
        if not self.account:
            return False
        exchange_api_key = self.account.get("api_key", None)
        exchange_api_secret = self.account.get("api_secret", None)
        exchange_api_password = self.account.get("api_password", None)

        self.marketApi = market.MarketApi(exchange_api_key, exchange_api_secret, exchange_api_password, use_server_time=False, first=False)
        self.accountApi = account.AccountApi(exchange_api_key, exchange_api_secret, exchange_api_password, use_server_time=False, first=False)
        self.positionApi = position.PositionApi(exchange_api_key, exchange_api_secret, exchange_api_password, use_server_time=False, first=False)
        self.orderApi = order.OrderApi(exchange_api_key, exchange_api_secret, exchange_api_password, use_server_time=False, first=False)
        self.publicApi = public.PublicApi(exchange_api_key, exchange_api_secret, exchange_api_password, use_server_time=False, first=False)

        return True

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
        if coin in self.df_market['symbol'].tolist():
            return coin
        symbol = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == base), "symbol"].values[0]
        return symbol

    def _get_coin(self, symbol, base = "USDT"):
        # self.get_future_market()
        if symbol in self.df_market['baseCoin'].tolist():
            return symbol

        if symbol in self.df_market['symbol'].tolist():
            coin = self.df_market.loc[(self.df_market['symbol'] == symbol) & (self.df_market['quoteCoin'] == base), "baseCoin"].values[0]
            return coin
        else:
            print("WARNING COIN NOT IN MARKET LIST")
            return symbol.split("USDT_UMCBL")[0]

    def single_position(self, symbol, marginCoin = "USDT"):
        single_position = self.positionApi.single_position(symbol, marginCoin='USDT')
        return single_position


    #@authentication_required
    def get_open_position(self):
        res = pd.DataFrame()
        n_attempts = 3
        while n_attempts > 0:
            try:
                all_positions = self.positionApi.all_position(productType='umcbl',marginCoin='USDT')
                lst_all_positions = [data for data in all_positions["data"] if float(data["total"]) != 0.]
                res = self._build_df_open_positions(lst_all_positions)
                self.success += 1
                break
            except:
                now = datetime.now()
                current_time = now.strftime("%H:%M:%S")
                print(" current time =", current_time, " - failure:  get_open_position  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1

        return res

    # @authentication_required
    def get_open_orders(self, symbols):
        res = pd.DataFrame()
        for symbol in symbols:
            n_attempts = 3
            while n_attempts > 0:
                try:
                    all_orders = self.orderApi.current(symbol=symbol + "USDT_UMCBL")
                    # print("all_orders : ", all_orders)
                    lst_all_orders = [data for data in all_orders["data"]]
                    current_res = self._build_df_open_orders(lst_all_orders)
                    res = pd.concat([res, current_res])
                    self.success += 1
                    break
                except:
                    now = datetime.now()
                    current_time = now.strftime("%H:%M:%S")
                    print(" current time =", current_time, " - failure:  get_open_position  - attempt: ", n_attempts)
                    self.failure += 1
                    print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ",
                          self.failure / (self.success + self.failure) * 100)
                    time.sleep(2)
                    n_attempts = n_attempts - 1
        return res

    @authentication_required
    def get_current_state(self, lst_symbols):
        df_open_orders = self.get_open_orders(lst_symbols)
        df_open_orders['symbol'] = df_open_orders['symbol'].apply(lambda x: self._get_coin(x))
        df_open_orders.drop(['marginCoin', 'clientOid'], axis=1, inplace=True)
        df_open_orders = self.set_open_orders_gridId(df_open_orders)

        df_open_positions = self.get_open_position()
        df_open_positions['symbol'] = df_open_positions['symbol'].apply(self._get_coin)
        df_open_positions_filtered = df_open_positions[df_open_positions['symbol'].isin(lst_symbols)]

        if any(df_open_positions_filtered):
            df_open_positions = df_open_positions_filtered

        df_prices = self.get_values(lst_symbols)
        current_state = {
            "open_orders": df_open_orders,
            "open_positions": df_open_positions,
            "prices": df_prices
        }
        return current_state

    @authentication_required
    def reset_current_postion(self, current_state):
        if self.get_nb_order_current_state(current_state) > 0:
            print("init - reset current order")
            self.cancel_all_orders(self.get_lst_symbol_current_state(current_state))

    def get_lst_symbol_current_state(self, current_state):
        return current_state["open_orders"]["symbol"].tolist()

    def get_lst_orderId_current_state(self, current_state):
        return current_state["open_orders"]["orderId"].tolist()

    def get_nb_order_current_state(self, current_state):
        return len(current_state["open_orders"])

    #@authentication_required
    def get_account_equity(self):
        n_attempts = 3
        while n_attempts > 0:
            try:
                account_equity = self.positionApi.account(symbol='BTCUSDT_UMCBL',marginCoin='USDT')
                self.success += 1
                break
            except:
                print("failure:  get_account_equity  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        return account_equity['data']['usdtEquity']

    #@authentication_required
    def get_account_available(self):
        n_attempts = 3
        while n_attempts > 0:
            try:
                account_equity = self.positionApi.account(symbol='BTCUSDT_UMCBL',marginCoin='USDT')
                self.success += 1
                break
            except:
                print("failure:  get_account_equity  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        return account_equity['data']['available']

    #@authentication_required
    def get_open_position_unrealizedPL(self, symbol):
        n_attempts = 3
        while n_attempts > 0:
            try:
                all_positions = self.positionApi.all_position(productType='umcbl',marginCoin='USDT')
                self.success += 1
                break
            except:
                print("failure:  get_open_position_unrealizedPL  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
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
    def _place_order_api(self, symbol, marginCoin, size, side, orderType, clientOId, price=''):
        result = {}
        n_attempts = 3
        while n_attempts > 0:
            try:
                result = self.orderApi.place_order(symbol, marginCoin, size, side, orderType,
                                                 price=price,
                                                 clientOrderId=clientOId, timeInForceValue='normal',
                                                 presetTakeProfitPrice='', presetStopLossPrice='')
                self.success += 1
                break
            except:
                print("failure:  get_open_position_unrealizedPL  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        # order structure contains order['data']['orderId'], order['data']['clientOid'] & order['requestTime']
        return result

    @authentication_required
    def get_portfolio_value(self):
        return self.get_usdt_equity()

    def get_info(self):
        return None, None, None

    @authentication_required
    def _open_long_position(self, symbol, amount, clientoid):
        return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='open_long', orderType='market', clientOId=clientoid)

    @authentication_required
    def _close_long_position(self, symbol, amount, clientoid):
        return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='close_long', orderType='market', clientOId=clientoid)

    @authentication_required
    def _open_short_position(self, symbol, amount, clientoid):
        return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='open_short', orderType='market', clientOId=clientoid)

    @authentication_required
    def _close_short_position(self, symbol, amount, clientoid):
        return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='close_short', orderType='market', clientOId=clientoid)

    @authentication_required
    def _open_long_order(self, symbol, amount, clientoid, price):
        return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='open_long', orderType='limit', price=price, clientOId=clientoid)

    @authentication_required
    def _close_long_order(self, symbol, amount, clientoid, price):
        return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='close_long', orderType='limit', price=price, clientOId=clientoid)

    @authentication_required
    def _open_short_order(self, symbol, amount, clientoid, price):
        return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='open_short', orderType='limit', price=price, clientOId=clientoid)

    @authentication_required
    def _close_short_order(self, symbol, amount, clientoid, price):
        return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='close_short', orderType='limit', price=price, clientOId=clientoid)

    @authentication_required
    def get_wallet_equity(self):
        n_attempts = 3
        while n_attempts > 0:
            try:
                self.get_list_of_account_assets()
                self.success += 1
                break
            except:
                print("failure:  get_wallet_equity  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        return self.df_account_assets['usdtEquity'].sum()

    @authentication_required
    def get_usdt_equity(self):
        n_attempts = 3
        while n_attempts > 0:
            try:
                self.get_list_of_account_assets()
                self.success += 1
                break
            except:
                print("failure:  get_usdt_equity  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        cell = self.df_account_assets.loc[(self.df_account_assets['baseCoin'] == 'USDT') & (self.df_market['quoteCoin'] == 'USDT'), "usdtEquity"]
        if len(cell.values) > 0:
            return cell.values[0]
        return 0

    @authentication_required
    def get_spot_usdt_equity(self, lst_symbols):
        n_attempts = 3
        while n_attempts > 0:
            try:
                df_spot_usdt_equity = self._get_df_spot_account(lst_symbols)
                self.success += 1
                break
            except:
                print("failure:  get_spot_usdt_equity  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        if len(df_spot_usdt_equity) > 0:
            return df_spot_usdt_equity["equity"].sum()
        return 0

    @authentication_required
    def get_cash(self, baseCoin="USDT"):
        available, crossMaxAvailable, fixedMaxAvailable = self.get_available_cash(baseCoin)
        return min(available, crossMaxAvailable, fixedMaxAvailable)

    @authentication_required
    def get_available_cash(self, baseCoin="USDT"):
        n_attempts = 3
        while n_attempts > 0:
            try:
                self.get_list_of_account_assets()
                self.success += 1
                break
            except:
                print("failure:  get_cash  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1

        if len(self.df_account_assets) == 0:
            return 0, 0, 0
        else:
            available = self.df_account_assets.loc[self.df_account_assets["symbol"] == baseCoin, "available"].values[0]
            crossMaxAvailable = self.df_account_assets.loc[self.df_account_assets["symbol"] == baseCoin, "crossMaxAvailable"].values[0]
            fixedMaxAvailable = self.df_account_assets.loc[self.df_account_assets["symbol"] == baseCoin, "fixedMaxAvailable"].values[0]
            return available, crossMaxAvailable, fixedMaxAvailable

    # available = size
    # available = ammount of cash available without any calculation (amount of USDT owned)
    # equity or usdtEquity = available - unrealizedPL => real value of usdt that can be used
    # crossMaxAvailable = usdtEquity - usdtEquity of each coin owned => reamaing usdt in casse of liquidation

    @authentication_required
    def get_balance(self):
        n_attempts = 3
        while n_attempts > 0:
            try:
                self.get_list_of_account_assets()
                self.success += 1
                break
            except:
                print("failure:  get_balance  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        return self.df_account_assets

    @authentication_required
    def get_order_history(self, symbol, startTime, endTime, pageSize):
        n_attempts = 3
        while n_attempts > 0:
            try:
                history = self.orderApi.history(symbol, startTime, endTime, pageSize, lastEndId='', isPre=False)
                self.success += 1
                break
            except:
                print("failure:  get_order_history  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        df_history = pd.DataFrame(columns=["symbol", "size", "side", "orderId", "filledQty", "leverage", "fee", "orderType", "marginCoin", "totalProfits", "cTime", "uTime"])
        orderList = history["data"]["orderList"]
        if orderList:
            for i in range(len(orderList)):
                data = orderList[i]
                df_history.loc[i] = pd.Series({"symbol": data["symbol"], "size": data["size"], "side": data["side"], "orderId": data["orderId"], "filledQty": data["filledQty"], "leverage": int(data["leverage"]),"fee": float(data["fee"]),"orderType": data["orderType"],"marginCoin": data["marginCoin"],"totalProfits": float(data["totalProfits"]),"cTime": utils.convert_ms_to_datetime(data["cTime"]),"uTime": utils.convert_ms_to_datetime(data["uTime"])})
        return df_history

    @authentication_required
    def get_value(self, symbol):
        symbol = symbol + 'USDT_UMCBL'
        value = 0
        n_attempts = 3
        while n_attempts > 0:
            try:
                # value = float(self.marketApi.market_price(symbol)["data"]["markPrice"])
                value = float(self.marketApi.ticker(symbol)["data"]["last"])
                self.success += 1
                break
            except:
                print("failure:  get_value  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        return value

    @authentication_required
    def get_values(self, symbols):
        values = []
        for symbol in symbols:
            values.append(self.get_value(symbol))
        df_prices = pd.DataFrame({
            "symbols": symbols,
            "values": values
        })
        return df_prices

    @authentication_required
    def get_asset_available(self, symbol):
        symbol = symbol + 'USDT_UMCBL'
        value = 0
        n_attempts = 3
        while n_attempts > 0:
            try:
                value = float(self.marketApi.market_price(symbol)["data"]["available"])
                self.success += 1
                break
            except:
                print("failure:  get_asset_equity  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        return value

    @authentication_required
    def get_asset_equity(self, symbol):
        symbol = symbol + 'USDT_UMCBL'
        value = 0
        n_attempts = 3
        while n_attempts > 0:
            try:
                value = float(self.marketApi.market_price(symbol)["data"]["usdtEquity"])
                self.success += 1
                break
            except:
                print("failure:  get_asset_equity  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        return value

    def _market_results_to_df(self, markets):
        lst_columns = ['symbol', 'quoteCoin', 'baseCoin', 'symbolType', 'makerFeeRate', 'takerFeeRate',
                       'minTradeNum', 'priceEndStep', 'volumePlace', 'pricePlace', 'sizeMultiplier']
        df = pd.DataFrame(columns=lst_columns)
        for market in markets['data']:
            lst_info_symbol = [ market['symbol'],
                                market['quoteCoin'],
                                market['baseCoin'],
                                market['symbolType'],
                                float(market['makerFeeRate']),
                                float(market['takerFeeRate']),
                                float(market['minTradeNum']),
                                float(market['priceEndStep']),
                                float(market['volumePlace']),
                                float(market['pricePlace']),
                                float(market['sizeMultiplier'])
                                ]
            df.loc[len(df)] = lst_info_symbol
        return df

    def _account_results_to_df(self, markets):
        lst_columns = ['symbol', 'marginCoin', 'available', 'crossMaxAvailable', 'fixedMaxAvailable',
                       'equity', 'usdtEquity', 'locked', 'btcEquity',
                       'unrealizedPL',
                       "size", "actualPrice", 'quoteCoin', 'baseCoin', 'symbolType',
                       'makerFeeRate', 'takerFeeRate',
                       'minTradeNum', 'priceEndStep', 'volumePlace', 'pricePlace', 'sizeMultiplier']
        df = pd.DataFrame(columns=lst_columns)
        for market in markets['data']:
            if float(market['equity']) > 0 \
                    and float(market['usdtEquity']) > 0 \
                    and float(market['btcEquity']) > 0:
                if market['unrealizedPL'] == None:
                    unrealizedPL = 0
                else:
                    unrealizedPL = float(market['unrealizedPL'])
                lst_info_symbol = [ market['marginCoin'], # USDT
                                    market['marginCoin'], # USDT
                                    float(market['available']),
                                    float(market['crossMaxAvailable']),
                                    float(market['fixedMaxAvailable']),
                                    float(market['equity']),
                                    float(market['usdtEquity']),
                                    float(market['locked']),
                                    float(market['btcEquity']),
                                    unrealizedPL,
                                    0, 0, "", "", "", 0, 0, 0, 0, 0, 0, 0]
                df.loc[len(df)] = lst_info_symbol
        return df.copy()

    @authentication_required
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
        return pd.concat([df_market_umcbl, df_market_dmcbl, df_market_cmcbl]).reset_index(drop=True)

    def _get_df_account(self):
        # update market
        not_usdt = False
        dct_account = self.accountApi.accounts('umcbl')
        df_account_umcbl = self._account_results_to_df(dct_account)
        if not_usdt:
            dct_account = self.accountApi.accounts('dmcbl')
            df_account_dmcbl =  self._account_results_to_df(dct_account)
            dct_account = self.accountApi.accounts('cmcbl')
            df_account_cmcbl = self._account_results_to_df(dct_account)
            self.df_account_assets = pd.concat([df_account_umcbl, df_account_dmcbl, df_account_cmcbl])
        else:
            self.df_account_assets = df_account_umcbl

        self.df_account_open_position = self.get_open_position()
        self.df_account_open_position.rename(columns={'usdEquity': 'usdtEquity',
                                                      'total': 'size'},
                                             inplace=True)
        self.df_account_open_position['equity'] = self.df_account_open_position['usdtEquity']
        self.df_account_assets = pd.concat([self.df_account_assets, self.df_account_open_position])
        self.df_account_assets.reset_index(inplace=True, drop=True)

    def _get_df_spot_account(self, lst_symbols):
        df_spot_asset = pd.DataFrame(columns=["symbol", "size", "price", "equity"])
        for symbol in lst_symbols:
            spot_asset = self.accountApi.assets_spot(symbol)
            spot_asset_size = float(spot_asset["data"][0]["available"])
            if spot_asset_size != 0:
                if symbol == "USDT":
                    spot_asset_equity = spot_asset_size
                else:
                    spot_asset_price = float(self.get_value(symbol))
                    spot_asset_equity = spot_asset_price * spot_asset_size
                df_spot_asset.loc[len(df_spot_asset)] = [symbol, spot_asset_size, spot_asset_price, spot_asset_equity]
        return df_spot_asset

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
                self.df_account_assets.at[idx, 'priceEndStep'] = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == 'USDT'), "priceEndStep"].values[0]
                self.df_account_assets.at[idx, 'volumePlace'] = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == 'USDT'), "volumePlace"].values[0]
                self.df_account_assets.at[idx, 'pricePlace'] = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == 'USDT'), "pricePlace"].values[0]
                self.df_account_assets.at[idx, 'sizeMultiplier'] = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == 'USDT'), "sizeMultiplier"].values[0]
            else:
                symbol_id = self.df_account_assets.at[idx, 'symbol']
                if not symbol_id.startswith('USDT'):
                    baseCoin = symbol_id.split('USDT')[0]
                    marketPrice = self.df_account_assets.at[idx, 'marketPrice']
                else:
                    baseCoin = symbol_id
                    marketPrice = 1.0
                self.df_account_assets.at[idx, 'quoteCoin'] = coin
                self.df_account_assets.at[idx, 'baseCoin'] = baseCoin
                self.df_account_assets.at[idx, 'size'] = self.df_account_assets.at[idx, 'available']
                self.df_account_assets.at[idx, 'actualPrice'] = marketPrice

        self.df_account_assets['symbol_broker'] = self.df_account_assets['symbol']
        self.df_account_assets['symbol'] = self.df_account_assets['symbol'].str.split('USDT_').str[0]

    def _fill_price_and_size_from_bitget(self):
        for symbol in self.df_account_assets['symbol'].tolist():
            if not symbol.startswith('USDT'):
                self.df_account_assets.loc[self.df_account_assets['symbol'] == symbol, "actualPrice"] = self.get_value(symbol)
            else:
                pass

    def print_account_assets(self):
        print(self.df_account_assets)

    def get_list_of_account_assets(self):
        self._get_df_account()
        self._fill_df_account_from_market()
        # self._fill_price_and_size_from_bitget()


    @authentication_required
    def get_account_asset(self):
        n_attempts = 3
        while n_attempts > 0:
            try:
                result = self.accountApi.accountAssets(productType='umcbl')
            except:
                print("failure:  get_account_asset  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        return result

    @authentication_required
    def get_order_current(self, symbol):
        n_attempts = 3
        while n_attempts > 0:
            try:
                current = self.orderApi.current(symbol)
                self.success += 1
                break
            except:
                print("failure:  get_order_current  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        return current

    @authentication_required
    def cancel_order(self, symbol, marginCoin, orderId):
        symbol = self._get_symbol(symbol)
        result = {}
        n_attempts = 3
        while n_attempts > 0:
            try:
                result = self.orderApi.cancel_orders(symbol, marginCoin, orderId)
                self.success += 1
                break
            except:
                print("failure:  cancel_order  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        if result.get("msg", "") == "success":
            return True, result['data']['orderId']
        else:
            return False , False

    # example : cancel_all_orders(["BTC", "ETH", "XRP"])
    def cancel_all_orders(self, lst_symbols):
        df_open_orders = self.get_open_orders(lst_symbols)
        for index, row in df_open_orders.iterrows():
            self.cancel_order(row["symbol"], row["marginCoin"], row["orderId"])

    def get_order_fill_detail(self, symbol, order_id):
        trade_id = price = fillAmount = sizeQty = fee = None
        n_attempts = 3
        while n_attempts > 0:
            try:
                response = self.orderApi.fills(symbol, order_id)
                self.success += 1
                break
            except:
                print("failure:  get_order_fill_detail  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        if len(response["data"]) == 0:
            # df_positions = self.get_open_position()
            # if symbol in df_positions["symbol"].tolist():
            cpt = 0
            while len(response["data"]) == 0 and cpt < 5:
                response = self.orderApi.fills(symbol, order_id)
                cpt += 1
                if len(response["data"]) > 0:
                    break
            if len(response["data"]) == 0:
                return trade_id, 0, 0, 0, 0
        if len(response["data"]) > 1:
            print("get_order_fill_detail multiple order", )

        trade_id = response["data"][0]["tradeId"]
        price = 0
        sizeQty = 0
        fee = 0
        fillAmount = 0
        for data in response["data"]:
            price += float(data["price"])
            sizeQty += float(data["sizeQty"])
            fee += float(data["fee"])
            fillAmount += float(data["fillAmount"])

        return trade_id, price, fillAmount, sizeQty, fee

    @authentication_required
    def get_symbol_min_max_leverage(self, symbol):
        n_attempts = 3
        while n_attempts > 0:
            try:
                symbol = self._get_symbol(symbol)
                leverage = self.marketApi.get_symbol_leverage(symbol)
                self.success += 1
                break
            except:
                print("failure:  get_symbol_min_max_leverage  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        return leverage['data']['minLeverage'], leverage['data']['maxLeverage']

    @authentication_required
    def get_account_symbol_leverage(self, symbol, marginCoin="USDT"):
        n_attempts = 3
        while n_attempts > 0:
            try:
                symbol = self._get_symbol(symbol, marginCoin)
                dct_account = self.accountApi.account(symbol, marginCoin)
                self.success += 1
                break
            except:
                print("failure:  get_account_symbol_leverage  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        return dct_account['data']['crossMarginLeverage'], dct_account['data']['fixedLongLeverage'], dct_account['data']['fixedShortLeverage']

    @authentication_required
    def set_account_symbol_leverage(self, symbol, leverage, hold="long"):
        n_attempts = 3
        while n_attempts > 0:
            try:
                dct_account = self.accountApi.leverage(symbol, "USDT", leverage, hold)
                self.success += 1
                break
            except:
                print("failure:  set_account_symbol_leverage  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        return dct_account['data']['crossMarginLeverage'], dct_account['data']['longLeverage'], dct_account['data']['shortLeverage']

    @authentication_required
    def set_account_symbol_margin(self, symbol, marginMode="fixed"):
        n_attempts = 3
        while n_attempts > 0:
            try:
                dct_account = self.accountApi.margin_mode(symbol, "USDT", marginMode)
                self.success += 1
                break
            except:
                print("failure:  set_account_symbol_margin  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        return dct_account['data']['marginMode']

    def get_min_order_amount(self, symbol):
        if "_" in symbol:
            symbol = symbol[:3]+"USDT_SPBL"
        else:
            symbol += "USDT_SPBL"
        product = self.publicApi.product(symbol)
        return float(product["data"]["minTradeAmount"])

    def get_commission(self, symbol):
        if symbol in self.df_market['baseCoin'].tolist():
            idx = self.df_market[  (self.df_market['baseCoin'] == symbol) & (self.df_market['quoteCoin'] == "USDT")  ].index
            return  self.df_market.at[idx[0], "takerFeeRate"]
        elif symbol in self.df_market['symbol'].tolist():
            idx = self.df_market[  (self.df_market['symbol'] == symbol) & (self.df_market['quoteCoin'] == "USDT")  ].index
            return  self.df_market.at[idx[0], "takerFeeRate"]
        else:
            return 0

    def get_minimum_size(self, symbol):
        if symbol in self.df_market['baseCoin'].tolist():
            idx = self.df_market[  (self.df_market['baseCoin'] == symbol) & (self.df_market['quoteCoin'] == "USDT")  ].index
            return self.df_market.at[idx[0], "minTradeNum"]
        elif symbol in self.df_market['symbol'].tolist():
            idx = self.df_market[  (self.df_market['symbol'] == symbol) & (self.df_market['quoteCoin'] == "USDT")  ].index
            return self.df_market.at[idx[0], "minTradeNum"]
        else:
            return 0

    def get_priceEndStep(self, symbol):
        if symbol in self.df_market['baseCoin'].tolist():
            idx = self.df_market[  (self.df_market['baseCoin'] == symbol) & (self.df_market['quoteCoin'] == "USDT")  ].index
            return self.df_market.at[idx[0], "priceEndStep"]
        elif symbol in self.df_market['symbol'].tolist():
            idx = self.df_market[  (self.df_market['symbol'] == symbol) & (self.df_market['quoteCoin'] == "USDT")  ].index
            return self.df_market.at[idx[0], "priceEndStep"]
        else:
            return 0

    def get_volumePlace(self, symbol):
        if symbol in self.df_market['baseCoin'].tolist():
            idx = self.df_market[  (self.df_market['baseCoin'] == symbol) & (self.df_market['quoteCoin'] == "USDT")  ].index
            return int(self.df_market.at[idx[0], "volumePlace"])
        elif symbol in self.df_market['symbol'].tolist():
            idx = self.df_market[  (self.df_market['symbol'] == symbol) & (self.df_market['quoteCoin'] == "USDT")  ].index
            return int(self.df_market.at[idx[0], "volumePlace"])
        else:
            return 0

    def get_pricePlace(self, symbol):
        if symbol in self.df_market['baseCoin'].tolist():
            idx = self.df_market[  (self.df_market['baseCoin'] == symbol) & (self.df_market['quoteCoin'] == "USDT")  ].index
            return self.df_market.at[idx[0], "pricePlace"]
        elif symbol in self.df_market['symbol'].tolist():
            idx = self.df_market[  (self.df_market['symbol'] == symbol) & (self.df_market['quoteCoin'] == "USDT")  ].index
            return self.df_market.at[idx[0], "pricePlace"]
        else:
            return 0

    def get_sizeMultiplier(self, symbol):
        if symbol in self.df_market['baseCoin'].tolist():
            idx = self.df_market[  (self.df_market['baseCoin'] == symbol) & (self.df_market['quoteCoin'] == "USDT")  ].index
            return self.df_market.at[idx[0], "sizeMultiplier"]
        elif symbol in self.df_market['symbol'].tolist():
            idx = self.df_market[  (self.df_market['symbol'] == symbol) & (self.df_market['quoteCoin'] == "USDT")  ].index
            return self.df_market.at[idx[0], "sizeMultiplier"]
        else:
            return 0

    def set_symbol_leverage(self, symbol, leverage, hold):
        n_attempts = 3
        while n_attempts > 0:
            try:
                crossMarginLeverage, longLeverage, shortLeverage = self.set_account_symbol_leverage(symbol, leverage, hold)
                self.success += 1
                break
            except:
                print("failure:  set_symbol_leverage  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        print(symbol, ' long leverage: ', longLeverage,' short leverage: ', shortLeverage)

    def set_symbol_margin(self, symbol, margin):
        n_attempts = 3
        set_symbol_margin = False
        while n_attempts > 0:
            try:
                set_symbol_margin = self.set_account_symbol_margin(symbol, margin)
                print(symbol, ' margin set to ', margin, " ", set_symbol_margin)
                self.success += 1
                break
            except:
                print("failure:  set_account_symbol_margin  - attempt: ", n_attempts)
                self.failure += 1
                print("failure: ", self.failure, " - success: ", self.success, " - percentage failure: ", self.failure / (self.success + self.failure) * 100)
                time.sleep(2)
                n_attempts = n_attempts - 1
        return set_symbol_margin

    def clear_broker_reset_data(self):
        folder = self.broker_dir_path
        if os.path.exists(folder):
            shutil.rmtree(folder)
        os.mkdir(folder)

    def get_broker_boot_data(self):
        if self.boot_status == "RESUMED" and os.path.exists(self.broker_dir_path_filename):
            df = pd.read_csv(self.broker_dir_path_filename)
            return df
        else:
            return None

    def get_broker_data_path(self):
        return self.broker_dir_path

    def set_boot_status_to_reseted(self):
        self.boot_status = "RESETED"

    def set_boot_status_to_resumed(self):
        self.boot_status = "RESUMED"
        folder = self.broker_dir_path
        if not os.path.exists(folder):
            os.mkdir(folder)

    def get_boot_status(self):
        return self.boot_status

    def broker_resumed(self):
        return self.get_boot_status() == "RESUMED"

    def save_reboot_data(self, df):
        df.to_csv(self.broker_dir_path_filename)

    def normalize_price(self, symbol, amount):
        pricePlace = int(self.get_pricePlace(symbol))
        priceEndStep = self.get_priceEndStep(symbol)

        amount = amount * pow(10, pricePlace)
        amount = math.floor(amount)
        amount = amount * pow(10, -pricePlace)

        amount = round(amount, pricePlace)

        # Calculate the decimal without using %
        decimal_multiplier = priceEndStep * pow(10, -pricePlace)
        decimal = amount - math.floor(round(amount / decimal_multiplier)) * decimal_multiplier

        amount = amount - decimal
        amount = round(amount, pricePlace)

        return amount

    def normalize_size(self, symbol, size):
        volumePlace = self.get_volumePlace(symbol)
        sizeMultiplier = self.get_sizeMultiplier(symbol)
        minsize = self.get_minimum_size(symbol)

        size = size * pow(10, volumePlace)
        size = math.floor(size)
        size = size * pow(10, -volumePlace)

        decimal = (size % sizeMultiplier)
        size = size - decimal
        size = round(size, volumePlace)
        if size < minsize:
            return 0
        return size

    def get_price_place_endstep(self, lst_symbol):
        lst = []
        for symbol in lst_symbol:
            dct = {}
            dct['symbol'] = symbol
            dct['pricePlace'] = int(self.get_pricePlace(symbol))
            dct['priceEndStep'] = self.get_priceEndStep(symbol)
            lst.append(dct)
        return lst
