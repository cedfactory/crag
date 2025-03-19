import math

from .bitget.mix import market_api as market
from .bitget.mix import account_api as account
from .bitget.mix import position_api as position
from .bitget.mix import order_api as order
from .bitget.mix import plan_api as plan
from .bitget.spot import public_api as public
from .bitget import exceptions
from .bitget.mix_v2 import order_api as orderV2
from .bitget.mix_v2 import account_api as accountV2
from .fdp.src import bitget_ws_positions, bitget_ws_account_tickers

from .bitget_ws.bitget_ws import BitgetWsClient

from . import broker_bitget
from . import utils
from datetime import datetime
import time
import os, shutil
import json
import pandas as pd
import numpy as np
from concurrent.futures import wait, ALL_COMPLETED, ThreadPoolExecutor, as_completed
import asyncio
import gc
from .bitget_ws_data import ws_Data, convert_open_orders_push_list_to_df, convert_triggers_convert_df_to_df
from .client_ws_bitget import BitgetWebSocketClient
import threading

class BrokerBitGetApi(broker_bitget.BrokerBitGet):
    def __init__(self, params = None):
        super().__init__(params)

        self.marketApi = None
        self.accountApi = None
        self.positionApi = None
        self.orderApi = None
        self.publicApi = None
        self.planApi = None
        self.positionV2Api = None
        self.orderV2Api = None
        self.ws_positions = None

        self.failure = 0
        self.success = 0

        self.boot_status = ""
        self.broker_dir_path = "./broker_data/"
        self.broker_dir_path_filename = os.path.join(self.broker_dir_path, "broker_init_data.csv")

        self.enable_cache_data = False
        self.requests_cache = {}

        self.df_triggers_previous = pd.DataFrame(columns=["planType", "symbol", "size", "side",
                                                          "orderId", "orderType", "clientOid",
                                                          "price", "triggerPrice", "triggerType",
                                                          "marginMode", "gridId", "strategyId",
                                                          "executeOrderId", "planStatus"])

        # https://bitgetlimited.github.io/apidoc/en/mix/#plantype
        # https://bitgetlimited.github.io/apidoc/en/mix/#isplan
        self.plan_mapping = {
            'profit_plan': 'profit_loss',
            'loss_plan': 'profit_loss',
            'pos_profit': 'profit_loss',
            'pos_loss': 'profit_loss',
            'moving_plan': 'profit_loss',
            'normal_plan': 'normal_plan',  # These remain the same, but we include them for completeness
            'track_plan': 'track_plan'
        }

        # initialize the websocket client
        api_key = self.account.get("api_key", "")
        api_secret = self.account.get("api_secret", "")
        api_password = self.account.get("api_password", "")

        self.df_normalize_price = pd.DataFrame(columns=["symbol", "pricePlace", "priceEndStep"])
        self.df_normalize_size = pd.DataFrame(columns=["symbol", "volumePlace", "sizeMultiplier", "minsize"])

        self.current_state = {}
        self.df_prices = pd.DataFrame()

        self.marketApi = market.MarketApi(api_key, api_secret, api_password, use_server_time=False, first=False)
        if api_key != "" and api_secret != "" and api_password != "":
            self.accountApi = account.AccountApi(api_key, api_secret, api_password, use_server_time=False, first=False)
            self.positionApi = position.PositionApi(api_key, api_secret, api_password, use_server_time=False, first=False)
            self.orderApi = order.OrderApi(api_key, api_secret, api_password, use_server_time=False, first=False)
            self.planApi = plan.PlanApi(api_key, api_secret, api_password, use_server_time=False, first=False)
            self.orderV2Api = orderV2.OrderApi(api_key, api_secret, api_password, use_server_time=False, first=False)
            self.accountV2Api = accountV2.AccountApi(api_key, api_secret, api_password, use_server_time=False, first=False)
            params["api_key"] = api_key
            params["api_secret"] = api_secret
            params["api_passphrase"] = api_password
            self.ws_positions = bitget_ws_positions.FDPWSPositions(params)

        # initialize the public api
        self.publicApi = public.PublicApi(api_key, api_secret, api_password, use_server_time=False, first=False)

        self.df_market = self.get_future_market()
        if isinstance(self.df_market, pd.DataFrame):
            self.df_market.drop( self.df_market[self.df_market['quoteCoin'] != 'USDT'].index, inplace=True)
            self.df_market.reset_index(drop=True)
            #self.log('list symbols perpetual/USDT: {}'.format(self.df_market["baseCoin"].tolist()))
        else:
            print("error df_market")
            exit(2956)

        # reset account
        if self.reset_account_start:
            self.log('reset account requested')
            self.execute_reset_account()
            self.clear_broker_reset_data()
            self.set_boot_status_to_reseted()
        else:
            self.log('reset account not requested')
            self.log('resume strategy')
            self.set_boot_status_to_resumed()

        # marginMode & leverages management
        if isinstance(self.df_symbols, pd.DataFrame):
            self.df_symbols["symbol_original"] = self.df_symbols["symbol"]
            self.df_symbols["symbol"] = self.df_symbols.apply(lambda row: self._get_symbol(row["symbol"]), axis=1)
            self.set_margin_mode_and_leverages()

        self.WS_ON = True
        if self.WS_ON:
            self.trigger_BitgetWsClient(self.df_symbols, params["data_description"], api_key, api_secret, api_password)
            '''
            # Create a thread that targets your function and passes the arguments.
            thread = threading.Thread(
                target=self.trigger_BitgetWsClient,
                args=(self.df_symbols, api_key, api_secret, api_password)
            )

            # Start the thread.
            thread.start()
            '''

    def stop(self):
        if hasattr(self, "ws_positions") and self.ws_positions:
            self.ws_positions.stop()

    def __del__(self):
        if hasattr(self, "ws_positions") and self.ws_positions:
            self.ws_positions.stop()

    def log_api_failure(self, function, e, n_attempts=0):
        self.failure += 1
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        self.log("💥 !!!!! Failure on: " + function)
        self.log("current time = " + str(current_time) + "  - attempt: " + str(n_attempts))
        self.log("failure: " + str(self.failure) + " - success: " + str(self.success) + " - percentage failure: " + str(self.failure / (self.success + self.failure) * 100))

        if hasattr(e, "message"):
            message = e.message
            self.log("message: " + message)

        if hasattr(e, "response") and hasattr(e.response, "content"):
            content = e.response.content.decode('utf-8')
            dict_content = json.loads(content)
            dict_content_as_str = ' - '.join(f'{key}: {value}' for key, value in dict_content.items())

            self.log("content: " + json.dumps(dict_content_as_str))

    def _authentification(self):
        return self.marketApi and self.accountApi and  self.positionApi and self.orderApi

    def authentication_required(fn):
        """decoration for methods that require authentification"""
        def wrapped(self, *args, **kwargs):
            if not self._authentification():
                self.log("You must be authenticated to use this method {}".format(fn))
                return None
            else:
                return fn(self, *args, **kwargs)
        return wrapped

    def _get_symbol(self, coin, base="USDT"):
        if not hasattr(self, 'df_market'):
            self.df_market = self.get_future_market()
        if coin in self.df_market['symbol'].tolist():
            return coin
        if coin.endswith(base):
            base_coin = coin.replace(base, '')
            row = self.df_market[(self.df_market['baseCoin'] == base_coin) & (self.df_market['quoteCoin'] == base)]
            if not row.empty:
                return row['symbol'].values[0]

        symbol = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == base), "symbol"].values[0]
        return symbol

    def _get_symbol_v2(self, coin, base="USDT"):
        # hack : use market from v1 and adapt it for v2
        # todo : fill df_market_v2 with api v2 and read it
        symbol = self._get_symbol(coin, base)
        if symbol.endswith('_UMCBL'):
            symbol = symbol.replace('_UMCBL', '')
        return symbol

    def _get_coin(self, symbol, base = "USDT"):
        # self.get_future_market()
        if symbol in self.df_market['baseCoin'].tolist():
            return symbol

        if symbol in self.df_market['symbol'].tolist():
            row_index = self.df_market.index[(self.df_market['symbol'] == symbol) & (self.df_market['quoteCoin'] == base)].tolist()
            coin = self.df_market.at[row_index[0], "baseCoin"] if row_index else None
            del row_index
            return coin
        elif symbol.endswith(base):
            symbol = symbol.replace(base, '')
            if symbol in self.df_market['baseCoin'].tolist():
                return symbol

        self.log("WARNING COIN NOT IN MARKET LIST")
        return symbol.split("USDT_UMCBL")[0]

    def single_position(self, symbol, marginCoin = "USDT"):
        single_position = self.positionApi.single_position(self._get_symbol(symbol), marginCoin='USDT')
        return single_position

    #@authentication_required
    def get_open_position(self, by_pass=False):
        if (
                not by_pass
                and self.WS_ON
                and getattr(self, "ws_data", None) is not None
                and getattr(self, "ws_client", None) is not None
                and self.ws_client.get_status() == "On"
        ):
            return self.ws_get_open_position()
        else:
            if self.get_cache_status():
                df_from_cache = self.requests_cache_get("get_open_position")
                if isinstance(df_from_cache, pd.DataFrame):
                    return df_from_cache.copy()
            res = pd.DataFrame()
            n_attempts = 3
            while n_attempts > 0:
                try:
                    all_positions = self.positionApi.all_position(productType='umcbl',marginCoin='USDT')
                    lst_all_positions = [data for data in all_positions["data"] if float(data["total"]) != 0.]
                    res = self._build_df_open_positions(lst_all_positions)
                    del all_positions["data"]
                    del all_positions
                    del lst_all_positions
                    self.success += 1
                    break
                except (exceptions.BitgetAPIException, Exception) as e:
                    self.log_api_failure("positionApi.all_position", e, n_attempts)
                    time.sleep(0.2)
                    n_attempts = n_attempts - 1
            del n_attempts
            if self.get_cache_status():
                self.requests_cache_set("get_open_position", res.copy())
            return res

    def ws_get_open_position(self):
        return self.ws_data.get_ws_open_positions()

    def get_open_position_v2(self):
        if self.get_cache_status():
            df_from_cache = self.requests_cache_get("get_open_position_v2")
            if isinstance(df_from_cache, pd.DataFrame):
                return df_from_cache.copy()
        res = pd.DataFrame()
        n_attempts = 3
        while n_attempts > 0:
            try:
                all_positions = self.accountV2Api.allPosition(
                    {
                        "productType":"USDT-FUTURES",
                        "marginCoin":"USDT"
                    })
                lst_all_positions = [data for data in all_positions["data"] if float(data["total"]) != 0.]
                res = self._build_df_open_positions_v2(lst_all_positions)
                del all_positions["data"]
                del all_positions
                del lst_all_positions
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("positionApi.all_position_v2", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        del n_attempts
        if self.get_cache_status():
            self.requests_cache_set("get_open_position_v2", res.copy())
        return res

    # @authentication_required
    def get_open_orders(self, symbols):
        res = pd.DataFrame()
        for symbol in symbols:
            n_attempts = 3
            while n_attempts > 0:
                try:
                    all_orders = self.orderApi.current(symbol=symbol + "USDT_UMCBL")
                    # self.log("all_orders : ", all_orders)
                    lst_all_orders = [data for data in all_orders["data"]]
                    current_res = self._build_df_open_orders(lst_all_orders)
                    res = pd.concat([res, current_res])
                    del lst_all_orders
                    del current_res
                    self.success += 1
                    break
                except (exceptions.BitgetAPIException, Exception) as e:
                    self.log_api_failure("orderApi.current", e, n_attempts)
                    time.sleep(0.2)
                    n_attempts = n_attempts - 1
        return res

    # @authentication_required
    def get_triggers(self, plan_type="normal_plan"):
        params = {
            "planType": plan_type,
            "productType": "USDT-FUTURES"
        }

        if False and self.get_cache_status():
            df_from_cache = self.requests_cache_get("get_triggers_"+plan_type)
            if isinstance(df_from_cache, pd.DataFrame):
                return df_from_cache.copy()

        res = pd.DataFrame()
        n_attempts = 3
        while n_attempts > 0:
            try:
                response = self.orderV2Api.ordersPlanPending(params)
                if "data" in response and "entrustedList" in response["data"]:
                    lst_triggers = []
                    if response["data"]["entrustedList"]:
                        lst_triggers = [data for data in response["data"]["entrustedList"]]
                    current_res = self._build_df_triggers(lst_triggers)
                    res = pd.concat([res, current_res])
                    del lst_triggers
                del response
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("orderV2Api.ordersPlanPending", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        del n_attempts

        if self.get_cache_status():
            self.requests_cache_set("get_triggers_"+plan_type, res.copy())

        return res

    # @authentication_required
    def get_orders_plan_history(self, orderId, plan_type="normal_plan"):
        params = {
            "orderId": orderId,
            "planType": plan_type,
            "productType": "USDT-FUTURES"
        }

        res = pd.DataFrame()
        n_attempts = 3
        while n_attempts > 0:
            try:
                response = self.orderV2Api.ordersPlanHistory(params)
                if "data" in response and "entrustedList" in response["data"]:
                    lst_orders = []
                    if response["data"]["entrustedList"]:
                        lst_orders = [data for data in response["data"]["entrustedList"]]
                    current_res = self._build_df_orders_plan_history(lst_orders)
                    res = pd.concat([res, current_res])
                    del lst_orders
                del response
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("orderV2Api.ordersPlanHistory", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        del n_attempts

        return res

    def apply_func(self, row):
        self.add_gridId_orderId(row['gridId'], row['executeOrderId'], row['strategyId'], row['trend'])

    def add_plan_history(self, df):
        for orderId, plan_type in zip(df['orderId'], df['planType']):
            order_plan_history = self.get_orders_plan_history(orderId, plan_type)
            self.plan_history_list.append(order_plan_history)

    @authentication_required
    def get_all_triggers(self, by_pass=False):
        if (
                not by_pass
                and self.WS_ON
                and getattr(self, "ws_data", None) is not None
                and getattr(self, "ws_client", None) is not None
                and self.ws_client.get_status() == "On"
        ):
            return self.ws_get_all_triggers()
        else:
            lst_df_triggers = []
            with ThreadPoolExecutor() as executor:
                futures = []
                for plan_type in ["normal_plan", "track_plan", "profit_loss"]:
                    futures.append(executor.submit(self.get_triggers, plan_type))

                wait(futures, timeout=1000, return_when=ALL_COMPLETED)

                for future in futures:
                    lst_df_triggers.append(future.result())

            df_triggers = pd.concat(lst_df_triggers).reset_index(drop=True)

            merged = pd.merge(self.df_triggers_previous, df_triggers, on='orderId', suffixes=('_previous', '_current'),
                              how='outer', indicator=True)

            previous_columns = [col for col in merged.columns if col.endswith('_previous')]
            current_columns = [col.replace('_previous', '_current') for col in previous_columns]

            disappeared = merged[merged['_merge'] == 'left_only']

            df_disappeared = disappeared[['orderId'] + previous_columns].rename(
                columns={col: col.replace('_previous', '') for col in previous_columns})

            # CEDE: df_differences should not happen tbc
            if len(df_disappeared) > 0:
                # Replace the values in the 'planType' column based on the mapping dictionary
                df_disappeared['planType'] = df_disappeared['planType'].replace(self.plan_mapping)

                self.plan_history_list = []
                self.add_plan_history(df_disappeared)

                if len(self.plan_history_list) == len(df_disappeared):
                    if self.plan_history_list:
                        df_plan_history = pd.concat(self.plan_history_list, ignore_index=True)
                        self.plan_history_list = []

                        df_plan_history = df_plan_history.sort_values(by='orderId')
                        df_disappeared = df_disappeared.sort_values(by='orderId')

                        if 'executeOrderId' in df_plan_history.columns and 'planStatus' in df_plan_history.columns:
                            df_disappeared['executeOrderId'] = df_plan_history['executeOrderId'].tolist()
                            df_disappeared['planStatus'] = df_plan_history['planStatus'].tolist()

                        self.df_triggers_previous = df_triggers
                        df_triggers = pd.concat([df_triggers, df_disappeared], ignore_index=True)

                        df_filtered = df_triggers[(df_triggers['planStatus'] == 'executed') & (df_triggers['executeOrderId'] != '')]
                        if len(df_filtered) > 0:
                            df_filtered.apply(self.apply_func, axis=1)
                    else:
                        self.df_triggers_previous = df_triggers
            else:
                self.df_triggers_previous = df_triggers

            return df_triggers

    def ws_get_all_triggers(self):
        return self.ws_data.get_ws_triggers()

    def merge_plan_history(self, df):
        df_plan_history = pd.DataFrame()
        for orderId, plan_type in zip(df['orderId'].tolist(), df['planType'].tolist()):
            order_plan_history = self.get_orders_plan_history(orderId, plan_type)
            if df_plan_history.empty:
                df_plan_history = order_plan_history
            else:
                df_plan_history = pd.merge(df_plan_history, order_plan_history)
        return df_plan_history

    @authentication_required
    def get_current_state(self, lst_symbols):
        del self.current_state
        success = True

        with ThreadPoolExecutor() as executor:
            futures = []
            futures.append(executor.submit(self.get_open_orders, lst_symbols))
            futures.append(executor.submit(self.get_open_position))
            futures.append(executor.submit(self.get_all_triggers))
            futures.append(executor.submit(self.get_values, lst_symbols))
            wait(futures, timeout=1000, return_when=ALL_COMPLETED)

            # orders
            df_open_orders = futures[0].result()  # self.get_open_orders(lst_symbols)
            if isinstance(df_open_orders, pd.DataFrame) and "symbol" in df_open_orders.columns:
                df_open_orders['symbol'] = df_open_orders['symbol'].apply(lambda x: self._get_coin(x))
                lst_tmp = []
                for x in df_open_orders['symbol']:
                    coin = self._get_coin(x)
                    lst_tmp.append(coin)
                    del coin
                df_open_orders['symbol'] = lst_tmp
                df_open_orders.drop(['marginCoin', 'clientOid'], axis=1, inplace=True)
                df_open_orders = self.set_open_orders_gridId(df_open_orders)
                self.clear_gridId_orderId(df_open_orders["orderId"].to_list())
                del lst_tmp
            else:
                success = False

            # positions
            df_open_positions = futures[1].result()  # self.get_open_position()
            if isinstance(df_open_positions, pd.DataFrame) and "symbol" in df_open_positions.columns:
                df_open_positions['symbol'] = df_open_positions['symbol'].apply(self._get_coin)
                df_open_positions_filtered = df_open_positions[df_open_positions['symbol'].isin(lst_symbols)]

                if any(df_open_positions_filtered):
                    df_open_positions = df_open_positions_filtered
            else:
                success = False

            # triggers
            df_triggers = futures[2].result()
            if isinstance(df_triggers, pd.DataFrame) and 'symbol' in df_triggers.columns:
                df_triggers['symbol'] = df_triggers['symbol'].apply(self._get_coin)
            else:
                success = False
                if df_triggers is None:
                    print("None")
                print("Error: 'symbol' column not found in df_triggers")
                print(df_triggers.to_string())
                df_triggers['symbol'] = df_triggers['symbol'].apply(self._get_coin)

            # prices
            df_prices = futures[3].result()  # self.get_values(lst_symbols)
            if not isinstance(df_triggers, pd.DataFrame):
                success = False

            self.current_state = {
                "success": success,
                "open_orders": df_open_orders,
                "open_positions": df_open_positions,
                "triggers": df_triggers,
                "prices": df_prices
            }

            del df_triggers
            del df_open_orders
            del df_open_positions
            del df_prices
            del df_open_positions_filtered

        return self.current_state

    @authentication_required
    def reset_current_postion(self, current_state):
        if self.get_nb_order_current_state(current_state) > 0:
            self.log("init - reset current order")
            self.cancel_all_orders(self.get_lst_symbol_current_state(current_state))

    def get_lst_symbol_current_state(self, current_state):
        symbols = current_state["open_orders"]["symbol"].tolist()
        symbols = list(set(symbols))
        return symbols

    def get_lst_orderId_current_state(self, current_state):
        return current_state["open_orders"]["orderId"].tolist()

    def get_nb_order_current_state(self, current_state):
        return len(current_state["open_orders"])

    #@authentication_required
    def get_account_equity(self, by_pass=False):
        if (
                not by_pass
                and self.WS_ON
                and getattr(self, "ws_data", None) is not None
                and getattr(self, "ws_client", None) is not None
                and self.ws_client.get_status() == "On"
        ):
            return self.ws_get_account_equity()

        n_attempts = 3
        while n_attempts > 0:
            try:
                account_equity = self.positionApi.account(symbol='BTCUSDT_UMCBL',marginCoin='USDT')
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:

                self.log_api_failure("positionApi.account", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        usdtEquity = account_equity['data']['usdtEquity']
        del account_equity['data']
        del account_equity
        return usdtEquity

    #@authentication_required
    def get_account_available(self, by_pass=False):
        if (
                not by_pass
                and self.WS_ON
                and getattr(self, "ws_data", None) is not None
                and getattr(self, "ws_client", None) is not None
                and self.ws_client.get_status() == "On"
        ):
            return self.ws_get_account_available()

        n_attempts = 3
        while n_attempts > 0:
            try:
                account_equity = self.positionApi.account(symbol='BTCUSDT_UMCBL',marginCoin='USDT')
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("positionApi.account", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        return account_equity['data']['available']

    def ws_get_account_equity(self):
        return self.ws_current_state["account"]["usdtEquity"]

    def ws_get_account_available(self):
        return self.ws_current_state["account"]["available"]

    def ws_get_account_maxOpenPosAvailable(self):
        return self.ws_current_state["account"]["maxOpenPosAvailable"]

    '''
    marginCoin: Deposit currency
    size: It is quantity when the price is limited. The market price is the limit. The sales is the quantity
    side \\in {open_long, open_short, close_long, close_short}
    orderType \\in {limit(fixed price), market(market price)}
    returns :
    - transaction_id
    - transaction_price
    - transaction_size
    - transaction_fee
    '''
    @authentication_required
    def _place_order_api(self, symbol, marginCoin, size, side, orderType, clientOId, price='',
                         presetTakeProfitPrice='',
                         presetStopLossPrice=''):
        result = {}
        n_attempts = 10
        while n_attempts > 0:
            try:
                if isinstance(size, str):
                    size_order = float(size)
                else:
                    size_order = size
                result = self.orderApi.place_order(symbol, marginCoin, size_order, side, orderType,
                                                   price=price,
                                                   clientOrderId=clientOId, timeInForceValue='normal',
                                                   presetTakeProfitPrice=presetTakeProfitPrice,
                                                   presetStopLossPrice=presetStopLossPrice)
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("orderApi.place_order", e, n_attempts)
                if getattr(e, 'code', None) == '40762':
                    print("########## err: 40762 ##########")
                    if isinstance(size, str):
                        size = float(size)
                    size = size - size * 0.1 / 100
                    size = utils.normalize_size(size, self.get_sizeMultiplier(symbol))
                time.sleep(0.1)
                n_attempts = n_attempts - 1
        n_attempts = False
        locals().clear()
        return result

    @authentication_required
    def place_trail_order(self, symbol, marginCoin, triggerPrice, side, clientOid=None, triggerType=None, size=None, rangeRate=1):
        result = {}
        n_attempts = 3
        while n_attempts > 0:
            try:
                result = self.planApi.mix_place_trailing_stop_order(symbol, marginCoin, triggerPrice, side, clientOid, triggerType,
                                                                    size, rangeRate)
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:

                self.log_api_failure("planApi.place_trail_order", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        n_attempts = False
        locals().clear()
        return result

    @authentication_required
    def get_orders(self, symbol):
        result = {}
        n_attempts = 3
        while n_attempts > 0:
            try:
                result = self.orderApi.current(symbol)
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("planApi.current", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        n_attempts = False
        locals().clear()
        return result

    @authentication_required
    def place_tpsl_order(self, symbol, marginCoin, triggerPrice, planType, holdSide, triggerType=None, size=None, rangeRate=None, clientOid=None):
        result = {}
        try:
            result = self.planApi.place_tpsl(symbol, marginCoin, triggerPrice, triggerType, planType, holdSide, size, rangeRate, clientOid)
            print("place_tpsl_order - success -", holdSide, " at: ", triggerPrice)
        except (exceptions.BitgetAPIException, Exception) as e:
            self.log_api_failure("planApi.place_tpsl", e)
        locals().clear()
        return result

    @authentication_required
    def get_order_detail_status(self, params):
        return self.orderApi.detail(self._get_symbol(params["symbol"]),
                                    params["orderId"])

    @authentication_required
    def _place_TPSL_Order_v2(self, symbol, marginCoin, planType, triggerPrice,
                             triggerType, executePrice, holdSide, size, rangeRate, clientOid):

        params = {}
        params["symbol"] = self._get_symbol_v2(symbol)
        params["marginCoin"] = "USDT"
        params["productType"] = "USDT-FUTURES"
        params["planType"] = planType
        params["triggerPrice"] = triggerPrice
        params["executePrice"] = executePrice
        params["triggerType"] = triggerType
        # params["executePrice"] = executePrice
        params["size"] = size
        params["holdSide"] = holdSide
        if rangeRate != '':
            params["rangeRate"] = rangeRate
        params["clienOid"] = clientOid

        result = {}
        n_attempts = 3
        while n_attempts > 0:
            try:
                result = self.orderV2Api.placeTPSLOrder(params)
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("planApi.place_plan_v2", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        n_attempts = False
        locals().clear()
        return result


    @authentication_required
    def _cancel_Plan_Order_v1(self, symbol, marginCoin, orderId, planType):
        # https://bitgetlimited.github.io/apidoc/en/mix/#cancel-plan-order-tpsl
        # planType : https://bitgetlimited.github.io/apidoc/en/mix/#plantype

        result = {}
        n_attempts = 3
        while n_attempts > 0:
            try:
                result = self.planApi.cancel_plan(symbol, marginCoin, orderId, planType)
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("planApi.cancel_plan", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        n_attempts = False
        locals().clear()
        return result

    @authentication_required
    def _cancel_Batch_Order_v2(self, symbol, lst_order_id):
        orderIdList = []
        for order_id in lst_order_id:
            if order_id != "":
                cancel_orderId = {
                    "orderId": order_id
                }
                orderIdList.append(cancel_orderId)
        if len(orderIdList) == 0:
            return None

        params = {
            "symbol": self._get_symbol_v2(symbol),
            "marginCoin": "USDT",
            "productType": "USDT-FUTURES",
            "orderIdList": orderIdList
        }

        result = {}
        n_attempts = 3
        while n_attempts > 0:
            try:
                result = self.orderV2Api.batchCancelOrders(params)
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("orderV2Api.batchCancelOrders", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        n_attempts = False
        locals().clear()
        return result

    @authentication_required
    def _cancel_Plan_Order_v2(self, symbol, marginCoin, lst_order_id):
        orderIdList = []
        for order_id in lst_order_id:
            if order_id != "":
                cancel_orderId = {
                    "orderId": order_id,
                    "clientOid": ""
                }
                orderIdList.append(cancel_orderId)
        if len(orderIdList) == 0:
            return None

        params = {
            "marginCoin": "USDT",
            "productType": "USDT-FUTURES",
            "symbol": self._get_symbol_v2(symbol),
            "orderIdList": orderIdList
        }

        result = {}
        n_attempts = 3
        while n_attempts > 0:
            try:
                result = self.orderV2Api.cancelPlanOrder(params)
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("orderV2Api.cancelPlanOrder", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        n_attempts = False
        locals().clear()
        return result

    # reference : https://www.bitget.com/api-doc/contract/plan/Place-Plan-Order
    @authentication_required
    def _place_trigger_order_v2(self, symbol, planType, triggerPrice, marginCoin, size, side, tradeSide, reduceOnly,
                                orderType, triggerType, clientOid, callbackRatio, price='',
                                sl='', tp='', stopLossTriggerType='', stopSurplusTriggerType=''):
        params = {}
        params["symbol"] = self._get_symbol_v2(symbol)
        params["planType"] = planType
        params["triggerPrice"] = triggerPrice
        params["marginCoin"] = "USDT"
        params["size"] = size
        params["side"] = side
        params["tradeSide"] = tradeSide
        params["reduceOnly"] = reduceOnly
        params["orderType"] = orderType
        params["triggerType"] = triggerType
        params["clienOid"] = clientOid
        if callbackRatio != "":
            params["callbackRatio"] = callbackRatio
        if price != "":
            params["price"] = price
        if sl != "":
            params["stopLossTriggerPrice"] = sl
        if tp != "":
            params["stopSurplusTriggerPrice"] = tp
        if stopLossTriggerType != "":
            params["stopLossTriggerType"] = stopLossTriggerType
        if stopSurplusTriggerType != "":
            params["stopSurplusTriggerType"] = stopSurplusTriggerType

        params["productType"] = "USDT-FUTURES"
        params["marginMode"] = "crossed"
        # params["marginMode"] = "cross" # CEDE to be fixed
        #params["stopSurplusTriggerPrice"] =  # optional
        #params["stopSurplusExecutePrice"] =  # optional
        #params["stopSurplusTriggerType"] =  # optional
        #params["stopLossTriggerPrice"] =  # optional
        #params["stopLossExecutePrice"] =  # optional
        #params["stopLossTriggerType"] = # optional
        result = {}
        n_attempts = 3
        while n_attempts > 0:
            try:
                result = self.orderV2Api.placePlanOrder(params)
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                result = getattr(e, "message", "")
                self.log_api_failure("planApi.place_plan_v2", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        n_attempts = False
        locals().clear()
        return result

    # reference : https://www.bitget.com/api-doc/contract/plan/Place-Plan-Order
    @authentication_required
    def _place_trigger_order(self, symbol, margin_coin, size, side, order_type, client_oid, trigger_params, price=''):
        params = {}
        params["planType"] = "normal_plan"
        params["symbol"] = symbol
        params["productType"] = "USDT-FUTURES"
        params["marginMode"] = "isolated"
        params["marginCoin"] = margin_coin
        params["size"] = size
        if price != "":
            params["price"] = price  # optional
        #params["callbackRatio"] =  # optional
        params["triggerPrice"] = trigger_params["trigger_price"]
        params["triggerType"] = "fill_price"
        params["side"] = side
        #params["tradeSide"] =  # optional
        params["orderType"] = order_type
        params["clienOid"] = client_oid
        # params["reduceOnly"] = "YES"
        #params["stopSurplusTriggerPrice"] =  # optional
        #params["stopSurplusExecutePrice"] =  # optional
        #params["stopSurplusTriggerType"] =  # optional
        #params["stopLossTriggerPrice"] =  # optional
        #params["stopLossExecutePrice"] =  # optional
        #params["stopLossTriggerType"] = # optional
        result = {}
        n_attempts = 3
        while n_attempts > 0:
            try:
                result = self.planApi.place_plan(symbol, margin_coin, str(size), side, order_type, str(trigger_params["trigger_price"]), "market_price", str(price), client_oid)

                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("planApi.place_plan", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        n_attempts = False
        locals().clear()
        return result

    @authentication_required
    def cancel_all_triggers_with_plan_type(self, plan_type, product_type="umcbl"):
        result = {}
        n_attempts = 3
        while n_attempts > 0:
            try:
                result = self.planApi.cancel_all_plans(product_type, plan_type)

                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("planApi.cancel_all_plans", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        n_attempts = False
        locals().clear()
        return result

    def cancel_all_triggers(self, product_type="umcbl"):
        self.cancel_all_triggers_with_plan_type("normal_plan", product_type)
        self.cancel_all_triggers_with_plan_type("track_plan", product_type)

    @authentication_required
    def _batch_orders_api(self, symbol, marginCoin, batch_order):

        result = self.orderApi.batch_orders(symbol, marginCoin, batch_order)

        locals().clear()
        return result

    @authentication_required
    def _batch_orders_api_V2(self, symbol, marginCoin, batch_order):
        params = {'symbol': symbol,  "productType": "usdt-futures", "marginMode": "crossed", 'marginCoin': marginCoin, 'orderList': batch_order}

        return self.orderV2Api.batchPlaceOrder(params)


    @authentication_required
    def _batch_cancel_orders_api(self, symbol, marginCoin, lst_ordersIds):

        result = self.orderApi.cancel_batch_orders(symbol, marginCoin, lst_ordersIds)

        locals().clear()
        return result

    @authentication_required
    def get_portfolio_value(self):
        return self.get_usdt_equity(by_pass=True)

    def get_info(self):
        return None, None, None

    def _get_open_order_detail(self, symbol, orderId):
        return self.orderApi.detail(symbol, orderId)

    @authentication_required
    def _open_long_position(self, symbol, amount, clientoid,
                            presetTakeProfitPrice="",
                            presetStopLossPrice=""):
        return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='open_long', orderType='market',
                                     clientOId=clientoid,
                                     presetTakeProfitPrice=presetTakeProfitPrice,
                                     presetStopLossPrice=presetStopLossPrice
                                     )

    @authentication_required
    def _close_long_position(self, symbol, amount, clientoid):
        return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='close_long', orderType='market', clientOId=clientoid)

    @authentication_required
    def _open_short_position(self, symbol, amount, clientoid,
                             presetTakeProfitPrice = "",
                             presetStopLossPrice = ""):
        return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='open_short', orderType='market',
                                     clientOId=clientoid,
                                     presetTakeProfitPrice=presetTakeProfitPrice,
                                     presetStopLossPrice=presetStopLossPrice
                                     )

    @authentication_required
    def _close_short_position(self, symbol, amount, clientoid):
        return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='close_short', orderType='market', clientOId=clientoid)

    @authentication_required
    def _open_long_order(self, symbol, amount, client_oid, price, trigger_params=None):
        if trigger_params:
            return self._place_trigger_order(symbol, margin_coin="USDT", size=amount, side='open_long',
                                             order_type='limit', price=price, client_oid=client_oid, trigger_params=trigger_params)
        else:
            return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='open_long',
                                         orderType='limit', price=price, clientOId=client_oid)

    @authentication_required
    def _close_long_order(self, symbol, amount, client_oid, price, trigger_params=None):
        if trigger_params:
            return self._place_trigger_order(symbol, margin_coin="USDT", size=amount, side='Sell',
                                             order_type='limit', price=price, client_oid=client_oid, trigger_params=trigger_params)
        else:
            return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='close_long',
                                         orderType='limit', price=price, clientOId=client_oid)

    @authentication_required
    def _open_short_order(self, symbol, amount, client_oid, price, trigger_params=None):
        if trigger_params:
            return self._place_trigger_order(symbol, margin_coin="USDT", size=amount, side='Sell',
                                             order_type='limit', price=price, client_oid=client_oid,
                                             trigger_params=trigger_params)
        else:
            return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='open_short',
                                         orderType='limit', price=price, clientOId=client_oid)

    @authentication_required
    def _close_short_order(self, symbol, amount, client_oid, price, trigger_params=None):
        if trigger_params:
            return self._place_trigger_order(symbol, margin_coin="USDT", size=amount, side='Buy',
                                             order_type='limit', price=price, client_oid=client_oid,
                                             trigger_params=trigger_params)
        else:
            return self._place_order_api(symbol, marginCoin="USDT", size=amount, side='close_short',
                                         orderType='limit', price=price, clientOId=client_oid)

    @authentication_required
    def get_wallet_equity(self):
        n_attempts = 3
        while n_attempts > 0:
            try:
                self.get_list_of_account_assets()
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("get_list_of_account_assets", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        return self.df_account_assets['usdtEquity'].sum()

    @authentication_required
    def get_usdt_equity_available(self, by_pass=False):
        if (
                not by_pass
                and self.WS_ON
                and getattr(self, "ws_data", None) is not None
                and getattr(self, "ws_client", None) is not None
                and self.ws_client.get_status() == "On"
        ):
            usdt_equity, available = self.ws_get_usdt_equity_available()
            if usdt_equity is not None and available is not None:
                return float(usdt_equity), float(available)

        self.df_account_assets = None  # reset self.df_account_assets
        n_attempts = 3
        while n_attempts > 0:
            try:
                self.get_list_of_account_assets()
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("get_list_of_account_assets", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        if isinstance(self.df_account_assets, pd.DataFrame):
            cell_usdtEquity = self.df_account_assets.loc[(self.df_account_assets['baseCoin'] == 'USDT') & (self.df_market['quoteCoin'] == 'USDT'), "usdtEquity"]
            cell_avaialable = self.df_account_assets.loc[(self.df_account_assets['baseCoin'] == 'USDT') & (self.df_market['quoteCoin'] == 'USDT'), "available"]
            if len(cell_usdtEquity.values) > 0 and len(cell_avaialable.values) > 0:
                return cell_usdtEquity.values[0], cell_avaialable.values[0]
        return None, None

    def ws_get_usdt_equity_available(self):
        return self.ws_data.get_usdt_equity_available()

    @authentication_required
    def get_usdt_equity(self, by_pass=False):
        if (
                not by_pass
                and self.WS_ON
                and getattr(self, "ws_data", None) is not None
                and getattr(self, "ws_client", None) is not None
                and self.ws_client.get_status() == "On"
        ):
            usdt_equity, _ = self.ws_get_usdt_equity_available()
            if usdt_equity is not None:
                return float(usdt_equity)

        self.df_account_assets = None  # reset self.df_account_assets
        n_attempts = 3
        while n_attempts > 0:
            try:
                self.get_list_of_account_assets()
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("get_list_of_account_assets", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        if isinstance(self.df_account_assets, pd.DataFrame):
            cell = self.df_account_assets.loc[(self.df_account_assets['baseCoin'] == 'USDT') & (self.df_market['quoteCoin'] == 'USDT'), "usdtEquity"]
            if len(cell.values) > 0:
                return cell.values[0]
            else:
                return 0
        return None

    @authentication_required
    def get_spot_usdt_equity(self, lst_symbols):
        n_attempts = 3
        while n_attempts > 0:
            try:
                df_spot_usdt_equity = self._get_df_spot_account(lst_symbols)
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("_get_df_spot_account", e, n_attempts)
                time.sleep(0.2)
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
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("get_list_of_account_assets", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1

        if len(self.df_account_assets) == 0:
            return 0, 0, 0
        else:
            available = self.df_account_assets.loc[self.df_account_assets["symbol"] == baseCoin, "available"].values[0]
            crossMaxAvailable = self.df_account_assets.loc[self.df_account_assets["symbol"] == baseCoin, "crossMaxAvailable"].values[0]
            fixedMaxAvailable = self.df_account_assets.loc[self.df_account_assets["symbol"] == baseCoin, "fixedMaxAvailable"].values[0]

            actualPrice = self.df_account_assets.loc[self.df_account_assets["symbol"] == baseCoin, "actualPrice"].values[0]

            return available * actualPrice, crossMaxAvailable, fixedMaxAvailable

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
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("get_list_of_account_assets", e, n_attempts)
                time.sleep(0.2)
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
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("orderApi.history", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        df_history = pd.DataFrame(columns=["symbol", "size", "side", "orderId", "filledQty", "leverage", "fee", "orderType", "marginCoin", "totalProfits", "cTime", "uTime"])
        orderList = history["data"]["orderList"]
        if orderList:
            for i in range(len(orderList)):
                data = orderList[i]
                df_history.loc[i] = pd.Series({"symbol": data["symbol"], "size": data["size"], "side": data["side"], "orderId": data["orderId"], "filledQty": data["filledQty"], "leverage": int(data["leverage"]),"fee": float(data["fee"]),"orderType": data["orderType"],"marginCoin": data["marginCoin"],"totalProfits": float(data["totalProfits"]),"cTime": utils.convert_ms_to_datetime(data["cTime"]),"uTime": utils.convert_ms_to_datetime(data["uTime"])})
        return df_history

    @authentication_required
    def get_value(self, symbol, by_pass=False):
        if (
                not by_pass
                and self.WS_ON
                and getattr(self, "ws_data", None) is not None
                and getattr(self, "ws_client", None) is not None
                and self.ws_client.get_status() == "On"
        ):
            ws_value = self.ws_get_value(symbol)
            if ws_value != None:
                return ws_value

        if not symbol.endswith('USDT_UMCBL'):
            symbol += 'USDT_UMCBL'
        value = 0
        n_attempts = 300
        while n_attempts > 0:
            try:
                # value = float(self.marketApi.market_price(symbol)["data"]["markPrice"])
                value = float(self.marketApi.ticker(symbol)["data"]["last"])
                if value != 0:
                    self.success += 1
                    return value
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("marketApi.ticker", e, n_attempts)
                time.sleep(1.)
                n_attempts = n_attempts - 1
        exit(123)

    def ws_get_value(self, symbol):
        return self.ws_data.get_value(symbol)

    def ws_get_values(self, symbols):
        return self.ws_data.get_values(symbols)

    def get_values(self, symbols, by_pass=False):
        if (
                not by_pass
                and self.WS_ON
                and getattr(self, "ws_data", None) is not None
                and getattr(self, "ws_client", None) is not None
                and self.ws_client.get_status() == "On"
        ):
            self.df_prices = self.ws_get_values(symbols)
            if self.df_prices is not None:
                return self.df_prices

        values = []
        timestamps = []
        now = datetime.now()
        current_timestamp = datetime.timestamp(now)
        for symbol in symbols:
            val = self.get_value(symbol)
            values.append(val)
            timestamps.append(current_timestamp)
            del val
        self.df_prices = pd.DataFrame({
            "timestamp": timestamps,
            "symbols": symbols,
            "values": values
        })
        del values
        del symbols
        return self.df_prices

    @authentication_required
    def get_asset_available(self, symbol):
        symbol = symbol + 'USDT_UMCBL'
        value = 0
        n_attempts = 3
        while n_attempts > 0:
            try:
                value = float(self.marketApi.market_price(symbol)["data"]["total"])
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("marketApi.market_price", e, n_attempts)
                time.sleep(0.2)
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
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("marketApi.market_price", e, n_attempts)
                time.sleep(0.2)
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
        columns = [
            'symbol', 'marginCoin', 'available', 'crossMaxAvailable', 'fixedMaxAvailable',
            'equity', 'usdtEquity', 'locked', 'btcEquity', 'unrealizedPL',
            'size', 'actualPrice', 'quoteCoin', 'baseCoin', 'symbolType',
            'makerFeeRate', 'takerFeeRate', 'minTradeNum', 'priceEndStep',
            'volumePlace', 'pricePlace', 'sizeMultiplier'
        ]

        rows = []
        for market in markets['data']:
            equity = float(market['equity'])
            usdt_equity = float(market['usdtEquity'])
            btc_equity = float(market['btcEquity'])

            if equity > 0 and usdt_equity > 0 and btc_equity > 0:
                # Ensure the behavior matches the original: check for None explicitly.
                unrealized_pl = 0.0 if market['unrealizedPL'] is None else float(market['unrealizedPL'])

                row = [
                    market['marginCoin'],  # symbol
                    market['marginCoin'],  # marginCoin (same as symbol, as in the original code)
                    float(market['available']),
                    float(market['crossMaxAvailable']),
                    float(market['fixedMaxAvailable']),
                    equity,
                    usdt_equity,
                    float(market['locked']),
                    btc_equity,
                    unrealized_pl,
                    0.0, 0.0, "", "", "", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
                ]
                rows.append(row)

        return pd.DataFrame(rows, columns=columns)

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
        try:
            dct_market = self.marketApi.contracts('umcbl')
            df_market_umcbl = self._market_results_to_df(dct_market)
            dct_market = self.marketApi.contracts('dmcbl')
            df_market_dmcbl = self._market_results_to_df(dct_market)
            dct_market = self.marketApi.contracts('cmcbl')
            df_market_cmcbl = self._market_results_to_df(dct_market)
            return pd.concat([df_market_umcbl, df_market_dmcbl, df_market_cmcbl]).reset_index(drop=True)
        except (exceptions.BitgetAPIException, Exception) as e:
            self.log_api_failure("accountApi.accounts", e)
            return None

    def _account_available(self, markets):
        for market in markets['data']:
            if market['marginCoin'] == "USDT":
                return float(market['available'])
        return None

    def _get_usdt_availbale(self, acc_type='umcbl'):
        dct_account = self.accountApi.accounts(acc_type)
        return self._account_available(dct_account)

    def _get_df_account(self):
        # update market
        not_usdt = False
        if not_usdt:
            n_attempts = 3
            while n_attempts > 0:
                try:
                    dct_account = self.accountApi.accounts('umcbl')
                    df_account_umcbl = self._account_results_to_df(dct_account)
                    dct_account = self.accountApi.accounts('dmcbl')
                    df_account_dmcbl =  self._account_results_to_df(dct_account)
                    dct_account = self.accountApi.accounts('cmcbl')
                    df_account_cmcbl = self._account_results_to_df(dct_account)
                    df_account_assets = pd.concat([df_account_umcbl, df_account_dmcbl, df_account_cmcbl])
                    df_account_umcbl = None
                    df_account_dmcbl = None
                    df_account_cmcbl = None
                except (exceptions.BitgetAPIException, Exception) as e:
                    self.log_api_failure("accountApi.accounts", e, n_attempts)
                    time.sleep(0.2)
                    n_attempts = n_attempts - 1
        else:
            dct_account = self.accountApi.accounts('umcbl')
            df_account_assets = self._account_results_to_df(dct_account)

        self.df_account_open_position = None
        self.df_account_open_position = self.get_open_position()
        self.df_account_open_position.rename(columns={'usdEquity': 'usdtEquity', 'total': 'size'}, inplace=True)
        self.df_account_open_position['equity'] = self.df_account_open_position['usdtEquity']
        # self.df_account_assets = pd.concat([df_account_assets, self.df_account_open_position])
        if not self.df_account_open_position.empty:
            # Concatenate the DataFrames
            self.df_account_assets = pd.concat([df_account_assets, self.df_account_open_position])
        else:
            # If df_account_open_position is empty, just assign df_account_assets
            self.df_account_assets = df_account_assets
        self.df_account_assets.reset_index(inplace=True, drop=True)
        del df_account_assets

    def _get_df_spot_account(self, lst_symbols):
        df_spot_asset = pd.DataFrame(columns=["symbol", "size", "price", "equity"])
        for symbol in lst_symbols:
            spot_asset = self.accountApi.assets_spot(symbol)
            spot_asset_size = float(spot_asset["data"][0]["total"])
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
        self.log(self.df_account_assets)

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
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("accountApi.accountAssets", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        return result

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
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("orderApi.cancel_orders", e, n_attempts)
                if e.code == '40768':
                    self.log("Warning : Order does not exist")
                    result = {"msg": "success", "data": {"orderId": orderId}}
                    break
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        if result.get("msg", "") == "success":
            return True, result["data"]["orderId"]
        else:
            return False , False

    # example : cancel_all_orders(["BTC", "ETH", "XRP"])
    def cancel_all_orders(self, lst_symbols):
        df_open_orders = self.get_open_orders(lst_symbols)
        for index, row in df_open_orders.iterrows():
            self.cancel_order(row["symbol"], row["marginCoin"], row["orderId"])
        df_open_orders = None

    def get_order_fill_detail(self, symbol, order_id):
        trade_id = price = fillAmount = sizeQty = fee = None
        n_attempts = 3
        while n_attempts > 0:
            try:
                response = self.orderApi.fills(symbol, order_id)
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("orderApi.fills", e, n_attempts)
                time.sleep(0.2)
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
            self.log("get_order_fill_detail multiple order")

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
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("marketApi.get_symbol_leverage", e, n_attempts)
                time.sleep(0.2)
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
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("accountApi.account", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        return dct_account['data']['crossMarginLeverage'], dct_account['data']['fixedLongLeverage'], dct_account['data']['fixedShortLeverage']

    @authentication_required
    def get_account_symbols_leverage(self, lst_symbols, marginCoin="USDT"):
        df_leverages = pd.DataFrame(columns=["symbol", "crossMarginLeverage", "fixedLongLeverage", "fixedShortLeverage"])

        with ThreadPoolExecutor() as executor:
            futures = []
            for symbol in lst_symbols:
                futures.append(executor.submit(self.get_account_symbol_leverage, symbol, marginCoin))

            wait(futures, timeout=1000, return_when=ALL_COMPLETED)

            for index, future in enumerate(futures):
                result = future.result()
                symbol = lst_symbols[index]
                df_leverages.loc[len(df_leverages)] = [symbol, result[0], result[1], result[2]]

        return df_leverages


    @authentication_required
    def set_account_symbol_leverage(self, symbol, leverage, hold="long"):
        n_attempts = 3
        dct_account = None
        while n_attempts > 0:
            try:
                symbol = self._get_symbol(symbol, "USDT")
                dct_account = self.accountApi.leverage(symbol, "USDT", leverage, hold)
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("accountApi.leverage", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        if dct_account:
            crossMarginLeverage = dct_account['data']['crossMarginLeverage']
            longLeverage = dct_account['data']['longLeverage']
            shortLeverage = dct_account['data']['shortLeverage']
            dct_account['data'].clear()
            del dct_account['data']
            dct_account.clear()
            del dct_account
            del n_attempts
            del symbol
            del leverage
            del hold
            return crossMarginLeverage, longLeverage, shortLeverage
        return None, None, None

    @authentication_required
    def set_account_symbol_margin(self, symbol, marginMode="fixed"):
        n_attempts = 3
        while n_attempts > 0:
            try:
                symbol = self._get_symbol(symbol, "USDT")
                dct_account = self.accountApi.margin_mode(symbol, "USDT", marginMode)
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("accountApi.margin_mode", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        margin_mode = dct_account['data']['marginMode']
        dct_account["data"].clear()
        del dct_account['data']
        dct_account.clear()
        del dct_account
        del symbol
        del n_attempts
        return margin_mode

    def get_min_order_amount(self, symbol):
        if "_" in symbol:
            symbol = symbol[:3]+"USDT_SPBL"
        else:
            symbol += "USDT_SPBL"
        product = self.publicApi.product(symbol)
        minTradeAmount = float(product["data"]["minTradeAmount"])
        del product["data"]
        del product
        del symbol
        return minTradeAmount

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

    def get_df_minimum_size(self, lst_symbols):
        lst = [self.get_minimum_size(symbol) for symbol in lst_symbols]
        lst_leverage_long = [self.get_leverage_long(self._get_symbol(symbol)) for symbol in lst_symbols]
        lst_leverage_short = [self.get_leverage_short(self._get_symbol(symbol)) for symbol in lst_symbols]
        df = pd.DataFrame({'symbol': lst_symbols, 'minBuyingSize': lst,'buyingSize': lst, 'leverage_long': lst_leverage_long, 'leverage_short': lst_leverage_short})
        return df

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
            pricePlace = self.df_market.at[idx[0], "pricePlace"]
            del symbol
            del idx
            return pricePlace
        elif symbol in self.df_market['symbol'].tolist():
            idx = self.df_market[  (self.df_market['symbol'] == symbol) & (self.df_market['quoteCoin'] == "USDT")  ].index
            pricePlace = self.df_market.at[idx[0], "pricePlace"]
            del idx
            del symbol
            return pricePlace
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
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("set_account_symbol_leverage", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        self.log(symbol + ' long leverage: ' + str(longLeverage) + ' short leverage: ' + str(shortLeverage))
        del crossMarginLeverage
        del longLeverage
        del shortLeverage
        del n_attempts
        del symbol
        del leverage
        del hold

    def set_symbol_margin(self, symbol, margin):
        n_attempts = 3
        set_symbol_margin = False
        while n_attempts > 0:
            try:
                set_symbol_margin = self.set_account_symbol_margin(symbol, margin)
                self.log(symbol + ' margin set to ' + margin + " " + set_symbol_margin)
                self.success += 1
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("set_account_symbol_margin", e, n_attempts)
                time.sleep(0.2)
                n_attempts = n_attempts - 1
        del n_attempts
        del symbol
        del margin
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

    def generateRangePrices(self, symbol, amount, range_percentage, nb_intervals, nb_values):
        amount = self.normalize_price(symbol, amount)
        start = self.normalize_price(symbol, amount - (amount * range_percentage / 2))
        end = self.normalize_price(symbol, amount + (amount * range_percentage / 2))

        lst_prices = np.linspace(start, end, nb_intervals)
        lst_normalized_prices = [self.normalize_price(symbol, price) for price in lst_prices]
        lst_normalized_prices = list(set(lst_normalized_prices))
        lst_normalized_prices.sort(key=lambda x: abs(x - amount))
        # CEDE WARNING TO BE TESTED WITH BIGGER VALUES
        # lst_str_prices = [str(price) for price in lst_normalized_prices]
        lst_str_prices = [f'{price:.10f}'.rstrip('0').rstrip('.') for price in lst_normalized_prices]

        lst_first_nb_values_elements = lst_str_prices[:nb_values]

        return lst_first_nb_values_elements

    def normalize_price(self, symbol, amount):
        if symbol in self.df_normalize_price["symbol"].tolist():
            index_with_value = self.df_normalize_price.index[self.df_normalize_price['symbol'] == symbol][0]
            pricePlace = int(self.df_normalize_price.at[index_with_value, 'pricePlace'])
            priceEndStep = self.df_normalize_price.at[index_with_value, 'priceEndStep']
            del index_with_value
        else:
            pricePlace = int(self.get_pricePlace(symbol))
            priceEndStep = self.get_priceEndStep(symbol)
            new_row_data = [symbol, pricePlace, priceEndStep]
            self.df_normalize_price.loc[len(self.df_normalize_price)] = new_row_data
            del new_row_data

        amount = amount * pow(10, pricePlace)
        amount = math.floor(amount)
        amount = amount * pow(10, -pricePlace)

        amount = round(amount, pricePlace)

        # Calculate the decimal without using %
        decimal_multiplier = priceEndStep * pow(10, -pricePlace)
        decimal = amount - math.floor(round(amount / decimal_multiplier)) * decimal_multiplier

        amount = amount - decimal
        amount = round(amount, pricePlace)
        del pricePlace
        del decimal
        del decimal_multiplier
        del priceEndStep
        del symbol
        return amount

    def normalize_size(self, symbol, size):
        if symbol in self.df_normalize_size["symbol"].tolist():
            index_with_value = self.df_normalize_size.index[self.df_normalize_size['symbol'] == symbol][0]
            volumePlace = self.df_normalize_size.at[index_with_value, 'volumePlace']
            sizeMultiplier = self.df_normalize_size.at[index_with_value, 'sizeMultiplier']
            minsize = self.df_normalize_size.at[index_with_value, 'minsize']
            del index_with_value
        else:
            volumePlace = self.get_volumePlace(symbol)
            sizeMultiplier = self.get_sizeMultiplier(symbol)
            minsize = self.get_minimum_size(symbol)
            new_row_data = [symbol, volumePlace, sizeMultiplier, minsize]
            self.df_normalize_size.loc[len(self.df_normalize_size)] = new_row_data
            del new_row_data

        size = size * pow(10, volumePlace)
        size = math.floor(size)
        size = size * pow(10, -volumePlace)

        decimal = (size % sizeMultiplier)
        size = size - decimal
        size = round(size, volumePlace)
        if size < minsize:
            return 0
        del decimal
        del volumePlace
        del sizeMultiplier
        del minsize
        del symbol
        return size

    def get_price_place_endstep(self, lst_symbol):
        lst = []
        for symbol in lst_symbol:
            dct = {}
            dct['symbol'] = symbol
            dct['pricePlace'] = int(self.get_pricePlace(symbol))
            dct['priceEndStep'] = self.get_priceEndStep(symbol)
            dct['sizeMultiplier'] = self.get_sizeMultiplier(symbol)
            lst.append(dct)
        return lst

    def requests_cache_clear(self):
        self.requests_cache.clear()
        self.requests_cache = {}

    def requests_cache_set(self, key, value):
        self.requests_cache[key] = value

    def requests_cache_get(self, key):
        if key in self.requests_cache:
            return self.requests_cache[key]
        return None

    def enable_cache(self):
        self.enable_cache_data = True

    def disable_cache(self):
        self.requests_cache_clear()
        self.enable_cache_data = False

    def get_cache_status(self):
        return self.enable_cache_data

    def process_data(self, result):
        # data = json.loads(result)

        # Step 3: Process the data (convert timestamp, format numbers, etc.)
        processed_data = []
        for entry in result:
            timestamp = int(entry[0])  # Convert the timestamp from string to integer
            open_price = float(entry[1])
            high_price = float(entry[2])
            low_price = float(entry[3])
            close_price = float(entry[4])

            # Create a dictionary or any structure you need
            processed_entry = {
                "timestamp": timestamp,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price
            }

            # Append to the list of processed data
            processed_data.append(processed_entry)

        df_result = pd.DataFrame(processed_data)
        return df_result

    def fetch_candles(self, symbol, granularity, start_time, end_time):
        """
        Fetch candles for a specific time range.
        """
        try:
            response = self.marketApi.candles(self._get_symbol(symbol), str(granularity), str(int(start_time)), str(int(end_time)))
            # response.raise_for_status()
            data = self.process_data(response)
            # data = response.json()
            return data
        except Exception as e:
            print(f"Error fetching data for time range {start_time} - {end_time}: {e}")
            return []

    def divide_time_ranges(self, start_time, end_time, granularity, max_limit=100):
        """
        Divide the total time range into chunks based on the maximum limit per request.
        """
        ranges = []
        convert = {
            "5m": 60*5
        }
        delta = convert[granularity] * 1000 * max_limit  # Convert granularity to milliseconds

        current_start = start_time
        while current_start < end_time:
            current_end = min(current_start + delta, end_time)
            ranges.append((current_start, current_end))
            current_start = current_end + convert[granularity] * 1000  # Move to the next interval without overlap
        return ranges

    def fetch_historical_data_multithreaded(self, symbol, granularity, start_time, end_time, max_workers=5):
        """
        Fetch historical data using multithreading.
        """
        # Divide the total time range into smaller ranges
        time_ranges = self.divide_time_ranges(start_time, end_time, granularity)

        data_frames = []
        convert = {
            "5m": 60*5
        }

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Dictionary to hold future objects and their corresponding time ranges
            future_to_time_range = {executor.submit(self.fetch_candles, symbol, granularity, tr[0], tr[1]): tr for tr in
                                    time_ranges}

            for future in as_completed(future_to_time_range):
                tr = future_to_time_range[future]
                try:
                    data = future.result()
                    if isinstance(data, pd.DataFrame) and not data.empty:
                        data_frames.append(data)
                        print(f"Fetched {len(data)} records for time range {tr}")
                    else:
                        print(f"No data returned for time range {tr}")
                except Exception as exc:
                    print(f"Generated an exception for time range {tr}: {exc}")

        if data_frames:
            all_data = pd.concat(data_frames, ignore_index=True)
        else:
            all_data = pd.DataFrame()

        # Remove duplicates and sort the data by timestamp
        # unique_data = {item[0]: item for item in all_data}
        # sorted_data = [unique_data[key] for key in sorted(unique_data.keys(), key=int)]
        sorted_data = all_data.sort_values(by='timestamp').reset_index(drop=True)
        return sorted_data

    # Function to get position history
    def get_position_history(self, symbol, start_time, end_time, product_type='umcbl'):
        response = None
        n_attempts = 3
        while n_attempts > 0:
            try:
                response = self.positionApi.history_position(self._get_symbol(symbol),
                                                             product_type,
                                                             utils.to_unix_millis(start_time),
                                                             utils.to_unix_millis(end_time))
                break
            except (exceptions.BitgetAPIException, Exception) as e:
                self.log_api_failure("positionApi.all_position", e, 0)
                time.sleep(0.2)
                n_attempts = n_attempts - 1

        # Process response
        if "msg" in response \
                and response["msg"] == "success" \
                and "data" in response \
                and "list" in response["data"]:

            df = pd.DataFrame(response["data"]["list"])
            if not isinstance(df, pd.DataFrame) or not df.columns.isin(["pnl", "netProfit"]).any():
                print("[get_position_history] df : ", df)
                return None, None, None, None
            #df['ctime'] = pd.to_datetime(df['ctime'], unit='ms')
            #df['utime'] = pd.to_datetime(df['utime'], unit='ms')

            # Filter out rows where either 'ctime' or 'utime' is before start_time
            # df = df[(df['ctime'] >= start_time) & (df['utime'] >= start_time)]  # CEDE Not sure

            # Calculating total profit and number of positions
            df['pnl'] = df['pnl'].astype(float)
            df['netProfit'] = df['netProfit'].astype(float)
            pnl = df['pnl'].sum()
            netProfit = df['netProfit'].sum()
            num_positions = len(df)

            return pnl, netProfit, num_positions, df
        else:
            print(f"Error: {response.status_code}, {response.text}")
            return None, None, None, None

    def trigger_BitgetWsClient(self, df_ticker_symbols, data_description, API_KEY, API_SECRET, API_PASSPHRASE):
        # ticker_symbols = [symbol + "USDT" for symbol in set(df_ticker_symbols["symbol"])]
        ticker_symbols = [symbol.replace("_UMCBL", "") for symbol in set(df_ticker_symbols["symbol"])] # CEDE DODGY USE GET_SYMBOL

        params = {"tickers": ticker_symbols,
                  "data_description": data_description,
                  "api_key": API_KEY,
                  "api_secret": API_SECRET,
                  "api_passphrase": API_PASSPHRASE,
                  }
        self.ws_client = bitget_ws_account_tickers.FDPWSAccountTickers(params)

        df_triggers_histo = self.get_all_triggers(by_pass=True)
        df_open_position_histo = self.get_open_position(by_pass=True)

        self.ws_data = ws_Data(df_open_positions=df_open_position_histo, df_triggers=df_triggers_histo)

        while True:
            time.sleep(0.5)
            self.ws_current_state = self.ws_client.get_state()

            self.ws_data.set_ws_prices(
                self.ws_current_state["ticker_prices"]
            )
            self.ws_data.set_ws_account(
                self.ws_current_state["account"]
            )

            df_trigger = convert_triggers_convert_df_to_df(self.ws_current_state["orders-algo"])
            df_trigger = self.set_open_orders_gridId(df_trigger)
            self.ws_data.set_ws_triggers(df_trigger)

            df_open_order = convert_open_orders_push_list_to_df(self.ws_current_state["orders"])
            df_open_order = self.set_open_orders_gridId(df_open_order)
            self.ws_data.set_ws_open_orders(df_open_order)

            df_open_position = self._build_df_open_positions_ws(self.ws_current_state["positions"],
                                                                self.ws_data.get_ws_prices())
            self.ws_data.set_ws_open_positions(df_open_position)

            TEST_DEBUG = False    # CEDE TO BE CLEARED WHEN DEBUG OK
            if TEST_DEBUG:
                symbols = df_ticker_symbols["symbol"]
                print("##################################################")
                if not utils.detailed_dataframes_equal(self.get_all_triggers(by_pass=True), self.get_all_triggers()):
                    print("titi")
                if not utils.detailed_dataframes_equal(self.get_open_position(by_pass=True), self.get_open_position()):
                    print("toto")

                print("##################################################")
                print(self.get_usdt_equity_available(by_pass=True))
                print(self.get_usdt_equity_available())
                print("##################################################")
                print("self.get_values(symbols, by_pass=True) \n", self.get_values(symbols, by_pass=True))
                print("self.get_values(symbols) \n", self.get_values(symbols))
                print("##################################################")
                for symbol in symbols:
                    print("self.get_value(symbol, by_pass=True)  ", self.get_value(symbol, by_pass=True))
                    print("self.get_value(symbol)  ", self.get_value(symbol))
                print("##################################################")