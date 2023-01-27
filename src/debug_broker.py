import ccxt
import pandas as pd
import time
from multiprocessing.pool import ThreadPool as Pool
import numpy as np

import os
from dotenv import load_dotenv

from src import broker_ccxt

import requests

def debug_crag_broker():
    test = True
    if test:
        pair = "BTC/USDT:USDT"

        load_dotenv()
        exchange_api_key = os.getenv("BITGET_API_KEY")
        exchange_api_secret = os.getenv("BITGET_API_SECRET")
        exchange_api_password = "orangeelephant"


        api_url = "https://api.bitget.com/api/mix/v1/market/contracts?productType=umcbl"

        response = requests.get(api_url)
        response.json()


        bitget = PerpBitget( apiKey= exchange_api_key,
                             secret=exchange_api_secret,
                             password=exchange_api_password
                             )

        usd_balance = float(bitget.get_usdt_equity())
        print("USD balance :", round(usd_balance, 2), "$")


        df_all_symbols = bitget.get_list_all_symbols()

        """
        FYI: 
            product Type:
                umcbl USDT perpetual contract
                dmcbl Universal margin perpetual contract
                cmcbl USDC perpetual contract
                sumcbl USDT simulation perpetual contract
                sdmcbl Universal margin simulation perpetual contract
                scmcbl USDC simulation perpetual contract
        """
        spot = False
        df_filtered_symbols = bitget.get_list_filtered_future_symbols(spot)
        print(df_filtered_symbols)

        base = "BTC"
        quote = "USDC"
        # type = "swap" or "spot"
        spot = True
        df_info_symbols = bitget.get_info_symbols(base, quote, spot)
        print(df_info_symbols)




        balance_data = bitget.get_all_balance()
        get_all_balance_total_usdt_free = balance_data['USDT']['free']
        get_all_balance_total_usdt_used = balance_data['USDT']['used']
        get_all_balance_total_usdt_total = balance_data['USDT']['total']
        print('total_usdt_free: ', get_all_balance_total_usdt_free)
        print('total_usdt_used: ', get_all_balance_total_usdt_used)
        print('total_usdt_total: ', get_all_balance_total_usdt_total)

        dict_position = balance_data['info'][0]
        df_balance = pd.DataFrame([dict_position], columns=dict_position.keys())
        print(df_balance.columns)
        equity = df_balance['equity'].values[0]
        usdt_equity = df_balance['usdtEquity'].values[0]
        btc_equity = df_balance['btcEquity'].values[0]
        print('equity: ',equity)
        print('usdt_equity: ',usdt_equity)
        print('btc_equity: ',btc_equity)

        lst_info_from_balance = ['marginCoin', 'locked', 'available', 'crossMaxAvailable',
                                 'fixedMaxAvailable', 'maxTransferOut', 'equity', 'usdtEquity',
                                 'btcEquity', 'unrealizedPL', 'bonus']

        positions_data = bitget.get_open_position()
        print(positions_data)
        print(positions_data.columns)
        lst_position_data_columns = ['marginCoin', 'symbol', 'holdSide', 'openDelegateCount', 'margin',
                                     'available', 'locked', 'total', 'leverage', 'achievedProfits',
                                     'averageOpenPrice', 'marginMode', 'holdMode', 'unrealizedPL',
                                     'liquidationPrice', 'keepMarginRate', 'marketPrice', 'cTime']

        coin = positions_data['symbol'].values[0]

        orders = bitget.get_my_orders(coin)

        # need coins bought to test....
        # coin = positions_data['symbol'].values[0]
        # balance_of_one_coin = bitget.get_balance_of_one_coin(coin)

        print('toto')
    else:
        my_broker = broker_ccxt.BrokerCCXT({'exchange': 'bitget', 'account': 'room2', 'simulation': 0})

        print("### balance ###")
        balance = my_broker.get_balance()
        print(balance)

        print("### positions ###")
        positions = my_broker.get_positions()
        print(positions)

        print("### orders ###")
        orders = my_broker.get_orders("BTC/USDT")
        print(orders)

        print("### my trades ###")
        my_broker.export_history()

        print("### portfolio value ###")
        print("{}".format(my_broker.get_portfolio_value()))

        print("### BTC ###")
        usdt_value = my_broker.get_value("BTC/USDT")
        print("USDT value = ", usdt_value)

        usdt_position_risk = my_broker.get_positions_risk(["BTC/USDT"])
        print("USDT oposition risk = ", usdt_position_risk)

        print("### sell everything ###")
        # my_broker.sell_everything()


class PerpBitget():
    def __init__(self, apiKey=None, secret=None, password=None):
        bitget_auth_object = {
            "apiKey": apiKey,
            "secret": secret,
            "password": password,
            'options': {
                'defaultType': 'swap',
            }
        }
        if bitget_auth_object['secret'] == None:
            self._auth = False
            self._session = ccxt.bitget()
        else:
            self._auth = True
            self._session = ccxt.bitget(bitget_auth_object)
        self.market = self._session.load_markets()

    def authentication_required(fn):
        """Annotation for methods that require auth."""

        def wrapped(self, *args, **kwargs):
            if not self._auth:
                # print("You must be authenticated to use this method", fn)
                raise Exception("You must be authenticated to use this method")
            else:
                return fn(self, *args, **kwargs)

        return wrapped

    def get_last_historical(self, symbol, timeframe, limit):
        result = pd.DataFrame(data=self._session.fetch_ohlcv(
            symbol, timeframe, None, limit=limit))
        result = result.rename(
            columns={0: 'timestamp', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'})
        result = result.set_index(result['timestamp'])
        result.index = pd.to_datetime(result.index, unit='ms')
        del result['timestamp']
        return result

    def get_more_last_historical_async(self, symbol, timeframe, limit):
        max_threads = 4
        pool_size = round(limit / 100)  # your "parallelness"

        # define worker function before a Pool is instantiated
        full_result = []

        def worker(i):

            try:
                return self._session.fetch_ohlcv(
                    symbol, timeframe, round(time.time() * 1000) - (i * 1000 * 60 * 60), limit=100)
            except Exception as err:
                raise Exception("Error on last historical on " + symbol + ": " + str(err))

        pool = Pool(max_threads)

        full_result = pool.map(worker, range(limit, 0, -100))
        full_result = np.array(full_result).reshape(-1, 6)
        result = pd.DataFrame(data=full_result)
        result = result.rename(
            columns={0: 'timestamp', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'})
        result = result.set_index(result['timestamp'])
        result.index = pd.to_datetime(result.index, unit='ms')
        del result['timestamp']
        return result.sort_index()

    def get_bid_ask_price(self, symbol):
        try:
            ticker = self._session.fetchTicker(symbol)
        except BaseException as err:
            raise Exception(err)
        return {"bid": ticker["bid"], "ask": ticker["ask"]}

    def get_min_order_amount(self, symbol):
        return self._session.markets_by_id[symbol]["info"]["minProvideSize"]

    def convert_amount_to_precision(self, symbol, amount):
        return self._session.amount_to_precision(symbol, amount)

    def convert_price_to_precision(self, symbol, price):
        return self._session.price_to_precision(symbol, price)

    @authentication_required
    def place_limit_order(self, symbol, side, amount, price, reduce=False):
        try:
            return self._session.createOrder(
                symbol,
                'limit',
                side,
                self.convert_amount_to_precision(symbol, amount),
                self.convert_price_to_precision(symbol, price),
                params={"reduceOnly": reduce}
            )
        except BaseException as err:
            raise Exception(err)

    @authentication_required
    def place_limit_stop_loss(self, symbol, side, amount, trigger_price, price, reduce=False):

        try:
            return self._session.createOrder(
                symbol,
                'limit',
                side,
                self.convert_amount_to_precision(symbol, amount),
                self.convert_price_to_precision(symbol, price),
                params={
                    'stopPrice': self.convert_price_to_precision(symbol, trigger_price),  # your stop price
                    "triggerType": "market_price",
                    "reduceOnly": reduce
                }
            )
        except BaseException as err:
            raise Exception(err)

    @authentication_required
    def place_market_order(self, symbol, side, amount, reduce=False):
        try:
            return self._session.createOrder(
                symbol,
                'market',
                side,
                self.convert_amount_to_precision(symbol, amount),
                None,
                params={"reduceOnly": reduce}
            )
        except BaseException as err:
            raise Exception(err)

    @authentication_required
    def place_market_stop_loss(self, symbol, side, amount, trigger_price, reduce=False):

        try:
            return self._session.createOrder(
                symbol,
                'market',
                side,
                self.convert_amount_to_precision(symbol, amount),
                self.convert_price_to_precision(symbol, trigger_price),
                params={
                    'stopPrice': self.convert_price_to_precision(symbol, trigger_price),  # your stop price
                    "triggerType": "market_price",
                    "reduceOnly": reduce
                }
            )
        except BaseException as err:
            raise Exception(err)

    @authentication_required
    def get_balance_of_one_coin(self, coin):
        try:
            allBalance = self._session.fetchBalance()
        except BaseException as err:
            raise Exception("An error occured", err)
        try:
            return allBalance['total'][coin]
        except:
            return 0

    @authentication_required
    def get_all_balance(self):
        try:
            allBalance = self._session.fetchBalance()
        except BaseException as err:
            raise Exception("An error occured", err)
        try:
            return allBalance
        except:
            return 0

    @authentication_required
    def get_usdt_equity(self):
        try:
            balance = self._session.fetchBalance()
            usdt_equity = balance["info"][0]["usdtEquity"]
        except BaseException as err:
            raise Exception("An error occured", err)
        try:
            return usdt_equity
        except:
            return 0

    @authentication_required
    def get_open_order(self, symbol, conditionnal=False):
        try:
            return self._session.fetchOpenOrders(symbol, params={'stop': conditionnal})
        except BaseException as err:
            raise Exception("An error occured", err)

    @authentication_required
    def get_my_orders(self, symbol):
        try:
            return self._session.fetch_orders(symbol)
        except BaseException as err:
            raise Exception("An error occured", err)

    @authentication_required
    def get_open_position(self, symbol=None):
        try:
            positions = self._session.fetchPositions(symbol)
            if positions['msg'] == 'success':
                df_truePositions = pd.DataFrame()
                for position in positions['data']:
                    if float(position['openDelegateCount']) > 0:
                        df = pd.DataFrame([position], columns=position.keys())
                        if len(df_truePositions) == 0:
                            df_truePositions = df
                        else:
                            df_truePositions = pd.concat(df_truePositions, df)
                return df_truePositions
            else:
                return []
        except BaseException as err:
            raise TypeError("An error occured in get_open_position", err)

    @authentication_required
    def cancel_order_by_id(self, id, symbol, conditionnal=False):
        try:
            if conditionnal:
                return self._session.cancel_order(id, symbol, params={'stop': True, "planType": "normal_plan"})
            else:
                return self._session.cancel_order(id, symbol)
        except BaseException as err:
            raise Exception("An error occured in cancel_order_by_id", err)

    def get_list_all_symbols(self):
        markets = self._session.fetch_markets()
        lst_columns = ['id', 'symbol', 'quote', 'base', 'type', 'spot', 'future', 'active', 'maker', 'taker', 'minsize']
        df = pd.DataFrame(columns=lst_columns)
        for market in markets:
            if market['active']:
                lst_info_symbol = [market['id'],
                                   market['symbol'],
                                   market['quote'],
                                   market['base'],
                                   market['type'],
                                   market['spot'],
                                   market['future'],
                                   market['active'],
                                   market['maker'],
                                   market['taker'],
                                   market['limits']['amount']['min']
                                   ]
            df.loc[len(df)] = lst_info_symbol
        return df

    def get_list_filtered_future_symbols(self, spot=False):
        markets = self._session.fetch_markets()
        lst_columns = ['id', 'symbol', 'quote', 'base', 'type', 'spot', 'future', 'active', 'maker', 'taker', 'minsize']
        df = pd.DataFrame(columns=lst_columns)
        for market in markets:
            if market['spot'] == spot:
                lst_info_symbol = [ market['id'],
                                    market['symbol'],
                                    market['quote'],
                                    market['base'],
                                    market['type'],
                                    market['spot'],
                                    market['future'],
                                    market['active'],
                                    market['maker'],
                                    market['taker'],
                                    market['limits']['amount']['min']
                                    ]
                df.loc[len(df)] = lst_info_symbol
        return df

    def get_info_symbols(self, base, quote, spot):
        markets = self._session.fetch_markets()
        lst_columns = ['id', 'symbol', 'quote', 'base', 'type', 'spot', 'future', 'active', 'maker', 'taker', 'minsize']
        df = pd.DataFrame(columns=lst_columns)
        for market in markets:
            if market['quote'] == quote and market['base'] == base and market['spot'] == spot:
                lst_info_symbol = [ market['id'],
                                    market['symbol'],
                                    market['quote'],
                                    market['base'],
                                    market['type'],
                                    market['spot'],
                                    market['future'],
                                    market['active'],
                                    market['maker'],
                                    market['taker'],
                                    market['limits']['amount']['min']
                                    ]
                df.loc[len(df)] = lst_info_symbol
        return df

""""
class ConnectBitget():
    def __init__(self, apiKey=None, secret=None, password=None):
        bitget_auth_object = {
            "apiKey": apiKey,
            "secret": secret,
            "password": password,
            'options': {
                'defaultType': 'swap',
            }
        }
        api_url = "https://api.bitget.com"
        todo = {"userId": 1, "title": "Buy milk", "completed": False}
        response = requests.post(api_url, json=todo)
        response.json()
        # {'userId': 1, 'title': 'Buy milk', 'completed': False, 'id': 201}

        print(response.status_code)


        if bitget_auth_object['secret'] == None:
            self._auth = False
            self._session = ccxt.bitget()
        else:
            self._auth = True
            self._session = ccxt.bitget(bitget_auth_object)
        self.market = self._session.load_markets()
"""