import ccxt
import pandas as pd
import time
from multiprocessing.pool import ThreadPool as Pool
import numpy as np


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
        if "/" in symbol:
            symbol = symbol.split("/")[0]+"USDT_SPBL" # hack : retrieve the symbol name
        else:
            symbol += "USDT_SPBL"
        return float(self._session.markets_by_id[symbol][0]["info"]["minTradeAmount"]) # minTradeUSDT

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
            print("exception : ", err)
            return None

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
            usdt_equity = self._session.fetchBalance()["info"][0]["usdtEquity"]
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
            truePositions = []
            if "data" in positions:
                positions = positions["data"]
            for position in positions:
                if "info" in position:
                    position = position["info"]
                if float(position['total']) > 0: # contractSize ?
                    truePositions.append(position)
            return truePositions
        except BaseException as err:
            raise TypeError("An error occured in get_open_position", err)
    '''
    @authentication_required
    def get_positions(self):
        result = {}
        try:
            response = self._session.fetch_positions()
            for position in response:
                position = position["info"]
                if position["total"] == '0':
                    continue
                if not position["symbol"] in result:
                    result[position["symbol"]] = {}
                result[position["symbol"]][position["holdSide"]] = {"total" : float(position["total"]), "available" : float(position["available"])}
        except BaseException as err:
            print("[BrokerCCXT::get_positions] An error occured : {}".format(err))
        return result
    '''
    @authentication_required
    def cancel_order_by_id(self, id, symbol, conditionnal=False):
        try:
            if conditionnal:
                return self._session.cancel_order(id, symbol, params={'stop': True, "planType": "normal_plan"})
            else:
                return self._session.cancel_order(id, symbol)
        except BaseException as err:
            raise Exception("An error occured in cancel_order_by_id", err)

    @authentication_required
    def get_orders(self, symbol):
        result = []
        try:
            result = self._session.fetch_orders(symbol)
        except BaseException as err:
            print("[BrokerCCXT::get_orders] An error occured : ", err)
        return result

    def _format_row(self, current_trade):
        datetime = current_trade['datetime']
        symbol = current_trade['symbol']
        side = current_trade['side']
        price = current_trade['price']
        amount = current_trade['amount']
        cost = current_trade['cost']
        fee_cost = current_trade['fee']['cost']
        fee_rate = current_trade['fee']['rate']
        return "{},{},{},{},{},{},{},{}".format(datetime,symbol,side,price,amount,cost,fee_cost,fee_rate)

    @authentication_required
    def export_history(self, target=None):
        if not self._session.has['fetchMyTrades']:
            return
        my_trades = self._session.fetch_my_trades("BTC/USDT")
        if len(my_trades) > 0:
            if target and target.endswith(".csv"):
                with open(target, 'w', newline='') as f:
                    f.write("datetime,symbol,side,price,amount,cost,fee_cost,fee_rate\n")
                    for current_trade in my_trades:
                        f.write(self._format_row(current_trade)+'\n')
                    f.close()
            else:
                print("datetime;symbol;side;price;amount;cost;fee_cost;fee_rate")
                for current_trade in my_trades:
                    print(self._format_row(current_trade))

    def export_status(self):
        pass

    @authentication_required
    def get_portfolio_value(self):
        return self.get_usdt_equity()