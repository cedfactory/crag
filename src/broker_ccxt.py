from . import broker,rtdp,utils
import ccxt
from dotenv import load_dotenv
import os

class BrokerCCXT(broker.Broker):
    def __init__(self, params = None):
        super().__init__(params)

        self.rtdp = rtdp.RealTimeDataProvider(params)
        self.trades = []
        self.simulation = False
        self.account = ""
        self.leverage = 0
        self.name = ""
        self.exchange_name = ""
        self.api_key = ""
        self.api_secret = ""
        self.api_password = ""
        self.orders = "market"
        self.chase_limit = False
        if params:
            self.name = params.get("name", self.name)
            self.exchange_name = params.get("exchange", self.exchange_name)
            self.api_key = self.exchange_name.upper()+"_API_KEY"
            self.api_secret = self.exchange_name.upper()+"_API_SECRET"
            if self.exchange_name == "bitget":
                self.api_password = self.exchange_name.upper()+"_API_PASSWORD"
            self.simulation = params.get("simulation", self.simulation)
            if self.simulation == 0 or self.simulation == "0":
                self.simulation = False
            if self.simulation == 1 or self.simulation == "1":
                self.simulation = True
            self.account = params.get("account", self.account)
            self.orders = params.get("orders", self.orders)
            self.leverage = params.get("leverage", self.leverage)
            if isinstance(self.leverage, str):
                self.leverage = int(self.leverage)
        if not self.authentification():
            print("[BrokerCCXT] : Problem encountered during authentification")
         
    def authentification(self):
        authentificated = False
        load_dotenv()
        exchange_api_key = os.getenv(self.api_key)
        exchange_api_secret = os.getenv(self.api_secret)
        exchange_api_password = os.getenv(self.api_password)
        params = {
            'apiKey': exchange_api_key,
            'secret': exchange_api_secret,
            'password': exchange_api_password,
            'options': {
                    #'defaultType': 'swap', # for futures
                }
            }
        if self.account != "":
            params["headers"] = {"EXCHANGE-SUBACCOUNT": self.account}

        self.exchange = None
        if self.exchange_name == "binance":
            self.exchange = ccxt.binance(params)
        if self.exchange_name == "bitget":
            self.exchange = ccxt.bitget(params)
        elif self.exchange_name == "hitbtc":
            self.exchange = ccxt.hitbtc(params)
        elif self.exchange_name == "kraken":
            self.exchange = ccxt.kraken(params)

        if self.exchange == None:
            return False

        self.exchange.load_markets()

        # check authentification
        try:
            authentificated = self.exchange.check_required_credentials()
            if self.leverage != 0:
                response = self.exchange.private_post_account_leverage({"leverage": self.leverage})
        except ccxt.AuthenticationError as err:
            print("[BrokerCCXT] AuthenticationError : ", err)
        except ccxt.NetworkError as err:
            print("[BrokerCCXT] NetworkError : ", err)
        except BaseException as err:
            print("[BrokerCCXT] BaseException : ", err)
        return authentificated

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
        return self.exchange != None

    def log_info(self):
        info = ""
        info += "{}".format(type(self).__name__)
        info += "\nCash : $ {}".format(utils.KeepNDecimals(self.get_cash(), 2))
        info += "\nLeverage : {}".format(self.leverage)
        return info

    @authentication_required
    def get_cash(self):
        # get free USD
        result = 0
        if self.exchange:
            try:
                balance = self.exchange.fetch_balance()
                if 'USD' in balance:
                    result = float(balance['USD']['free'])
            except BaseException as err:
                print("[BrokerCCXT::get_cash] An error occured : {}".format(err))
        return result

    @authentication_required
    def get_balance(self):
        result = {}
        if self.exchange:
            try:
                balance = self.exchange.fetch_balance()
                if "info" not in balance or len(balance["info"]) == 0:
                    return {}
                result = {coin['coinName']:{"available":float(coin['available'])} for coin in balance["info"] if float(coin['available']) != 0.}
            except BaseException as err:
                print("[BrokerCCXT::get_balance] An error occured : {}".format(err))
        return result
    
    @authentication_required
    def get_portfolio_value(self):
        balance = self.get_balance()
        print(balance)
        portfolio_value = 0
        for coin in balance:
            #print("{}: {}".format(coin, balance[coin]["usdValue"]))
            portfolio_value += balance[coin]["available"]
        return portfolio_value

    @authentication_required
    def get_positions(self):
        result = {}
        if self.exchange:
            try:
                response = self.exchange.fetch_positions()
                positions = response["data"]
                for position in positions:
                    if not position["symbol"] in result:
                        result[position["symbol"]] = {}
                    result[position["symbol"]][position["holdSide"]] = {"total" : position["total"], "available" : position["available"]}
            except BaseException as err:
                print("[BrokerCCXT::get_positions] An error occured : {}".format(err))
        return result

    @authentication_required
    def get_orders(self, symbol):
        result = []
        if self.exchange:
            try:
                result = self.exchange.fetch_orders(symbol)
            except BaseException as err:
                print("[BrokerCCXT::get_orders] An error occured : {}".format(err))
        return result
       
    @authentication_required
    def get_value(self, symbol):
        result = {"close" : None, "bid" : None, "ask" : None}
        if self.exchange:
            try:
                ticker = self.exchange.fetch_ticker(symbol)
                result = {"close" : ticker["close"], "bid" : ticker["bid"], "ask" : ticker["ask"]}
            except BaseException as err:
                print("[BrokerCCXT::get_value] An error occured : {}".format(err))
        return result

    @authentication_required
    def get_commission(self, symbol):
        if self.exchange_name == "bitget":
            return 0.01
        return 0.0067307233

    def get_info(self):
        return None, None, None

    def get_min_size(self, ticker):
        min_size = False
        if self.exchange:
            markets = [mk for mk in self.exchange.fetch_markets() if mk["symbol"] == ticker]
            if len(markets) == 0:
                return False
            min_size = markets[0]["limits"]["amount"]["min"]
        return min_size

    #
    # limit chase
    #
    # ref https://nukewhales.com/p/limitchase.html
    #
    def _refresh_order(self, order):
        updated_orders = self.exchange.fetch_orders()
        for updated_order in updated_orders:
            if updated_order["id"] == order["id"]:
                return updated_order
        print("Failed to find order {}".format(order["id"]))
        return None

    @authentication_required
    def _chase_limit(self, ticker, amount):
        import time, random
        min_size = self.get_min_size(ticker)
        if min_size == False:
            return False
        amount_traded = 0

        # Initialise empty order and prices, preparing for loop
        order = None
        bid, ask = 0, 1e10

        attempts_execute = 3

        while amount - amount_traded > min_size:
            move = False
            ticker_data = self.exchange.fetch_ticker(ticker)
            new_bid, new_ask = ticker_data['bid'], ticker_data['ask']

            if bid != new_bid:
                bid = new_bid

                # If an order exists then cancel it
                if order is not None:
                    # cancel order
                    try:
                        self.exchange.cancel_order(order["id"])
                    except Exception as e:
                        print(e)

                    # refresh order details and track how much we got filled
                    order = self._refresh_order(order)
                    if order:
                        amount_traded += float(order["info"]["filledSize"])

                    # Exit now if we're done!
                    if amount - amount_traded < min_size:
                        break

                # place order
                try:
                    order = self.exchange.create_limit_buy_order(ticker, amount, new_bid, {"postOnly": True})
                    print("Buy {} {} at {}".format(amount, ticker, new_bid))
                except Exception as e:
                    print(e)
                    attempts_execute -= 1
                    if attempts_execute == 0:
                        return False
                time.sleep(random.random())

            # Even if the price has not moved, check how much we have filled.
            if order is not None:
                order = self._refresh_order(order)
                if order:
                    amount_traded += float(order["info"]["filledSize"])
            time.sleep(0.1)

        print("Finished buying {} of {}".format(amount, ticker))
        return True
        

    @authentication_required
    def execute_trade(self, trade):
        if self.simulation:
            return True

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

            if side == "":
                return False

            symbol = trade.symbol
            amount = trade.net_price / trade.symbol_price

            if type == "market" and self.chase_limit:
                return self._chase_limit(symbol, amount)

            try:
                order_structure = self.exchange.create_order(symbol, type, side, amount)
            except BaseException as err:
                print("[BrokerCCXT::execute_trade] An error occured : {}".format(err))
                print("[BrokerCCXT::execute_trade]   -> symbol : {}".format(symbol))
                print("[BrokerCCXT::execute_trade]   -> type :   {}".format(type))
                print("[BrokerCCXT::execute_trade]   -> side :   {}".format(side))
                print("[BrokerCCXT::execute_trade]   -> amount : {}".format(amount))
            return True

        return False

    @authentication_required
    def sell_everything(self):
        print("cash",self.get_cash())
        my_balance = self.get_balance()
        print(my_balance)
        for coin in my_balance:
            if coin == "EUR" or coin == "USD":
                continue
            print("{} : {}".format(coin, my_balance[coin]["availableForWithdrawal"]))
            try:
                order_structure = self.exchange.create_order(coin+"/USD", "market", "sell", my_balance[coin]["availableForWithdrawal"])
                #print(order_structure)
            except BaseException as err:
                print("[BrokerCCXT::execute_trade] An error occured : {}".format(err))

        return True

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
        if not self.exchange.has['fetchMyTrades']:
            return
        my_trades = self.exchange.fetch_my_trades("BTC/USDT")
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
