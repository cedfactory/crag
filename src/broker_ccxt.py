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
        if params:
            self.name = params.get("name", self.name)
            self.exchange_name = params.get("exchange", self.exchange_name)
            self.api_key = params.get("api_key", self.api_key)
            self.api_secret = params.get("api_secret", self.api_secret)
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
            print("[BrokerCCXT] : Problem encountered during authentification")
         
    def authentification(self):
        authentificated = False
        load_dotenv()
        exchange_api_key = os.getenv(self.api_key)
        exchange_api_secret = os.getenv(self.api_secret)
        params = {
            'apiKey': exchange_api_key,
            'secret': exchange_api_secret
            }
        if self.account != "":
            params["headers"] = {"EXCHANGE-SUBACCOUNT": self.account}

        self.exchange = None
        if self.exchange_name == "binance":
            self.exchange = ccxt.binance(params)
        elif self.exchange_name == "hitbtc":
            self.exchange = ccxt.hitbtc(params)
        elif self.exchange_name == "kraken":
            self.exchange = ccxt.kraken(params)

        if self.exchange == None:
            return False

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
                #print(balance)
                result = {coin['coin']:{"availableForWithdrawal":float(coin['total']), "usdValue":float(coin["usdValue"])} for coin in balance["info"]["result"] if coin['total'] != "0.0"}
            except BaseException as err:
                print("[BrokerCCXT::get_balance] An error occured : {}".format(err))
        return result
    
    @authentication_required
    def get_portfolio_value(self):
        balance = self.get_balance()
        portfolio_value = 0
        for coin in balance:
            #print("{}: {}".format(coin, balance[coin]["usdValue"]))
            portfolio_value += balance[coin]["usdValue"]
        return portfolio_value

    @authentication_required
    def get_positions(self):
        result = []
        if self.exchange:
            try:
                result = self.exchange.fetch_positions()
            except BaseException as err:
                print("[BrokerCCXT::get_positions] An error occured : {}".format(err))
        return result
       
    @authentication_required
    def get_value(self, symbol):
        result = None
        if self.exchange:
            try:
                result = self.exchange.fetch_ticker(symbol)["close"]
            except BaseException as err:
                print("[BrokerCCXT::get_value] An error occured : {}".format(err))
        return result

    @authentication_required
    def get_commission(self, symbol):
        return 0.0067307233

    def get_info(self):
        return None, None, None

    @authentication_required
    def execute_trade(self, trade):
        if self.simulation:
            return True

        print("!!!!!!! EXECUTE THE TRADE !!!!!!!")
        if self.exchange:
            side = ""
            if trade.type == "SELL":
                side = "sell"
            elif trade.type == "BUY":
                side = "buy"
            if side == "":
                return False

            symbol = trade.symbol
            amount = trade.net_price / trade.symbol_price
            try:
                order_structure = self.exchange.create_order(symbol, "market", side, amount)
            except BaseException as err:
                print("[BrokerCCXT::execute_trade] An error occured : {}".format(err))
                print("[BrokerCCXT::execute_trade]   -> symbol : {}".format(symbol))
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
        my_trades = self.exchange.fetch_my_trades()
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
