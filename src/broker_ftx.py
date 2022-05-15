'''
- intégrer fud / ftx / spot_ftx
- externaliser la stratégie (d'achat et de vente)
'''

from . import broker
import ccxt
from dotenv import load_dotenv
import os

class BrokerFTX(broker.Broker):
    def __init__(self, params = None):
        super().__init__(params)

        self.trades = []
        self.simulation = False
        if params:
            self.simulation = params.get("simulation", self.simulation)
            
        print(self.simulation)

    def initialize(self, params):
        load_dotenv()
        ftx_api_key = os.getenv("FTX_API_KEY")
        ftx_api_secret = os.getenv("FTX_API_SECRET")
        self.ftx_exchange = ccxt.ftx({
            'apiKey': ftx_api_key,
            'secret': ftx_api_secret
            })
        self.authenficated = self.ftx_exchange is not None
        return self.authenficated

    def authentication_required(fn):
        """decoration for methods that require authentification"""
        def wrapped(self, *args, **kwargs):
            if not self.ftx_exchange:
                print("You must be authenticated to use this method {}".format(fn))
                exit()
            else:
                return fn(self, *args, **kwargs)
        return wrapped

    @authentication_required
    def get_balance(self):
        result = {}
        if self.ftx_exchange:
            try:
                balance = self.ftx_exchange.fetch_balance()
                print(balance)
                result = {coin['coin']:float(coin['total']) for coin in balance["info"]["result"] if coin['total'] != "0.0"}
            except BaseException as err:
                print("[BrokerFTX::get_balance] An error occured : {}".format(err))
        return result
    
    @authentication_required
    def get_positions(self):
        result = []
        if self.ftx_exchange:
            try:
                result = self.ftx_exchange.fetch_positions()
            except BaseException as err:
                print("[BrokerFTX::get_positions] An error occured : {}".format(err))
        return result
       
    def get_value(self, symbol):
        ticker = self.ftx_exchange.fetch_ticker(symbol)
        return ticker["close"]

    @authentication_required
    def get_commission(self, symbol):
        return 0.07

    @authentication_required
    def execute_trade(self, trade):
        if self.simulation:
            return True

        print("!!!!!!! EXECUTE THE TRADE !!!!!!!")
        if self.ftx_exchange:
            side = ""
            if trade.type == "SELL":
                side = "sell"
            elif trade.type == "BUY":
                side = "buy"
            if side == "":
                return False

            symbol = trade.symbol
            amount = trade.net_price
            try:
                order_structure = self.ftx_exchange.create_order(symbol, "market", side, amount)
                print(order_structure)
            except BaseException as err:
                print("[BrokerFTX::execute_trade] An error occured : {}".format(err))
            return True
        return False

    @authentication_required
    def export_history(self, target=""):
        my_trades = self.ftx_exchange.fetch_my_trades()
        for my_trade in my_trades:
            datetime = my_trade['datetime']
            symbol = my_trade['symbol']
            side = my_trade['side']
            price = my_trade['price']
            amount = my_trade['amount']
            cost = my_trade['cost']
            fee_cost = my_trade['fee']['cost']
            fee_rate = my_trade['fee']['rate']
            print("{};{};{};{};{};{};{};{}".format(datetime,symbol,side,price,amount,cost,fee_cost,fee_rate))

    def export_status(self):
        pass
