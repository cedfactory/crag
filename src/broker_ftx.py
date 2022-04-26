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
        if self.ftx_exchange:
            balance = self.ftx_exchange.fetch_balance()
            print(balance)
            return balance["info"]["result"][0]["free"]
        return 0.
    
    @authentication_required
    def get_positions(self):
        if self.ftx_exchange:
            positions = self.ftx_exchange.fetch_positions()
            return positions
        return []
       
    @authentication_required
    def get_commission(self, symbol):
        return 0.07

    @authentication_required
    def execute_trade(self, trade):
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
            order_structure = self.ftx_exchange.create_order(symbol, type, side, amount)
            print(order_structure)
            return True
        return False

    @authentication_required
    def export_history(self, target):
        my_trades = self.ftx_exchange.fetch_my_trades()
        print(my_trades)
        pass

    def export_status(self):
        pass
