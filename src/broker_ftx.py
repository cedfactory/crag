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
        return self.ftx_exchange is not None

        my_positions = ftx_exchange.fetch_positions()
        print(my_positions)
        #my_trades = ftx_exchange.fetch_my_trades()
        #print(my_trades)

    def get_balance(self):
        if self.ftx_exchange:
            balance = self.ftx_exchange.fetch_balance()
            print(balance)
            return balance["info"]["result"][0]["free"]
        return 0.
    
    def get_positions(self):
        if self.ftx_exchange:
            positions = self.ftx_exchange.fetch_positions()
            return positions
        return []
       

    def get_commission(self, symbol):
        return 0.07

    def execute_trade(self, trade):
        return True

    def export_history(self, target):
        pass

    def export_status(self):
        pass
