'''
- intégrer fud / ftx / spot_ftx
- externaliser la stratégie (d'achat et de vente)
'''

from . import broker,rtdp
import ccxt
from dotenv import load_dotenv
import os

class BrokerFTX(broker.Broker):
    def __init__(self, params = None):
        super().__init__(params)

        self.rtdp = rtdp.RealTimeDataProvider(params)
        self.trades = []
        self.simulation = False
        account = "Main Account"
        if params:
            self.simulation = params.get("simulation", self.simulation)
            account = params.get("account", account)
        self.authentificated = self.authentification(account)
         
    def authentification(self, account):
        authentificated = False
        load_dotenv()
        ftx_api_key = os.getenv("FTX_API_KEY")
        ftx_api_secret = os.getenv("FTX_API_SECRET")
        self.ftx_exchange = ccxt.ftx({
            'headers': {
                'FTX-SUBACCOUNT': account,
            },
            'apiKey': ftx_api_key,
            'secret': ftx_api_secret
            })
        # check authentification
        try:
            authentificated = self.ftx_exchange.check_required_credentials()
        except ccxt.AuthenticationError as err:
            print("[BrokerFTX] AuthenticationError : ", err)
        return authentificated

    def authentication_required(fn):
        """decoration for methods that require authentification"""
        def wrapped(self, *args, **kwargs):
            if not self.authentificated:
                print("You must be authenticated to use this method {}".format(fn))
                return None
            else:
                return fn(self, *args, **kwargs)
        return wrapped

    @authentication_required
    def get_cash(self):
        # get free USD
        result = 0
        if self.ftx_exchange:
            try:
                balance = self.ftx_exchange.fetch_balance()
                if 'USD' in balance:
                    result = float(balance['USD']['free'])
            except BaseException as err:
                print("[BrokerFTX::get_balance] An error occured : {}".format(err))
        return result

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
        return 0.0007

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

    def _format_row(self, current_trade):
        datetime = current_trade['datetime']
        symbol = current_trade['symbol']
        side = current_trade['side']
        price = current_trade['price']
        amount = current_trade['amount']
        cost = current_trade['cost']
        fee_cost = current_trade['fee']['cost']
        fee_rate = current_trade['fee']['rate']
        return "{};{};{};{};{};{};{};{}".format(datetime,symbol,side,price,amount,cost,fee_cost,fee_rate)

    @authentication_required
    def export_history(self, target=None):
        my_trades = self.ftx_exchange.fetch_my_trades()
        if len(my_trades) > 0:
            if target and target.endswith(".csv"):
                with open(target, 'w', newline='') as f:
                    f.write("datetime;symbol;side;price;amount;cost;fee_cost;fee_rate\n")
                    for current_trade in my_trades:
                        f.write(self._format_row(current_trade)+'\n')
                    f.close()
            else:
                for current_trade in my_trades:
                    print(self._format_row(current_trade))

    def export_status(self):
        pass
