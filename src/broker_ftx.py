'''
- intégrer fud / ftx / spot_ftx
- externaliser la stratégie (d'achat et de vente)
'''

from . import broker,rtdp,utils
import ccxt
from dotenv import load_dotenv
import os

class BrokerFTX(broker.Broker):
    def __init__(self, params = None):
        super().__init__(params)

        self.rtdp = rtdp.RealTimeDataProvider(params)
        self.trades = []
        self.simulation = False
        self.account = ""
        self.leverage = 0
        if params:
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
            print("[BrokerFTX] : Problem encountered during authentification")
         
    def authentification(self):
        authentificated = False
        load_dotenv()
        ftx_api_key = os.getenv("FTX_API_KEY")
        ftx_api_secret = os.getenv("FTX_API_SECRET")
        params = {
            'apiKey': ftx_api_key,
            'secret': ftx_api_secret
            }
        if self.account != "":
            params["headers"] = {"FTX-SUBACCOUNT": self.account}
        self.ftx_exchange = ccxt.binance(params)
        # check authentification
        try:
            authentificated = self.ftx_exchange.check_required_credentials()
            if self.leverage != 0:
                response = self.ftx_exchange.private_post_account_leverage({"leverage": self.leverage})
        except ccxt.AuthenticationError as err:
            print("[BrokerFTX] AuthenticationError : ", err)
        except ccxt.NetworkError as err:
            print("[BrokerFTX] NetworkError : ", err)
        except BaseException as err:
            print("[BrokerFTX] BaseException : ", err)
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
        if self.ftx_exchange:
            try:
                balance = self.ftx_exchange.fetch_balance()
                if 'USD' in balance:
                    result = float(balance['USD']['free'])
            except BaseException as err:
                print("[BrokerFTX::get_cash] An error occured : {}".format(err))
        return result

    @authentication_required
    def get_balance(self):
        result = {}
        if self.ftx_exchange:
            try:
                balance = self.ftx_exchange.fetch_balance()
                #print(balance)
                result = {coin['coin']:{"availableForWithdrawal":float(coin['total']), "usdValue":float(coin["usdValue"])} for coin in balance["info"]["result"] if coin['total'] != "0.0"}
            except BaseException as err:
                print("[BrokerFTX::get_balance] An error occured : {}".format(err))
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
        if self.ftx_exchange:
            try:
                result = self.ftx_exchange.fetch_positions()
            except BaseException as err:
                print("[BrokerFTX::get_positions] An error occured : {}".format(err))
        return result
       
    @authentication_required
    def get_value(self, symbol):
        result = None
        if self.ftx_exchange:
            try:
                result = self.ftx_exchange.fetch_ticker(symbol)["close"]
            except BaseException as err:
                print("[BrokerFTX::get_value] An error occured : {}".format(err))
        return result

    @authentication_required
    def get_commission(self, symbol):
        # https://docs.ftx.com/#execution-report-8
        return 0.0067307233

    def get_info(self):
        return None, None, None

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
            amount = trade.net_price / trade.symbol_price
            try:
                order_structure = self.ftx_exchange.create_order(symbol, "market", side, amount)
            except BaseException as err:
                print("[BrokerFTX::execute_trade] An error occured : {}".format(err))
                print("[BrokerFTX::execute_trade]   -> symbol : {}".format(symbol))
                print("[BrokerFTX::execute_trade]   -> side :   {}".format(side))
                print("[BrokerFTX::execute_trade]   -> amount : {}".format(amount))
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
                order_structure = self.ftx_exchange.create_order(coin+"/USD", "market", "sell", my_balance[coin]["availableForWithdrawal"])
                #print(order_structure)
            except BaseException as err:
                print("[BrokerFTX::execute_trade] An error occured : {}".format(err))

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
        my_trades = self.ftx_exchange.fetch_my_trades()
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
