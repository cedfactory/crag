from . import broker,rtdp,utils
import pandas as pd

class BrokerBitGet(broker.Broker):
    def __init__(self, params = None):
        self.rtdp = rtdp.RealTimeDataProvider(params)
        self.trades = []
        self.simulation = False
        self.account = ""
        self.leverage = 0
        self.name = ""
        self.exchange_name = "bitget"
        self.api_key = "BITGET_API_KEY"
        self.api_secret = "BITGET_API_SECRET"
        self.api_password = "BITGET_API_PASSWORD"
        self.chase_limit = False
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
        if not self._authentification():
            print("[BrokerBitGet] : Problem encountered during authentification")

    def authentication_required(fn):
        """decoration for methods that require authentification"""
        def wrapped(self, *args, **kwargs):
            if not self._authentification():
                print("You must be authenticated to use this method {}".format(fn))
                return None
            else:
                return fn(self, *args, **kwargs)
        return wrapped

    def ready(self):
        return self.marketApi != None and self.accountApi != None

    def log_info(self):
        info = ""
        info += "{}".format(type(self).__name__)
        info += "\nCash : $ {}".format(utils.KeepNDecimals(self.get_cash(), 2))
        info += "\nLeverage : {}".format(self.leverage)
        return info



    @authentication_required
    def get_commission(self, symbol):
        pass

    @authentication_required
    def execute_trade(self, trade):
        print("!!!!!!! EXECUTE THE TRADE !!!!!!!")
        trade.success = False
        symbol = self._get_symbol(trade.symbol)
        amount = trade.gross_size
        if trade.type == "OPEN_LONG":
            if amount > self.get_min_order_amount(symbol):
                transaction = self._open_long_position(symbol, trade.gross_size)
                if transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                    trade.success = True
                    trade.orderId = transaction["data"]["orderId"]
                    trade.tradeId, trade.symbol_price, trade.gross_price, trade.gross_size, trade.buying_fee = self.get_order_fill_detail(symbol, trade.orderId)
                    trade.net_size = trade.gross_size
                    trade.net_price = trade.gross_price
                    trade.bought_gross_price = trade.gross_price
                    trade.buying_price = trade.symbol_price

        elif trade.type == "OPEN_SHORT":
            if amount > self.get_min_order_amount(symbol):
                transaction = self._open_short_position(symbol, trade.gross_size)
                if transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                    trade.success = True
                    trade.orderId = transaction["data"]["orderId"]
                    trade.tradeId, trade.symbol_price, trade.gross_price, trade.gross_size, trade.buying_fee = self.get_order_fill_detail(symbol, trade.orderId)
                    trade.net_size = trade.gross_size
                    trade.net_price = trade.gross_price
                    trade.bought_gross_price = trade.gross_price
                    trade.buying_price = trade.symbol_price

        elif trade.type == "CLOSE_LONG":
            transaction = self._close_long_position(symbol, trade.gross_size)
            if transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                trade.success = True
                trade.orderId = transaction["data"]["orderId"]
                trade.tradeId, trade.symbol_price, trade.gross_price, trade.gross_size, trade.selling_fee = self.get_order_fill_detail(symbol, trade.orderId)
                # CEDE to be confirmed : selling_fee is selling_fee + buying_fee or just selling_fee
                trade.net_size = trade.gross_size
                trade.net_price = trade.gross_price
                trade.roi = 100 * (trade.gross_price - trade.bought_gross_price - trade.selling_fee - trade.buying_fee) / trade.bought_gross_price

        elif trade.type == "CLOSE_SHORT":
            transaction = self._close_short_position(symbol, trade.gross_size)
            if transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                trade.success = True
                trade.orderId = transaction["data"]["orderId"]
                trade.tradeId, trade.symbol_price, trade.gross_price, trade.gross_size, trade.selling_fee = self.get_order_fill_detail(symbol, trade.orderId)
                # CEDE to be confirmed : selling_fee is selling_fee + buying_fee or just selling_fee
                trade.net_size = trade.gross_size
                trade.net_price = trade.gross_price
                trade.roi = 100 * (-1) * (trade.gross_price - trade.bought_gross_price - trade.selling_fee - trade.buying_fee) / trade.bought_gross_price

        # CEDE COMMENT self.trades never used
        self.trades.append(trade)
        return trade.success

    @authentication_required
    def export_history(self, target):
        pass

    def _build_df_open_positions(self, open_positions):
        df_open_positions = pd.DataFrame(columns=["symbol", "holdSide", "leverage", "marginCoin", "available", "total", "marketPrice"])
        for i in range(len(open_positions)):
            data = open_positions[i]
            df_open_positions.loc[i] = pd.Series({"symbol": data["symbol"], "holdSide": data["holdSide"], "leverage": data["leverage"], "marginCoin": data["marginCoin"],"available": float(data["available"]),"total": float(data["total"]),"marketPrice": data["marketPrice"]})
        return df_open_positions
