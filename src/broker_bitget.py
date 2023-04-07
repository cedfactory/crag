from . import broker,rtdp,utils
import pandas as pd

class BrokerBitGet(broker.Broker):
    def __init__(self, params = None):
        super().__init__(params)
        self.rtdp = rtdp.RealTimeDataProvider(params)
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

        self.clientOIdprovider = utils.ClientOIdProvider()
        # leverages management
        self.leveraged_symbols = []
        self.leverage_short = 1
        self.leverage_long = 1
        if params:
            self.leverage_short = int(params.get("leverage_short", self.leverage_short))
            self.leverage_long = int(params.get("leverage_long", self.leverage_long))

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
    def get_minimum_size(self, symbol):
        pass

    @authentication_required
    def set_symbol_leverage(self, symbol, leverage):
        pass

    @authentication_required
    def set_symbol_margin(self, symbol, margin):
        pass

    @authentication_required
    def set_margin_and_leverage(self, symbol):
        if symbol not in self.leveraged_symbols:
            self.set_symbol_margin(symbol, "fixed")
            self.set_symbol_leverage(symbol, self.leverage_long, "long")
            self.set_symbol_leverage(symbol, self.leverage_short, "short")
            self.leveraged_symbols.append(symbol)

    @authentication_required
    def execute_trade(self, trade):
        print("!!!!!!! EXECUTE THE TRADE !!!!!!!")
        trade.success = False
        symbol = self._get_symbol(trade.symbol)

        self.set_margin_and_leverage(symbol)

        amount = trade.gross_size
        minsize = trade.minsize
        if trade.type == "OPEN_LONG":
            if amount > minsize:
                clientOid = self.clientOIdprovider.get_name(symbol, "OPEN_LONG")
                transaction = self._open_long_position(symbol, trade.gross_size, clientOid)
                if transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                    trade.success = True
                    trade.orderId = transaction["data"]["orderId"]
                    trade.clientOid = transaction["data"]["clientOid"]
                    trade.tradeId, trade.symbol_price, trade.gross_price, trade.gross_size, trade.buying_fee = self.get_order_fill_detail(symbol, trade.orderId)
                    trade.net_size = trade.gross_size
                    trade.net_price = trade.gross_price
                    trade.bought_gross_price = trade.gross_price
                    trade.buying_price = trade.symbol_price

        elif trade.type == "OPEN_SHORT":
            if amount < -minsize:
                clientOid = self.clientOIdprovider.get_name(symbol, "OPEN_SHORT")
                transaction = self._open_short_position(symbol, -trade.gross_size, clientOid)
                if transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                    trade.success = True
                    trade.orderId = transaction["data"]["orderId"]
                    trade.clientOid = transaction["data"]["clientOid"]
                    trade.tradeId, trade.symbol_price, trade.gross_price, trade.gross_size, trade.buying_fee = self.get_order_fill_detail(symbol, trade.orderId)
                    trade.net_size = trade.gross_size
                    trade.net_price = trade.gross_price
                    trade.bought_gross_price = trade.gross_price
                    trade.buying_price = trade.symbol_price

        elif trade.type == "CLOSE_LONG":
            clientOid = self.clientOIdprovider.get_name(symbol, "CLOSE_LONG")
            transaction = self._close_long_position(symbol, trade.gross_size, clientOid)
            if transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                trade.success = True
                trade.orderId = transaction["data"]["orderId"]
                trade.clientOid = transaction["data"]["clientOid"]
                trade.tradeId, trade.symbol_price, trade.gross_price, trade.gross_size, trade.selling_fee = self.get_order_fill_detail(symbol, trade.orderId)
                # CEDE to be confirmed : selling_fee is selling_fee + buying_fee or just selling_fee
                trade.net_size = trade.gross_size
                trade.net_price = trade.gross_price
                trade.roi = 100 * (trade.gross_price - trade.bought_gross_price - trade.selling_fee - trade.buying_fee) / trade.bought_gross_price

        elif trade.type == "CLOSE_SHORT":
            clientOid = self.clientOIdprovider.get_name(symbol, "CLOSE_SHORT")
            transaction = self._close_short_position(symbol, trade.gross_size, clientOid)
            if transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                trade.success = True
                trade.orderId = transaction["data"]["orderId"]
                trade.clientOid = transaction["data"]["clientOid"]
                trade.tradeId, trade.symbol_price, trade.gross_price, trade.gross_size, trade.selling_fee = self.get_order_fill_detail(symbol, trade.orderId)
                # CEDE to be confirmed : selling_fee is selling_fee + buying_fee or just selling_fee
                trade.net_size = trade.gross_size
                trade.net_price = trade.gross_price
                trade.roi = 100 * (-1) * (trade.gross_price - trade.bought_gross_price - trade.selling_fee - trade.buying_fee) / trade.bought_gross_price

        # CEDE COMMENT self.trades never used
        return trade.success

    @authentication_required
    def export_history(self, target):
        pass

    def _build_df_open_positions(self, open_positions):
        df_open_positions = pd.DataFrame(columns=["symbol", "holdSide", "leverage", "marginCoin",
                                                  "available", "total", "usdtEquity",
                                                  "marketPrice", "averageOpenPrice",
                                                  "achievedProfits", "unrealizedPL", "liquidationPrice"])
        for i in range(len(open_positions)):
            data = open_positions[i]
            df_open_positions.loc[i] = pd.Series({"symbol": data["symbol"], "holdSide": data["holdSide"], "leverage": data["leverage"], "marginCoin": data["marginCoin"],
                                                  "available": float(data["available"]), "total": float(data["total"]), "usdtEquity": float(data["margin"]),
                                                  "marketPrice": float(data["marketPrice"]),  "averageOpenPrice": float(data["averageOpenPrice"]),
                                                  "achievedProfits": float(data["achievedProfits"]), "unrealizedPL": float(data["unrealizedPL"]), "liquidationPrice": float(data["liquidationPrice"])
                                                  })
        return df_open_positions
