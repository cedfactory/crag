from . import broker,trade,rtdp,utils
import pandas as pd
import ast

class BrokerBitGet(broker.Broker):
    def __init__(self, params = None):
        super().__init__(params)
        self.rtdp = rtdp.RealTimeDataProvider(params)
        self.simulation = False
        self.leverage = 0
        self.name = ""
        self.exchange_name = "bitget"
        self.chase_limit = False
        self.reset_account = True  # Reset account default behavior
        if params:
            self.simulation = params.get("simulation", self.simulation)
            if self.simulation == 0 or self.simulation == "0":
                self.simulation = False
            if self.simulation == 1 or self.simulation == "1":
                self.simulation = True
            self.leverage = params.get("leverage", self.leverage)
            if isinstance(self.leverage, str):
                self.leverage = int(self.leverage)
            self.reset_account = params.get("reset_account", self.reset_account)
            if isinstance(self.reset_account, str):
                try:
                    self.reset_account = ast.literal_eval(self.reset_account)
                except BaseException as err:
                    self.reset_account = True
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

    def resume_strategy(self):
        return not self.reset_account

    def ready(self):
        return self.marketApi != None and self.accountApi != None

    def log_info(self):
        info = ""
        info += "{}".format(type(self).__name__)
        info += "\nCash : $ {}".format(utils.KeepNDecimals(self.get_cash(), 2))
        # info += "\nLeverage : {}".format(self.leverage)
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
        if trade.time != None:
            print("execute trade at: ", trade.time)
        trade.success = False
        symbol = self._get_symbol(trade.symbol)

        self.set_margin_and_leverage(symbol)
        clientOid = self.clientOIdprovider.get_name(symbol, trade.type)
        print("TRADE GROSS SIZE: ", trade.gross_size)
        trade.gross_size = self.normalize_size(symbol, trade.gross_size)
        # trade.gross_price = self.normalize_price(symbol, trade.gross_price) # price not used yet
        print("TRADE GROSS SIZE NORMALIZED: ", trade.gross_size)

        if trade.gross_size == 0:
            print('transaction failed ", trade.type, " : ', symbol, ' - gross_size: ', trade.gross_size)

        if trade.type == "OPEN_LONG":
            transaction = self._open_long_position(symbol, trade.gross_size, clientOid)
            if transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                trade.success = True
                trade.orderId = transaction["data"]["orderId"]
                trade.clientOid = transaction["data"]["clientOid"]
                print('request OPEN_LONG: ', symbol, ' gross_size: ', trade.gross_size)
                trade.tradeId, trade.symbol_price, trade.gross_price, trade.gross_size, trade.buying_fee = self.get_order_fill_detail(symbol, trade.orderId)
                print('OPEN_LONG: ', symbol, ' gross_size: ', trade.gross_size, ' price: ', trade.gross_price, ' fee: ', trade.buying_fee)
                trade.net_size = trade.gross_size
                trade.net_price = trade.gross_price
                trade.bought_gross_price = trade.gross_price
                trade.buying_price = trade.symbol_price

        elif trade.type == "OPEN_SHORT":
            transaction = self._open_short_position(symbol, trade.gross_size, clientOid)
            if transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                trade.success = True
                trade.orderId = transaction["data"]["orderId"]
                trade.clientOid = transaction["data"]["clientOid"]
                print('request OPEN_SHORT: ', symbol, ' gross_size: ', trade.gross_size)
                trade.tradeId, trade.symbol_price, trade.gross_price, trade.gross_size, trade.buying_fee = self.get_order_fill_detail(symbol, trade.orderId)
                print('OPEN_SHORT: ', symbol, ' gross_size: ', trade.gross_size, ' price: ', trade.gross_price, ' fee: ', trade.buying_fee)
                trade.net_size = trade.gross_size
                trade.net_price = trade.gross_price
                trade.bought_gross_price = trade.gross_price
                trade.buying_price = trade.symbol_price

        elif trade.type == "CLOSE_LONG":
            trade.gross_size = self.get_symbol_available(symbol)
            transaction = self._close_long_position(symbol, trade.gross_size, clientOid)
            if transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                trade.success = True
                trade.orderId = transaction["data"]["orderId"]
                trade.clientOid = transaction["data"]["clientOid"]
                print('request CLOSE_LONG: ', symbol, ' gross_size: ', trade.gross_size)
                trade.tradeId, trade.symbol_price, trade.gross_price, trade.gross_size, trade.selling_fee = self.get_order_fill_detail(symbol, trade.orderId)
                print('CLOSE_LONG: ', symbol, ' gross_size: ', trade.gross_size, ' price: ', trade.gross_price, ' fee: ', trade.selling_fee)
                # CEDE to be confirmed : selling_fee is selling_fee + buying_fee or just selling_fee
                trade.net_size = trade.gross_size
                trade.net_price = trade.gross_price
                if hasattr(trade, "bought_gross_price"):
                   # trade.roi = 100 * (trade.gross_price - trade.bought_gross_price - trade.selling_fee - trade.buying_fee) / trade.bought_gross_price
                   trade.roi = utils.get_variation(trade.bought_gross_price, trade.gross_price)

        elif trade.type == "CLOSE_SHORT":
            trade.gross_size = self.get_symbol_available(symbol)
            transaction = self._close_short_position(symbol, trade.gross_size, clientOid)
            if transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                trade.success = True
                trade.orderId = transaction["data"]["orderId"]
                trade.clientOid = transaction["data"]["clientOid"]
                print('request CLOSE_SHORT: ', symbol, ' gross_size: ', trade.gross_size)
                trade.tradeId, trade.symbol_price, trade.gross_price, trade.gross_size, trade.selling_fee = self.get_order_fill_detail(symbol, trade.orderId)
                print('CLOSE_SHORT: ', symbol, ' gross_size: ', trade.gross_size, ' price: ', trade.gross_price, ' fee: ', trade.selling_fee)
                # CEDE to be confirmed : selling_fee is selling_fee + buying_fee or just selling_fee
                trade.net_size = trade.gross_size
                trade.net_price = trade.gross_price
                if hasattr(trade, "bought_gross_price"):
                    # trade.roi = 100 * (-1) * (trade.gross_price - trade.bought_gross_price - trade.selling_fee - trade.buying_fee) / trade.bought_gross_price
                    trade.roi = (-1) * utils.get_variation(trade.bought_gross_price, trade.gross_price)

        if not trade.success:
            print('transaction failed ", trade.type, " : ', symbol, ' - gross_size: ', trade.gross_size)
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

    @authentication_required
    def execute_reset_account(self):
        df_positions = self.get_open_position()
        original_df_positions = df_positions

        if len(df_positions) == 0:
            usdtEquity = self.get_account_equity()
            print("reset - no position - account already cleared")
            print('equity USDT: ', usdtEquity)
            return original_df_positions

        original_df_positions = df_positions

        for symbol in df_positions['symbol'].tolist():
            current_trade = trade.Trade()
            current_trade.symbol = symbol
            current_trade.gross_size = df_positions.loc[(df_positions['symbol'] == symbol), "available"].values[0]
            current_trade.type = "CLOSE_LONG"
            holdSize = df_positions.loc[(df_positions['symbol'] == symbol), "holdSide"].values[0]
            if holdSize == 'long':
                current_trade.type = "CLOSE_LONG"
            else:
                current_trade.type = "CLOSE_SHORT"
            current_trade.minsize = 0
            res = self.execute_trade(current_trade)
            if res:
                usdtEquity = df_positions.loc[(df_positions['symbol'] == symbol), "usdtEquity"].values[0]
                print('reset - close ', holdSize, 'position - symbol: ', symbol,' value: ', current_trade.gross_size, ' - $', usdtEquity)

        df_positions = self.get_open_position()
        if len(df_positions) != 0:
            print("reset - failure")
        else:
            usdtEquity = self.get_account_equity()
            print('reset - account cleared')
            print('equity USDT: ', usdtEquity)

        return original_df_positions

    @authentication_required
    def get_symbol_unrealizedPL(self, symbol):
        df_positions = self.get_open_position()
        unrealizedPL = df_positions.loc[(df_positions['symbol'] == symbol), "unrealizedPL"].values[0]
        return unrealizedPL

    @authentication_required
    def get_symbol_holdSide(self, symbol):
        df_positions = self.get_open_position()
        holdSide = df_positions.loc[(df_positions['symbol'] == symbol), "holdSide"].values[0]
        return holdSide

    @authentication_required
    def get_symbol_averageOpenPrice(self, symbol):
        df_positions = self.get_open_position()
        averageOpenPrice = df_positions.loc[(df_positions['symbol'] == symbol), "averageOpenPrice"].values[0]
        return averageOpenPrice

    @authentication_required
    def get_symbol_marketPrice(self, symbol):
        df_positions = self.get_open_position()
        marketPrice = df_positions.loc[(df_positions['symbol'] == symbol), "marketPrice"].values[0]
        return marketPrice

    @authentication_required
    def get_symbol_total(self, symbol):
        df_positions = self.get_open_position()
        total = df_positions.loc[(df_positions['symbol'] == symbol), "total"].values[0]
        return total

    @authentication_required
    def get_symbol_available(self, symbol):
        df_positions = self.get_open_position()
        if symbol in df_positions['symbol'].tolist():
            available = df_positions.loc[(df_positions['symbol'] == symbol), "available"].values[0]
            return available
        else:
            return 0

    @authentication_required
    def get_symbol_usdtEquity(self, symbol):
        df_positions = self.get_open_position()
        usdtEquity = df_positions.loc[(df_positions['symbol'] == symbol), "usdtEquity"].values[0]
        return usdtEquity

    @authentication_required
    def get_lst_symbol_position(self):
        df_positions = self.get_open_position()
        return df_positions['symbol'].tolist()

    @authentication_required
    def get_global_unrealizedPL(self):
        df_positions = self.get_open_position()
        if len(df_positions) > 0:
            global_unrealizedPL = df_positions["unrealizedPL"].sum()
            # print('global_unrealizedPL: ', global_unrealizedPL) #CEDE DEBUG
            return global_unrealizedPL
        else:
            return 0

    def get_coin_from_symbol(self, symbol):
        return self._get_coin(symbol)

    def is_reset_account(self):
        return self.reset_account
