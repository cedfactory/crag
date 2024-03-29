from . import broker,trade,rtdp,utils
import pandas as pd

class BrokerBitGet(broker.Broker):
    def __init__(self, params = None):
        super().__init__(params)
        self.rtdp = rtdp.RealTimeDataProvider(params)
        self.simulation = False
        self.leverage = 0
        self.name = ""
        self.exchange_name = "bitget"
        self.chase_limit = False
        self.log_trade = ""
        self.zero_print = True
        if params:
            self.simulation = params.get("simulation", self.simulation)
            if self.simulation == 0 or self.simulation == "0":
                self.simulation = False
            if self.simulation == 1 or self.simulation == "1":
                self.simulation = True
            self.leverage = params.get("leverage", self.leverage)
            if isinstance(self.leverage, str):
                self.leverage = int(self.leverage)
        #if not self._authentification():
        #    print("[BrokerBitGet] : Problem encountered during authentification")

        self.clientOIdprovider = utils.ClientOIdProvider()
        # leverages management
        self.leveraged_symbols = []
        self.leverage_short = 1
        self.leverage_long = 1
        if params:
            self.leverage_short = int(params.get("leverage_short", self.leverage_short))
            self.leverage_long = int(params.get("leverage_long", self.leverage_long))
        self.df_grid_id_match = pd.DataFrame(columns=["orderId", "grid_id"])

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
        cash, _, _ = self.get_available_cash()
        info += "\nCash : $ {}".format(utils.KeepNDecimals(cash, 2))
        # info += "\nLeverage : {}".format(self.leverage)
        return info

    def log_info_trade(self):
        return self.log_trade

    def clear_log_info_trade(self):
        self.log_trade = ""

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

    def normalize_grid_df_buying_size_size(self, df_buying_size):
        if not isinstance(df_buying_size, pd.DataFrame) \
                or len(df_buying_size) == 0:
            return

        for symbol in df_buying_size['symbol'].tolist():
            size = df_buying_size.loc[df_buying_size['symbol'] == symbol, "buyingSize"].values[0]
            normalized_size = self.normalize_size(symbol, size)
            df_buying_size.loc[df_buying_size['symbol'] == symbol, "buyingSize"] = normalized_size
        return df_buying_size

    @authentication_required
    def execute_trade(self, trade):
        if not self.zero_print:
            print("!!!!!!! EXECUTE THE TRADE !!!!!!!")
        if trade.time != None:
            if not self.zero_print:
                print("execute trade at: ", trade.time)
        trade.success = False
        symbol = self._get_symbol(trade.symbol)

        self.set_margin_and_leverage(symbol)
        clientOid = self.clientOIdprovider.get_name(symbol, trade.type)
        if not self.zero_print:
            print("TRADE GROSS SIZE: ", trade.gross_size)
        trade.gross_size = self.normalize_size(symbol, trade.gross_size)
        if not self.zero_print:
            print("TRADE GROSS SIZE NORMALIZED: ", trade.gross_size)
        if hasattr(trade, 'price'):
            if not self.zero_print:
                print("TRADE GROSS PRICE: ", trade.price)
            trade.price = self.normalize_price(symbol, trade.price) # price not used yet
            if not self.zero_print:
                print("TRADE GROSS PRICE NORMALIZED: ", trade.price)

        if trade.gross_size == 0:
            if not self.zero_print:
                print('transaction failed ", trade.type, " : ', symbol, ' - gross_size: ', trade.gross_size)

        if trade.type in ["OPEN_LONG", "OPEN_SHORT", "OPEN_LONG_ORDER", "OPEN_SHORT_ORDER", "CLOSE_LONG_ORDER", "CLOSE_SHORT_ORDER"]:
            if trade.type == "OPEN_LONG":
                if not self.zero_print:
                    print(trade.type, " size: ", trade.gross_size)
                transaction = self._open_long_position(symbol, trade.gross_size, clientOid)
            elif trade.type == "OPEN_SHORT":
                if not self.zero_print:
                    print(trade.type, " size: ", trade.gross_size)
                transaction = self._open_short_position(symbol, trade.gross_size, clientOid)
            elif trade.type == "OPEN_LONG_ORDER":
                if not self.zero_print:
                    print(trade.type, " size: ", trade.gross_size, " price: ", trade.price)
                transaction = self._open_long_order(symbol, trade.gross_size, clientOid, trade.price)
            elif trade.type == "OPEN_SHORT_ORDER":
                if not self.zero_print:
                    print(trade.type, " size: ", trade.gross_size, " price: ", trade.price)
                transaction = self._open_short_order(symbol, trade.gross_size, clientOid, trade.price)
            elif trade.type == "CLOSE_LONG_ORDER":
                if not self.zero_print:
                    print(trade.type, " size: ", trade.gross_size, " price: ", trade.price)
                transaction = self._close_long_order(symbol, trade.gross_size, clientOid, trade.price)
            elif trade.type == "CLOSE_SHORT_ORDER":
                if not self.zero_print:
                    print(trade.type, " size: ", trade.gross_size, " price: ", trade.price)
                transaction = self._close_short_order(symbol, trade.gross_size, clientOid, trade.price)
            else:
                transaction = {"msg": "failure"}

            if not self.zero_print:
                print(transaction)
            if "msg" in transaction and transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                trade.success = True
                trade.orderId = transaction["data"]["orderId"]
                trade.clientOid = transaction["data"]["clientOid"]
                if "_ORDER" in trade.type:
                    trade.symbol_price = trade.price
                    trade.tradeId = trade.orderId
                else:
                    trade.tradeId, trade.symbol_price, trade.gross_price, trade.gross_size, trade.buying_fee = self.get_order_fill_detail(symbol, trade.orderId)
                trade.net_size = trade.gross_size
                trade.net_price = trade.gross_price
                trade.bought_gross_price = trade.gross_price
                trade.buying_price = trade.symbol_price

                if not self.zero_print:
                    print('request ',trade.type, ': ', symbol, ' gross_size: ', trade.gross_size)
                    print(trade.type, ': ', symbol, ' gross_size: ', trade.gross_size, ' price: ', trade.gross_price, ' fee: ', trade.buying_fee)
            else:
                if not self.zero_print:
                    print("Something went wrong inside execute_trade :")
                    print(transaction)

        elif trade.type == "CLOSE_LONG":
            trade.gross_size = self.get_symbol_available(symbol)
            transaction = self._close_long_position(symbol, trade.gross_size, clientOid)
            if "msg" in transaction and transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                trade.success = True
                trade.orderId = transaction["data"]["orderId"]
                trade.clientOid = transaction["data"]["clientOid"]
                if not self.zero_print:
                    print('request CLOSE_LONG: ', symbol, ' gross_size: ', trade.gross_size)
                trade.tradeId, trade.symbol_price, trade.gross_price, trade.gross_size, trade.selling_fee = self.get_order_fill_detail(symbol, trade.orderId)
                if not self.zero_print:
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
                if not self.zero_print:
                    print('request CLOSE_SHORT: ', symbol, ' gross_size: ', trade.gross_size)
                trade.tradeId, trade.symbol_price, trade.gross_price, trade.gross_size, trade.selling_fee = self.get_order_fill_detail(symbol, trade.orderId)
                if not self.zero_print:
                    print('CLOSE_SHORT: ', symbol, ' gross_size: ', trade.gross_size, ' price: ', trade.gross_price, ' fee: ', trade.selling_fee)
                trade.net_size = trade.gross_size
                trade.net_price = trade.gross_price
                if hasattr(trade, "bought_gross_price"):
                    # trade.roi = 100 * (-1) * (trade.gross_price - trade.bought_gross_price - trade.selling_fee - trade.buying_fee) / trade.bought_gross_price
                    trade.roi = (-1) * utils.get_variation(trade.bought_gross_price, trade.gross_price)

        if not trade.success:
            msg = 'trade failed: ' + trade.symbol
            msg += " - " + trade.type
            if hasattr(trade, 'price'):
                msg += " - " + str(trade.price)
            msg += " - " + str(trade.gross_size) + '\n'
            if not self.zero_print:
                print('transaction failed : ', trade.symbol, " - type: ", trade.type, ' - gross_size: ', trade.gross_size)
                print("!!!!!!! EXECUTE THE TRADE NOT COMPLETED !!!!!!!")
        else:
            msg = trade.symbol
            msg += " - " + trade.type
            if hasattr(trade, 'price'):
                msg += " - " + str(trade.price)
            msg += " - " + str(trade.gross_size) + '\n'
            if not self.zero_print:
                print('transaction success : ', trade.symbol, " - type: ", trade.type, ' - gross_size: ', trade.gross_size)
                print("!!!!!!! EXECUTE THE TRADE COMPLETED !!!!!!!")
        self.log_trade = self.log_trade + msg.upper()
        return trade.success

    def check_validity_order(self, order):
        # CEDE avoid empty dict field: from order -> trade
        lst_key = ["time", "success", "orderId", "clientOid", "tradeId", "symbol_price", "gross_price",
                   "buying_fee", "net_size", "net_price", "bought_gross_price", "buying_price"]
        for key in lst_key:
            if key not in order:
                order[key] = None
        return order

    def store_gridId_orderId(self, trade):
        orderId = trade.orderId
        gridId = trade.grid_id
        success = trade.success

        if success and orderId != None and gridId != -1:
            # if gridId in self.df_grid_id_match["grid_id"].tolist():
                # drop the previous grid_id used
            #    self.df_grid_id_match = self.df_grid_id_match.drop(self.df_grid_id_match[self.df_grid_id_match['grid_id'] == gridId].index)
            self.df_grid_id_match.loc[len(self.df_grid_id_match)] = [orderId, gridId]
            self.df_grid_id_match = self.df_grid_id_match.drop_duplicates()

    class OrderToTradeConverter:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

    @authentication_required
    def execute_orders(self, lst_orders):
        for order in lst_orders:
            order = self.check_validity_order(order)
            trade = self.OrderToTradeConverter(**order)
            self.execute_trade(trade)
            self.store_gridId_orderId(trade)

    def set_open_orders_gridId(self, df_open_orders):
        df_open_orders["gridId"] = None
        for orderId in df_open_orders["orderId"].tolist():
            # Define a condition
            condition = df_open_orders['orderId'] == orderId
            # Set a value in the 'City' column where the condition is true
            df_open_orders.loc[condition, 'gridId'] = self.get_gridId_from_orderId(orderId)
        return df_open_orders

    def get_gridId_from_orderId(self, orderId):
        condition = self.df_grid_id_match['orderId'] == orderId
        gridId = self.df_grid_id_match.loc[condition, 'grid_id'].values[0] if not self.df_grid_id_match.loc[condition].empty else None
        return gridId

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

    def _build_df_open_orders(self, open_orders):
        df_open_orders = pd.DataFrame(columns=["symbol", "price", "side", "size", "leverage", "marginCoin", "clientOid", "orderId"])
        for i in range(len(open_orders)):
            data = open_orders[i]
            df_open_orders.loc[i] = pd.Series({"symbol": data["symbol"], "price": data["price"], "side": data["side"], "size": data["size"], "leverage": data["leverage"], "marginCoin": data["marginCoin"], "clientOid": data["clientOid"], "orderId": data["orderId"]})
        return df_open_orders

    @authentication_required
    def execute_reset_account(self):
        df_positions = self.get_open_position()
        original_df_positions = df_positions
        #return original_df_positions

        if len(df_positions) == 0:
            usdtEquity = self.get_account_equity()
            if not self.zero_print:
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
                if not self.zero_print:
                    print('reset - close ', holdSize, 'position - symbol: ', symbol,' value: ', current_trade.gross_size, ' - $', usdtEquity)

        df_positions = self.get_open_position()
        if len(df_positions) != 0:
            if not self.zero_print:
                print("reset - failure")
        else:
            usdtEquity = self.get_account_equity()
            if not self.zero_print:
                print('reset - account cleared')
                print('equity USDT: ', usdtEquity)

        return original_df_positions

    @authentication_required
    def get_symbol_unrealizedPL(self, symbol):
        df_positions = self.get_open_position()
        try:
            unrealizedPL = df_positions.loc[(df_positions['symbol'] == symbol), "unrealizedPL"].values[0]
        except:
            if not self.zero_print:
                print("error: get_symbol_unrealizedPL ", len(df_positions))
            unrealizedPL = 0
        return unrealizedPL

    @authentication_required
    def get_symbol_holdSide(self, symbol):
        df_positions = self.get_open_position()
        try:
            holdSide = df_positions.loc[(df_positions['symbol'] == symbol), "holdSide"].values[0]
        except:
            if not self.zero_print:
                print("error: get_symbol_holdSide ", len(df_positions))
            holdSide = ""
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
        if symbol in df_positions['symbol'].tolist():
            total = df_positions.loc[(df_positions['symbol'] == symbol), "total"].values[0]
            return total
        else:
            return 0

    @authentication_required
    def get_symbol_available(self, symbol):
        df_positions = self.get_open_position()
        if symbol in df_positions['symbol'].tolist():
            available = df_positions.loc[(df_positions['symbol'] == symbol), "available"].values[0]
            return available
        else:
            return 0

    @authentication_required
    def get_symbol_data(self, symbol):
        df_positions = self.get_open_position()
        if symbol in df_positions['symbol'].tolist():
            available = df_positions.loc[(df_positions['symbol'] == symbol), "available"].values[0]
            total = df_positions.loc[(df_positions['symbol'] == symbol), "total"].values[0]
            leverage = df_positions.loc[(df_positions['symbol'] == symbol), "leverage"].values[0]
            marketPrice = df_positions.loc[(df_positions['symbol'] == symbol), "marketPrice"].values[0]
            averageOpenPrice = df_positions.loc[(df_positions['symbol'] == symbol), "averageOpenPrice"].values[0]
            unrealizedPL = df_positions.loc[(df_positions['symbol'] == symbol), "unrealizedPL"].values[0]
            liquidation = df_positions.loc[(df_positions['symbol'] == symbol), "liquidationPrice"].values[0]
            side = df_positions.loc[(df_positions['symbol'] == symbol), "holdSide"].values[0]
            return total, available, leverage, averageOpenPrice, marketPrice, unrealizedPL, liquidation, side
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
            return global_unrealizedPL
        else:
            return 0

    def get_coin_from_symbol(self, symbol):
        return self._get_coin(symbol)