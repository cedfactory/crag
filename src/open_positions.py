import pandas as pd

class OpenPositions():
    def __init__(self, symbol, strategy_id, grouped_id,
                 strategy_name, strategy_side,
                 presetTakeProfitPrice="",
                 presetStopLossPrice=""):
        self.symbol = symbol
        self.strategy_id = strategy_id
        self.grouped_id = grouped_id
        self.strategy_name = strategy_name
        self.strategy_side = strategy_side
        self.presetTakeProfitPrice = presetTakeProfitPrice
        self.presetStopLossPrice = presetStopLossPrice

        columns = ['symbol', 'strategy_id', "grouped_id", 'trigger_type', 'strategy_name',
                   'order_id', "order_id_closing", 'orderType',
                   'amount', 'size', 'type', "trade_side",
                   'buying_price', 'selling_price', 'trade_status',
                   "stopLossId", "takeProfiId"]

        self.open_position = pd.DataFrame(columns=columns)

        self.state_mapping = {
            "open_long": "open_long",
            "open_short": "open_short",
            "close_long": "close_long",
            "close_short": "close_short"
        }

        self.state_mapping_closing = {
            "close_long": "CLOSE_LONG_ORDER",
            "close_short": "CLOSE_SHORT_ORDER"
        }

        self.stat = {
            "strategy_name": self.strategy_name,
            "symbol": self.symbol,
            "nb_open_position": 0,
            "nb_transaction_completed": 0,
            "nb_transaction_profit": 0,
            "nb_long_transaction_completed": 0,
            "nb_short_transaction_completed": 0,
            "total_PnL": 0
        }

    def create_open_market_order(self, amount, side_type, order_type):
        mapped_state = self.state_mapping.get(side_type)
        trade_side = "LONG" if "long" in mapped_state else "SHORT" if "short" in mapped_state else None
        new_order = {
            "symbol": self.symbol,
            "strategy_id": self.strategy_id,
            "grouped_id": self.grouped_id,
            "trigger_type": "MARKET_OPEN_POSITION",
            "strategy_name": self.strategy_name,
            "strategy_side": self.strategy_side,
            "order_id": "pending",
            "order_id_closing": "empty",
            "orderType": order_type,
            "amount": amount,
            "size": "pending",
            "type": mapped_state,
            "trade_side": trade_side,
            "buying_price": "pending",
            "selling_price": "pending",
            "trade_status": "pending",
            "presetTakeProfitPrice": self.presetTakeProfitPrice,
            "presetStopLossPrice": self.presetStopLossPrice,
            "stopLossId": "",
            "takeProfiId": ""
        }
        return new_order

    def validate_open_market_order(self, lst_order):
        for order in lst_order:
            if order["symbol"] in [self.symbol] \
                    and order["trigger_type"] == "MARKET_OPEN_POSITION" \
                    and (order["strategy_id"] == self.strategy_id
                         or order["grouped_id"] == self.grouped_id) \
                    and order["trade_status"] == "SUCCESS"\
                    and order["trade_side"] in self.strategy_side:
                order["strategy_id"] = self.strategy_id
                self.open_position.loc[len(self.open_position)] = order
                self.stat["nb_open_position"] = len(self.open_position)

    def create_close_market_order(self, order_type):
        # Filter for the open position matching our criteria.
        filtered_order = self.open_position[
            (self.open_position['symbol'] == self.symbol) &
            (self.open_position['strategy_id'] == self.strategy_id) &
            (self.open_position['trigger_type'] == "MARKET_OPEN_POSITION") &
            (self.open_position['trade_status'] == "SUCCESS")
            ]

        # Return early if no matching order is found.
        if filtered_order.empty:
            return None

        # Map the order_type to a state and determine the trade side.
        mapped_state = self.state_mapping.get(order_type)
        trade_side = None
        if mapped_state:
            lower_state = mapped_state.lower()
            if "long" in lower_state:
                trade_side = "LONG"
            elif "short" in lower_state:
                trade_side = "SHORT"

        # Get the first matching row.
        row = filtered_order.iloc[0]

        # Build parameters for canceling stop loss and take profit orders if they exist.
        order_cancel_stopLoss = [{
            "trigger_type": "CANCEL_ORDER",
            "strategy_id": row['strategy_id'],
            "orderId": row['stopLossId'],
            "symbol": row['symbol'],
            "productType": "usdt-futures",
            "marginCoin": "USDT"
        }] if row['stopLossId'] else []

        order_cancel_takeProfit = [{
            "trigger_type": "CANCEL_ORDER",
            "strategy_id": row['strategy_id'],
            "orderId": row['takeProfiId'],
            "symbol": row['symbol'],
            "productType": "usdt-futures",
            "marginCoin": "USDT"
        }] if row['takeProfiId'] else []

        # Construct the new order dictionary.
        new_order = {
            "symbol": row['symbol'],
            "strategy_id": row['strategy_id'],
            "grouped_id": row['grouped_id'],
            "trigger_type": "MARKET_CLOSE_POSITION",
            "strategy_name": row['strategy_name'],
            "order_id": row['order_id'],
            "close_order_id": row['order_id'],
            "order_id_closing": "pending",
            "orderType": order_type,
            "amount": row['amount'],
            "size": row['size'],
            "type": mapped_state,
            "trade_side": trade_side,
            "buying_price": row['buying_price'],
            "selling_price": "pending",
            "trade_status": "pending",
        }

        # Concatenate the new order with any cancel orders.
        return [new_order] + order_cancel_stopLoss + order_cancel_takeProfit

    def validate_close_market_order(self, lst_order):
        for order in lst_order:
            if order["symbol"] in [self.symbol] \
                    and (order["strategy_id"] == self.strategy_id
                         or order["grouped_id"] == self.grouped_id)\
                    and order["trigger_type"] == "MARKET_CLOSE_POSITION"\
                    and order["trade_status"] == "SUCCESS"\
                    and order["trade_side"] in self.strategy_side:
                order_id = order["close_order_id"]
                side = order["type"]
                diff_price = order["selling_price"] - order["buying_price"]

                self.open_position = self.open_position[self.open_position['order_id'] != order_id].copy()

                self.stat["nb_open_position"] = len(self.open_position)
                self.stat["nb_transaction_completed"] += 1
                if diff_price > 0:
                    self.stat["nb_transaction_profit"] += 1

                if "long" in side:
                    self.stat["nb_long_transaction_completed"] += 1
                elif "short" in side:
                    self.stat["nb_short_transaction_completed"] += 1

                self.stat["total_PnL"] += round(diff_price * 100 / order["buying_price"], 2)

    def get_strategy_stat(self):
        return self.stat

    def validate_market_order(self, lst_order):
        self.validate_open_market_order(lst_order)
        self.validate_close_market_order(lst_order)

    def get_nb_open_long_position(self):
        return len(self.open_position[self.open_position['type'] == "open_long"])

    def get_nb_open_short_position(self):
        return len(self.open_position[self.open_position['type'] == "open_short"])

    def get_nb_open_total_position(self):
        return self.get_nb_open_long_position() + self.get_nb_open_short_position()

    def get_lst_sltp_orderId(self):
        lst_sltp_orderId = pd.concat([
            self.open_position["stopLossId"][self.open_position["stopLossId"] != ""],
            self.open_position["takeProfiId"][self.open_position["takeProfiId"] != ""]
        ]).drop_duplicates().tolist()

        return [
            {
                "symbol": self.symbol,
                "strategy_id": self.strategy_id,
                "marginCoin": "USDT",
                "poll_interval": 1,
                "timeout": 5,
                "orderId": orderId,
            }
            for orderId in lst_sltp_orderId
        ]

    def update_sltp_orderId(self, order):
        # Only proceed if the order belongs to this strategy and there is an open position.
        if order.get("strategy_id") != self.strategy_id or self.open_position.empty:
            return

        order_id = order.get("orderId")
        order_status = order.get("orderId_status")
        # Normalize order status to uppercase for consistency.
        order_status = order_status.upper() if order_status else ""

        # Make sure the order_id is found in either the stopLossId or takeProfiId columns.
        if order_id not in self.open_position["stopLossId"].values and order_id not in self.open_position[
            "takeProfiId"].values:
            return

        # If the order is still live, no update is necessary.
        if order_status == "LIVE":
            return

        # For CANCELLED or FAIL_EXECUTE orders, clear the order_id from the corresponding columns.
        if order_status in ("CANCELLED", "FAIL_EXECUTE"):
            self.open_position.loc[self.open_position["stopLossId"] == order_id, "stopLossId"] = ""
            self.open_position.loc[self.open_position["takeProfiId"] == order_id, "takeProfiId"] = ""
            return

        # If the order has been EXECUTED, remove the entry from the open positions.
        if order_status == "EXECUTED":
            if order_id in self.open_position["stopLossId"].values:
                self.open_position = self.open_position[self.open_position["stopLossId"] != order_id]
            elif order_id in self.open_position["takeProfiId"].values:
                self.open_position = self.open_position[self.open_position["takeProfiId"] != order_id]