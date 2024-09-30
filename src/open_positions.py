import pandas as pd

class OpenPositions():
    def __init__(self, symbol, strategy_id, strategy_name):
        self.symbol = symbol
        self.strategy_id = strategy_id
        self.strategy_name = strategy_name
        columns = ['symbol', 'strategy_id', 'trigger_type', 'strategy_name',
                   'order_id', "order_id_closing", 'orderType',
                   'amount', 'size', 'type',
                   'buying_price', 'selling_price', 'trade_status']

        self.open_position = pd.DataFrame(columns=columns)

        self.state_mapping = {
            "open_long": "open_long",
            "open_short": "open_short",
            "close_long": "close_long",
            "close_short": "close_short"
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
        new_order = {
            "symbol": self.symbol,
            "strategy_id": self.strategy_id,
            "trigger_type": "MARKET_OPEN_POSITION",
            "strategy_name": self.strategy_name,
            "order_id": "pending",
            "order_id_closing": "empty",
            "orderType": order_type,
            "amount": amount,
            "size": "pending",
            "type": self.state_mapping.get(side_type),
            "buying_price": "pending",
            "selling_price": "pending",
            "trade_status": "pending"
        }
        return new_order

    def validate_open_market_order(self, lst_order):
        for order in lst_order:
            if order["symbol"] in [self.symbol] \
                    and order["strategy_id"] == self.strategy_id \
                    and order["trigger_type"] == "MARKET_OPEN_POSITION"\
                    and order["trade_status"] == "SUCCESS":
                order["symbol"] = order["symbol"]
                self.open_position.loc[len(self.open_position)] = order

                self.stat["nb_open_position"] = len(self.open_position)

    def create_close_market_order(self, order_type):
        filtered_order = self.open_position[
            (self.open_position['symbol'] == self.symbol) &
            (self.open_position['strategy_id'] == self.strategy_id) &
            (self.open_position['trigger_type'] == "MARKET_OPEN_POSITION") &
            (self.open_position['trade_status'] == "SUCCESS")
            ]
        if not filtered_order.empty:
            row = filtered_order.iloc[0]

            # Create the dictionary using the values from the row
            new_order = {
                "symbol": row['symbol'],
                "strategy_id": row['strategy_id'],
                "trigger_type": "MARKET_CLOSE_POSITION",
                "strategy_name": row['strategy_name'],
                "order_id": row['order_id'],
                "order_id_closing": "pending",
                "orderType": order_type,
                "amount": row['amount'],
                "size": row['size'],
                "type": self.state_mapping.get(order_type),
                "buying_price": row['buying_price'],
                "selling_price": "pending",
                "trade_status": "pending"
            }

            return new_order

        return None

    def validate_close_market_order(self, lst_order):
        for order in lst_order:
            if order["symbol"] in [self.symbol] \
                    and order["strategy_id"] == self.strategy_id \
                    and order["trigger_type"] == "MARKET_CLOSE_POSITION"\
                    and order["trade_status"] == "SUCCESS":
                order_id = order["order_id"]
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