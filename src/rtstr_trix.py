import pandas as pd
import json

from . import rtdp, rtstr

import utils

class StrategyTrix(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.strategy_id = utils.generate_random_id(4)
        self.df_current_data = pd.DataFrame()


        self.side = ""
        self.id = ""
        self.symbol = ""
        if params:
            self.id = params.get("id", self.id)
            self.lst_symbols = [params.get("strategy_symbol", self.lst_symbols)]
            self.symbol = params.get("strategy_symbol", self.symbol)
            self.strategy_str_interval = params.get("interval", self.strategy_str_interval)
            self.trix_period = params.get("trix_period", self.trix_period)
            self.stoch_rsi_period = params.get("stoch_rsi_period", self.stoch_rsi_period)
        else:
            exit(5)

        self.positions = OpenPositions(self.lst_symbols, self.strategy_id, self.get_info())
        self.zero_print = True

        self.mutiple_strategy = False

        self.interval_map = {
            "1m": 60,
            "5m": 5 * 60,
            "15m": 15 * 60,
            "30m": 30 * 60,
            "1h": 60 * 60,
            "2h": 2 * 60 * 60,
            "4h": 4 * 60 * 60
        }

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.strategy_id = self.strategy_id
        ds.strategy_name = self.get_info()
        ds.symbols = self.lst_symbols
        ds.fdp_features = {
            "close": {},
            "trix_histo_id1" : {"indicator": "trix_histo", "trix_window_size": self.trix_period, "window_size": self.trix_period, "id": "1",
                                "output": ["trix_histo", "stoch_rsi"]},
            "stoch_rsi": {"indicator": "stoch_rsi", "stoch_rsi_window_size": self.stoch_rsi_period, "window_size": self.stoch_rsi_period, "id": "1",
                          "output": ["trix_histo", "stoch_rsi"]}
        }

        """
        ds.fdp_features = {"close": {},
                           "bollinger_id1": {"indicator": "bollinger", "window_size": 20, "id": "1", "bol_std": 2.5, "output": ["lower_band", "higher_band", "ma_band"]},
                           "rsi": {"indicator": "rsi", "id": "1", "window_size": 14},
                           "atr": {"indicator": "atr", "id": "1", "window_size": 14},
                           "long_ma": {"indicator": "sma", "id": "long_ma", "window_size": 100},
                           "postprocess1": {"indicator": "shift", "window_size": 1, "id": "1", "n": "1", "input": ['lower_band', "higher_band", "ma_band"]},
                           "postprocess2": {"indicator": "shift", "window_size": 1, "n": "1", "input": ["close"]}
                           }
        """

        ds.features = self.get_feature_from_fdp_features(ds.fdp_features)
        # ds.interval = self.strategy_interval
        ds.interval = self.interval_map.get(self.strategy_str_interval, None)
        ds.str_interval = self.strategy_str_interval
        ds.current_data = pd.DataFrame()

        return ds

    def get_info(self):
        return "StrategyTrix"

    def get_strategy_id(self):
        return self.strategy_id

    def get_interval(self):
        return self.strategy_str_interval

    def set_current_state(self, ds):
        self.symbol = ds.symbols[0]
        self.df_current_data = ds.current_data.copy()

    def set_current_data(self, current_data):
        self.df_current_data = current_data

    def set_multiple_strategy(self):
        self.mutiple_strategy = True

    def get_lst_trade(self):
        open_long = self.condition_for_opening_long_position(self.symbol)
        open_short = self.condition_for_closing_long_position(self.symbol)
        close_long = self.condition_for_closing_long_position(self.symbol)
        close_short = self.condition_for_closing_short_position(self.symbol)

        lst_order = []
        if self.positions.get_nb_open_total_position() > 0 \
                and open_long:
            lst_order.append(self.positions.create_open_market_order(self.margin, "open_long", "MARKET_OPEN_POSITION"))
        if self.positions.get_nb_open_total_position() > 0 \
                and open_short:
            lst_order.append(self.positions.create_open_market_order(self.margin, "open_short", "MARKET_OPEN_POSITION"))
        if self.positions.get_nb_open_long_position() > 0 \
                and close_long:
            lst_order.append(self.positions.create_close_market_order(self.margin, "close_long", "MARKET_CLOSE_POSITION"))
        if self.positions.get_nb_open_short_position() > 0 \
                and close_short:
            lst_order.append(self.positions.create_close_market_order(self.margin, "close_short", "MARKET_CLOSE_POSITION"))
        return lst_order

    def condition_for_opening_long_position(self, symbol):
        return self.df_current_data['trix_histo_1'][symbol] > 0 \
               and self.df_current_data['stoch_rsi_1'][symbol] < 0.8

    def condition_for_closing_long_position(self, symbol):
        return self.df_current_data['trix_histo_1'][symbol] < 0 \
               and self.df_current_data['stoch_rsi_1'][symbol] > 0.2

    def condition_for_closing_long_position(self, symbol):
        return False

    def condition_for_closing_short_position(self, symbol):
        return False

    def update_executed_trade_status(self, lst_order):
        return self.positions.validate_market_order(lst_order)

    def get_strategy_stat(self):
        stat_string = json.dumps(self.positions.get_strategy_stat(), indent=4)
        return stat_string

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
            if order.symbol in self.symbol \
                    and order.strategy_id == self.strategy_id \
                    and order.trigger_type == "MARKET_OPEN_POSITION"\
                    and order.trade_status == "SUCCESS":
                self.open_position.loc[len(self.open_position)] = order

                self.stat["nb_open_position"] = len(self.open_position)

    def create_close_market_order(self, side_type, order_type):
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
                "type": self.state_mapping.get(side_type),
                "buying_price": row['buying_price'],
                "selling_price": "pending",
                "trade_status": "pending"
            }

            return new_order

        return None

    def validate_close_market_order(self, lst_order):
        for order in lst_order:
            if order.symbol in self.symbol \
                    and order.strategy_id == self.strategy_id \
                    and order.trigger_type == "MARKET_CLOSE_POSITION"\
                    and order.trade_status == "SUCCESS":
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