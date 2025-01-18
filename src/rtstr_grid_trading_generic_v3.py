from . import rtdp, rtstr
import math
import pandas as pd
import numpy as np

import copy

from . import utils
from src import logger

class StrategyGridTradingGenericV3(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.grid_high = 0
        self.grid_low = 0
        self.nb_grid = 0
        self.grid_margin = 0
        self.percent_per_grid = 0
        self.nb_position_limits = 1
        self.side = ""
        self.id = ""
        if params:
            self.id = params.get("id", self.id)
            self.side = params.get("type", self.side)
            self.lst_symbols = [params.get("strategy_symbol", self.lst_symbols)]
            self.symbol = params.get("strategy_symbol", self.lst_symbols)

            self.grid_high = params.get("grid_high", self.grid_high)
            if isinstance(self.grid_high, str):
                if len(self.grid_high) > 0:
                    self.grid_high = float(self.grid_high)
                elif len(self.grid_high) == 0:
                    self.grid_high = 0
            self.grid_low = params.get("grid_low", self.grid_low)
            if isinstance(self.grid_low, str):
                if len(self.grid_low) > 0:
                    self.grid_low = float(self.grid_low)
                elif len(self.grid_low) == 0:
                    self.grid_low = 0
            if self.grid_high < self.grid_low:
                tmp = self.grid_low
                self.grid_low = self.grid_high
                self.grid_high = tmp
            self.nb_grid = params.get("nb_grid", self.nb_grid)
            if isinstance(self.nb_grid, str):
                if len(self.nb_grid) > 0:
                    self.nb_grid = int(self.nb_grid)
                elif len(self.nb_grid) == 0:
                    self.nb_grid = 0
            self.percent_per_grid = params.get("percent_per_grid", self.percent_per_grid)
            if isinstance(self.percent_per_grid, str):
                if len(self.percent_per_grid) > 0:
                    self.percent_per_grid = float(self.percent_per_grid)
                elif len(self.percent_per_grid) == 0:
                    self.percent_per_grid = 0
            self.grid_margin = params.get("grid_margin", self.grid_margin)
            if isinstance(self.grid_margin, str):
                if len(self.grid_margin) > 0:
                    self.grid_margin = float(self.grid_margin)
                elif len(self.grid_margin) == 0:
                    self.grid_margin = 0
            self.nb_position_limits = params.get("nb_position_limits", self.nb_position_limits)
            if isinstance(self.nb_position_limits, str):
                if len(self.nb_position_limits) > 0:
                    self.nb_position_limits = float(self.nb_position_limits)
                elif len(self.nb_position_limits) == 0:
                    self.nb_position_limits = 0
        else:
            exit(5)

        self.zero_print = True
        self.strategy_id = utils.generate_random_id(4)
        self.grid = GridPosition(self.get_info(),
                                 self.side,
                                 self.symbol,
                                 self.grid_high, self.grid_low, self.nb_grid, self.percent_per_grid,
                                 self.nb_position_limits,
                                 self.strategy_id,
                                 self.zero_print,
                                 self.loggers)
        if self.percent_per_grid != 0:
            self.nb_grid = self.grid.get_grid_nb_grid()
        self.df_grid_buying_size = pd.DataFrame()
        self.execute_timer = None
        self.df_price = None
        self.mutiple_strategy = False

        self.previous_stats = {}
        self.self_execute_trade_recorder_not_active = True

    def get_data_description(self):
        return {}

    def get_info(self):
        return "StrategyGridTradingGenericV3"

    def get_symbol(self):
        return self.symbol

    def get_strategy_type(self):
        return "CONTINUE"

    def set_multiple_strategy(self):
        self.mutiple_strategy = True

    def set_execute_time_recorder(self, execute_timer):
        if self.self_execute_trade_recorder_not_active:
            self.iter_set_broker_current_state = 0
            return

        if self.execute_timer is not None:
            del self.execute_timer
        self.execute_timer = execute_timer
        self.iter_set_broker_current_state = 0

    def set_df_grid_buying_size(self, df_grid_buying_size):
        self.df_grid_buying_size = df_grid_buying_size
        del df_grid_buying_size

    def get_strategy_id_code(self):
        return self.strategy_id

    def set_broker_current_state(self, current_state):
        """
        current_state = {
            "open_orders": df_open_orders,
            "open_positions": df_open_positions,
            "prices": df_prices
        }
        """
        if not current_state:
            return []

        df_current_states = current_state["open_orders"].copy()
        # df_open_positions = current_state["open_positions"].copy()
        df_open_triggers = current_state["triggers"].copy()
        df_price = current_state["prices"].copy()

        if not self.mutiple_strategy:
            del current_state["open_orders"]
            del current_state["open_positions"]
            del current_state["prices"]
            del current_state

        del self.df_price
        self.df_price = df_price

        self.grid.set_current_price(df_price.loc[df_price['symbols'] == self.symbol, 'values'].values[0])

        self.grid.update_grid_side()

        self.grid.update_orders_id_status(self.symbol, df_current_states, df_open_triggers)

        lst_order_to_execute = self.grid.get_order_list(self.symbol)

        if not self.zero_print:
            df_sorted = df_current_states.sort_values(by='price')
            self.log("#############################################################################################")
            self.log("current_state: \n" + df_sorted.to_string(index=False))
            del df_sorted
            self.log("price: \n" + df_price.to_string(index=False))
            lst_order_to_print = []
            for order in lst_order_to_execute:
                lst_order_to_print.append((order["grid_id"], order["price"], order["type"]))
            self.log("order list: \n" + str(lst_order_to_print))
            del lst_order_to_print

        del df_current_states
        del df_price

        # self.iter_set_broker_current_state += 1

        return lst_order_to_execute

    def print_grid(self):
        self.grid.print_grid()

    def save_grid_scenario(self, path, cpt):
        self.grid.save_grid_scenario(self.symbol, path, cpt)

    def set_normalized_grid_price(self, lst_symbol_plc_endstp):
        return

    def get_info_msg_status(self):
        stats = self.grid.get_stats()
        if not(utils.dicts_are_equal(stats, self.previous_stats)):
            msg_stats = utils.dict_to_string(stats)
            self.previous_stats = copy.deepcopy(stats)
            del stats
            return msg_stats
        del stats
        return ""

    def get_grid(self, cpt):
        return self.grid.get_grid(self.symbol, cpt)

    def record_status(self):
        if self.self_execute_trade_recorder_not_active:
            return

        if hasattr(self, 'df_price'):
            df_grid_values = self.grid.get_grid_for_record(self.symbol)
            self.execute_timer.set_grid_infos(df_grid_values, self.side)

    def update_executed_trade_status(self, lst_orders):
        if lst_orders is None:
            return []
        if len(lst_orders) > 0:
            self.grid.update_executed_trade_status(self.symbol, lst_orders)

    def get_strategy_id(self):
        return self.strategy_id

    def set_df_normalize_buying_size(self, df_normalized_buying_size):
        self.df_grid_buying_size = df_normalized_buying_size
        self.grid.set_grid_buying_size(self.df_grid_buying_size)
        self.grid.cross_check_buying_size(self.symbol, self.get_grid_buying_min_size(self.symbol))

    def set_df_buying_size(self, df_symbol_size, cash):
        if not isinstance(df_symbol_size, pd.DataFrame):
            return
        # cash = 10000 # CEDE GRID SCENARIO
        self.df_grid_buying_size = pd.concat([self.df_grid_buying_size, df_symbol_size])
        self.df_grid_buying_size['margin'] = None

        i = 0
        min_amount_requested = self.grid_margin
        while True:
            # Step 1: Generate the 'position' column
            positions = np.linspace(self.grid_low, self.grid_high, self.nb_grid)

            # Step 2: Retrieve 'pricePlace' and 'priceEndStep' values
            symbol_data = self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol]
            price_place = symbol_data['pricePlace'].values[0]
            price_end_step = symbol_data['priceEndStep'].values[0]

            # Step 3: Normalize 'position' values
            normalized_positions = [utils.normalize_price(pos, price_place, price_end_step) for pos in positions]

            # Step 4: Set the 'size' column
            size_value = symbol_data['minBuyingSize'].values[0] + symbol_data['sizeMultiplier'].values[0] * i
            sizes = [size_value] * self.nb_grid

            # Step 5: Compute the 'val' column
            vals = [size * pos for size, pos in zip(sizes, normalized_positions)]

            # Create the DataFrame
            df = pd.DataFrame({
                'position': normalized_positions,
                'size': sizes,
                'val': vals
            })

            if df["val"].sum() < self.grid_margin:
                min_amount_requested = df["val"].sum()
                dol_per_grid = df["val"].min()
                size = size_value
                size = utils.normalize_size(size,
                                            self.df_grid_buying_size.loc[
                                                self.df_grid_buying_size['symbol'] == self.symbol,
                                                "sizeMultiplier"].values[0])
                i += 1
            else:
                break  # Exit the loop when the condition is met

        if (min_amount_requested < self.grid_margin) \
                and (dol_per_grid > 5) \
                and ((cash >= self.grid_margin)
                     or (cash >= min_amount_requested)):

            self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, "strategy_id"] = self.strategy_id
            self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, "buyingSize"] = size    # CEDE: Average size
            self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, "margin"] = self.grid_margin
            self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, "maxSizeToBuy"] = self.nb_grid
            msg = "**" + self.symbol + "**\n"
            msg += "**cash: " + str(round(cash, 2)) + "**\n"
            msg += "**grid_margin: " + str(round(self.grid_margin, 2)) + "**\n"
            msg += "**nb grid: " + str(self.nb_grid) + "**\n"
            msg += "**steps: " + str((self.grid_high - self.grid_low) / self.nb_grid) + "**\n"
            msg += "**amount buying > 5 usd: " + str(round(size * self.grid_low, 2)) + "**\n"
            msg += "**buying size: " + str(size) \
                   + " - $" + str(size * (self.grid_high + self.grid_low )/2) + "**\n"
            msg += "**min size: " + str(self.get_grid_buying_min_size(self.symbol)) \
                   + " - $" + str(self.get_grid_buying_min_size(self.symbol) * (self.grid_high + self.grid_low )/2) + "**\n"
            msg += "**strategy verified" + "**\n"
            self.log(msg, "GRID SETUP")
        else:
            msg = "**" + self.symbol + "**\n"
            msg += "**cash: " + str(round(cash, 2)) + "**\n"
            msg += "**grid_margin: " + str(round(self.grid_margin, 2)) + "**\n"
            msg += "**nb grid: " + str(self.nb_grid) + "**\n"
            msg += "**steps: " + str((self.grid_high - self.grid_low) / self.nb_grid) + "**\n"
            msg += "**amount buying > 5 usd: " + str(round(size * self.grid_low, 2)) + "**\n"
            msg += "**buying size: " + str(size) + " - $" + str(size * (self.grid_high + self.grid_low )/2) + "**\n"
            msg += "**min size: " + str(self.get_grid_buying_min_size(self.symbol)) \
                   + " - $" + str(self.get_grid_buying_min_size(self.symbol) * (self.grid_high + self.grid_low )/2) + "**\n"
            msg += "**strategy stopped : ERROR NOT ENOUGH $ FOR GRID - INCREASE MARGIN OR REDUCE GRID SIZE **\n"
            self.log(msg, "GRID SETUP FAILED")
            print(msg)
            print("GRID SETUP FAILED")
            print("set_df_buying_size")
            exit(2)
        return self.df_grid_buying_size


class GridPosition:
    def __init__(self, strategy_name, side, symbol, grid_high, grid_low, nb_grid, percent_per_grid,
                 nb_position_limits, strategy_id, debug_mode=True, loggers=[]):
        self.grid_side = side
        side_mapping = {
            "long": ("open_long", "close_long"),
            "short": ("open_short", "close_short")
        }
        self.side_mapping_sltp = {
            "long": "buy",
            "short": "sell"
        }
        self.str_open, self.str_close = side_mapping.get(self.grid_side, (None, None))

        self.strategy_id = strategy_id
        self.grid_high = grid_high
        self.grid_low = grid_low
        self.nb_grid = nb_grid
        self.symbol = symbol
        self.percent_per_grid = percent_per_grid

        self.zero_print = debug_mode
        self.loggers = loggers
        self.current_price = None
        self.percent_per_grid = percent_per_grid
        self.steps = 0
        self.msg = ""
        self.df_grid_string = ""

        self.current_state = {}

        if self.percent_per_grid !=0:
            self.steps = self.grid_high * self.percent_per_grid / 100
            self.nb_grid = int((self.grid_high - self.grid_low) / self.steps)
        # Create a list with nb_grid split between high and low
        self.lst_grid_values = np.linspace(self.grid_high, self.grid_low, self.nb_grid + 1, endpoint=True).tolist()
        if self.grid_side == "long":
            low = self.lst_grid_values[-2]
            high = max(self.lst_grid_values) + (self.grid_high - self.grid_low) / self.nb_grid
            self.lst_grid_values_close = np.linspace(high, low, self.nb_grid + 1, endpoint=True).tolist()
        elif self.grid_side == "short":
            low = self.grid_low - (self.grid_high - self.grid_low) / self.nb_grid
            high = self.lst_grid_values[1]
            self.lst_grid_values_close = np.linspace(high, low, self.nb_grid + 1, endpoint=True).tolist()

        self.log("nb_grid: {}".format(self.nb_grid))
        self.log("grid steps: {}".format(self.steps))
        self.log("grid values: {}".format(self.lst_grid_values))

        self.columns = ["grid_id", "close_grid_id",
                        "position", "close_position",
                        "side", "size", "dol_per_grid",
                        "status", "orderId"
                        ]

        # self.grid = {key: pd.DataFrame(columns=self.columns) for key in self.lst_symbols}
        self.grid = pd.DataFrame(columns=self.columns)

        self.grid["position"] = self.lst_grid_values
        self.grid["close_position"] = self.lst_grid_values_close

        self.grid["grid_id"] = np.arange(len(self.grid))[::-1]
        if self.grid_side == "long":
            sequence = [-1] + list(np.arange(len(self.grid) - 1, 0, -1))
        elif self.grid_side == "short":
            sequence = list(np.arange(len(self.grid) - 2, -1, -1)) + [-1]
        self.grid['close_grid_id'] = sequence
        del sequence

        self.grid["orderId"] = "empty"
        self.grid["side"] = ""
        self.grid["orderId"] = "empty"

        self.nb_position_limits = nb_position_limits
        for i in range(self.nb_position_limits):
            column_name = f"orderId_TP_{i}"
            self.grid[column_name] = "empty"

        self.grid["status"] = "empty"

        self.stats = {
            "startegy": strategy_name,
            "side": side,
            "nb_open_order": 0,
            "nb_open_tp": 0,
            "total_closed": 0,
            "total_triggered": 0
        }

    def get_stats(self):
        return self.stats

    def log(self, msg, header="", attachments=[]):
        if self.zero_print:
            return
        for iter_logger in self.loggers:
            iter_logger.log(msg, header=header, author=type(self).__name__, attachments=attachments)

    def get_grid_nb_grid(self):
        return self.nb_grid

    def update_grid_side(self):
        df = self.grid
        position = self.current_price

        side_map = {
            "long": {"greater": self.str_close, "less": self.str_open},
            "short": {"greater": self.str_open, "less": self.str_close}
        }
        df.loc[df['position'] > position, 'side'] = side_map[self.grid_side]['greater']
        df.loc[df['position'] < position, 'side'] = side_map[self.grid_side]['less']

        df.loc[df['position'] == self.current_price, 'side'] = "on_edge"

        differences = df['position'].diff().dropna()

        diff = differences.mean() / 5
        df.loc[(df['position'] >= self.current_price - abs(diff))
               & (df['position'] <= self.current_price + abs(diff)), 'side'] = "on_edge"

    def update_orders_id_status(self, symbol, df_current_state, df_open_triggers):
        self.current_state = {}
        self.current_state[self.grid_side] = {}
        if not df_current_state.empty:
            df_current_state = df_current_state[df_current_state['symbol'] == symbol]
            df_current_state = df_current_state[df_current_state['strategyId'] == self.strategy_id]

            condition_side = df_current_state['side'] == "open_" + self.grid_side
            df_current_state_side = df_current_state[condition_side]

            self.current_state[self.grid_side]["lst_open_orders_order_id_all"] = df_current_state["orderId"].to_list()
            self.current_state[self.grid_side]["lst_open_orders_order_id_side"] = df_current_state_side["orderId"].to_list()

            order_id_list = self.current_state[self.grid_side]["lst_open_orders_order_id_side"]
            self.grid['orderId'] = self.grid['orderId'].apply(lambda x: "empty" if x not in order_id_list else x)
        else:
            self.current_state[self.grid_side]["lst_open_orders_order_id_all"] = []
            self.current_state[self.grid_side]["lst_open_orders_order_id_side"] = []
            self.grid['orderId'] = "empty"

        if not df_open_triggers.empty:
            df_open_triggers = df_open_triggers[df_open_triggers["symbol"] == symbol]
            df_open_triggers = df_open_triggers.loc[df_open_triggers['strategyId'] == self.strategy_id]
            df_open_triggers = df_open_triggers[df_open_triggers['planType'] == 'profit_plan']

            condition_open = df_open_triggers['planStatus'] != 'executed'
            condition_not_canceled = df_open_triggers['planStatus'] != 'cancelled'
            condition_side_sltp = df_open_triggers['side'] == self.side_mapping_sltp.get(self.grid_side, None)

            df_open_triggers_side = df_open_triggers[condition_open
                                                     & condition_not_canceled
                                                     & condition_side_sltp]

            self.current_state[self.grid_side]["lst_TP_orders_order_id"] = df_open_triggers["orderId"].to_list()
            self.current_state[self.grid_side]["lst_TP_orders_order_id_side"] = df_open_triggers_side["orderId"].to_list()

            order_id_list_TP = self.current_state[self.grid_side]["lst_TP_orders_order_id_side"]

            # tp_columns = [f"orderId_TP_{i}" for i in range(self.nb_position_limits)]
            # all_empty_rows = self.grid[tp_columns].apply(lambda row: (row == 'empty').all(), axis=1)
            # before_update = all_empty_rows.sum()

            for i in range(self.nb_position_limits):
                column_name = f"orderId_TP_{i}"
                # Apply the logic to each dynamically created column
                self.grid[column_name] = self.grid[column_name].apply(lambda x: "empty" if x not in order_id_list_TP else x
                )

            # tp_columns = [f"orderId_TP_{i}" for i in range(self.nb_position_limits)]
            # any_not_empty_rows = self.grid[tp_columns].apply(lambda row: (row != 'empty').any(), axis=1)
            # after_update = any_not_empty_rows.sum()

            for _, row in df_open_triggers_side.iterrows():
                order_id = row['orderId']
                grid_id = row['gridId']
                # Find the index of the row in self.grid where grid_id matches
                index = self.grid[self.grid['grid_id'] == grid_id].index
                if not index.empty:  # Ensure that the index is not empty
                    # Loop through orderId_TP_0 to orderId_TP_{self.nb_position_limits - 1}
                    for i in range(self.nb_position_limits):
                        column_name = f'orderId_TP_{i}'
                        # Check if the current column is empty
                        if self.grid.loc[index, column_name].iloc[0] == 'empty':
                            # Update the first empty orderId_TP column with the order_id
                            self.grid.loc[index, column_name] = order_id
                            self.stats["total_triggered"] += 1
                            break  # Exit the loop once the order_id is added

            self.stats["total_closed"] = self.stats["total_triggered"] - self.stats["nb_open_tp"]
        else:
            self.current_state[self.grid_side]["lst_TP_orders_order_id"] = []
            self.current_state[self.grid_side]["lst_TP_orders_order_id_side"] = []
            for i in range(self.nb_position_limits):
                column_name = f'orderId_TP_{i}'
                self.grid[column_name] = "empty"

        def update_status(row):
            if not row['side'] in ["open_long", "open_short"]:
                return "empty"

            # Check if all 'orderId_TP_n' columns are "empty"
            all_tp_empty = all(row[f'orderId_TP_{i}'] == "empty" for i in range(self.nb_position_limits))
            all_tp_engaged = all(row[f'orderId_TP_{i}'] != "empty" for i in range(self.nb_position_limits))

            if row['orderId'] != "empty" or all_tp_engaged:
                return "engaged"

            if row['orderId'] == "empty" and not all_tp_engaged:
                return "missing_order"

            if all_tp_empty or not all_tp_engaged:
                return "missing_order"

            return "empty"

        # Apply the function to the dataframe
        # In order to update status
        self.grid['status'] = self.grid.apply(update_status, axis=1)

        self.stats["nb_open_order"] = self.grid[self.grid['orderId'] != 'empty'].shape[0]
        tp_columns = [f"orderId_TP_{i}" for i in range(self.nb_position_limits)]
        self.stats["nb_open_tp"] = self.grid[self.grid[tp_columns].ne('empty').any(axis=1)].shape[0]

    def confirm_orderId(self, symbol, grid_id_engaged, list_orderId):
        df_grid = self.grid
        lst_orderId_from_grid = df_grid.loc[df_grid['grid_id'] == grid_id_engaged, "lst_orderId"].values[0]
        return all(order_id in list_orderId for order_id in lst_orderId_from_grid)

    def set_current_price(self, price):
        self.current_price = price

    def get_order_list(self, symbol):
        """
        order_to_execute = {
            "symbol": order.symbol,
            "gross_size": order.gross_size,
            "type": order.type,
            "price": order.price,
            "grid_id": grid_id,
            "linked_position": linked_position
        }
        """
        # Filter rows where 'status' is "missing_order"
        missing_orders_df = self.grid[self.grid['status'] == "missing_order"]

        # Select relevant columns and convert the result to a list of dictionaries
        lst_missing_orders = missing_orders_df[['grid_id', 'position', 'close_position', 'size', 'side']].to_dict(orient='records')

        lst_orders = []
        for order in lst_missing_orders:
            if order["grid_id"] != -1:
                order_to_execute = {}
                order_to_execute["strategy_id"] = self.strategy_id
                order_to_execute["symbol"] = symbol
                order_to_execute["trigger_type"] = "LIMIT_ORDER_SLTP"

                # order_to_execute["trigger_type"] = "MARKET_OPEN_POSITION"

                if order["side"] == "open_long":
                    order_to_execute["type"] = "OPEN_LONG_ORDER"
                elif order["side"] == "close_long":   # CEDE to be removed
                    order_to_execute["type"] = "CLOSE_LONG_ORDER"   # CEDE to be removed
                elif order["side"] == "open_short":
                    order_to_execute["type"] = "OPEN_SHORT_ORDER"
                elif order["side"] == "close_short":   # CEDE to be removed
                    order_to_execute["type"] = "CLOSE_SHORT_ORDER"   # CEDE to be removed
                if "type" in order_to_execute:
                    order_to_execute["price"] = order['position']
                    order_to_execute["grid_id"] = order['grid_id']
                    order_to_execute["gross_size"] = order['size']
                    order_to_execute["TP"] = order['close_position']
                    order_to_execute["trade_status"] = "pending"
                    self.grid.loc[self.grid["grid_id"] == order['grid_id'], 'status'] = 'pending'
                    lst_orders.append(order_to_execute)
                del order_to_execute

        sorting_order = ['OPEN_LONG_ORDER', 'OPEN_SHORT_ORDER', 'CLOSE_LONG_ORDER', 'CLOSE_SHORT_ORDER']
        sorted_list = sorted(lst_orders, key=lambda x: sorting_order.index(x['type']))

        return sorted_list

    def update_executed_trade_status(self, symbol, lst_orders):
        df_grid = self.grid
        for order in lst_orders:
            if order["strategy_id"] == self.strategy_id:
                grid_id = order["grid_id"]
                if order["trade_status"] == "SUCCESS":
                    df_grid.loc[df_grid["grid_id"] == grid_id, "status"] = "engaged"
                    df_grid.loc[df_grid["grid_id"] == grid_id, "orderId"] = order["orderId"]

    def print_grid(self):
        if self.zero_print:
            return
        df_grid = self.grid
        self.log("\n" + df_grid.to_string(index=False))
        del df_grid

    def cross_check_order_with_price(self, lst_orders):
        lst_filtered = []
        for order in lst_orders:
            if order["type"] in ["OPEN_LONG_ORDER"]:
                if order["price"] < self.current_price:
                    lst_filtered.append(order)
            elif order["type"] in ["CLOSE_LONG_ORDER"]:
                if order["price"] > self.current_price:
                    lst_filtered.append(order)
        return lst_filtered

    def set_on_hold_from_grid_id(self, symbol, grid_id):
        df = self.grid
        row_index = df.index[df['grid_id'] == grid_id].tolist()  # Get the index of rows where 'grid_id' equals grid_id
        for index in row_index:
            df.at[index, 'status'] = 'on_hold'  # Update the 'status' column for each matching row
        del df
        del row_index

    def normalize_grid_price(self, symbol, pricePlace, priceEndStep, sizeMultiplier):
        return

    def set_grid_buying_size(self, df_grid_buying_size):
        self.df_grid_buying_size = df_grid_buying_size

        # Calculate dol_per_grid for the symbol, dividing grid_margin by the number of rows (cells) in grid
        grid_margin = self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, "margin"].values[0]
        minBuyingSize = self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, "minBuyingSize"].values[0]
        pricePlace = self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, "pricePlace"].values[0]
        priceEndStep = self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, "priceEndStep"].values[0]
        size = self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, "buyingSize"].values[0]
        self.grid['position'] = self.grid['position'].apply(lambda x: utils.normalize_price(x, pricePlace, priceEndStep))
        self.grid['close_position'] = self.grid['close_position'].apply(lambda x: utils.normalize_price(x, pricePlace, priceEndStep))
        self.grid["size"] = size

        # Get the sizeMultiplier for the symbol
        sizeMultiplier = self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, "sizeMultiplier"].values[0]

        # Apply the normalize_size function to each value in the 'size' column
        self.grid["size"] = self.grid["size"].apply(
            lambda size: utils.normalize_size(size, sizeMultiplier))

        self.grid["dol_per_grid_verif"] = self.grid["size"] * self.grid["position"]

        # Check if any value in dol_per_grid_verif is less than 5
        if (self.grid["dol_per_grid_verif"] < 5).any() and (self.grid["size"] < minBuyingSize).any():
            print(f"Exiting because dol_per_grid_verif for {self.symbol} has a value less than $5.")
            exit(33)
        self.grid = self.grid.drop(columns=["dol_per_grid_verif"])
        return

    def cross_check_buying_size(self, symbol, buying_min_size):
        if (self.grid["size"] < buying_min_size).any():
            print(f"Exiting because buying_min_size for {symbol} has a value less than buying_min_size {buying_min_size}.")
            exit(44)
        return

    def dct_status_info_to_txt(self, symbol):
        del self.msg
        self.msg = ""
        return self.msg.upper()

    def get_grid_as_str(self, symbol):
        del self.df_grid_string
        df_grid = self.grid
        self.df_grid_string = df_grid.to_string(index=False)
        del df_grid
        return self.df_grid_string

    def get_grid(self, symbol, cycle):
        df = self.grid.copy()
        if isinstance(cycle, int):
            df['cycle'] = cycle

            column_to_move = 'cycle'
            first_column = df.pop(column_to_move)
            df.insert(0, column_to_move, first_column)
        return df

    def get_grid_for_record(self, symbol):
        df = self.grid.copy()
        df["values"] = df["side"] + "_" + df["status"]
        df.set_index('position', inplace=True)
        df = df[["values"]]
        return df

    def save_grid_scenario(self, symbol, path, cpt):
        if cpt >= 30:
            cpt += 1
            df = self.grid
            filename = path + "/grid_" + str(cpt) + ".csv"
            df.to_csv(filename)

            df_filtered = df[df["status"] == "engaged"]
            df_current_state = df_filtered.copy()
            df_current_state.rename(columns={'grid_id': 'gridId'}, inplace=True)
            df_current_state.rename(columns={'lst_orderId': 'orderId'}, inplace=True)
            df_current_state.rename(columns={'nb_position': 'leverage'}, inplace=True)
            df_current_state.rename(columns={'position': 'price'}, inplace=True)

            columns_to_drop = ['close_grid_id', 'triggered_by', 'previous_side', 'previous_status', 'changes', 'cross_checked', 'on_edge']
            df_current_state.drop(columns=columns_to_drop, inplace=True)

            df_exploded = df_current_state.explode('orderId').reset_index(drop=True)
            df_exploded['leverage'] = 1
            df_exploded['symbol'] = 'XRP'

            filename = path + "/data_" + str(cpt) + "_df_current_states.csv"
            df_exploded.reset_index(drop=True, inplace=True)
            df_reversed = df_exploded.iloc[::-1].reset_index(drop=True)
            columns = df_reversed.columns.tolist()
            columns = [columns[-1]] + columns[1:-1] + [columns[0]]
            df_reversed = df_reversed[columns]
            df_reversed.to_csv(filename)

            df_filtered = df_filtered[df_filtered["side"] == "close_long"]
            nb_pos = len(df_filtered)

            lst_columns = ["symbol", "holdSide", "leverage", "marginCoin", "available", "total", "usdtEquity", "marketPrice", "averageOpenPrice", "achievedProfits", "unrealizedPL", "liquidationPrice"]
            lst_data = ["XRP", "long", 2, "USDT", 10.0, 40.0, 5.86875, 0.58664, 0.586875, 0.0, -0.0047, 0.0]

            df = pd.DataFrame([lst_data], columns=lst_columns)
            df.loc[0, 'total'] = 10 * nb_pos

            filename = path + "/data_" + str(cpt) + "_df_open_positions.csv"
            df.reset_index(drop=True, inplace=True)
            df.to_csv(filename)