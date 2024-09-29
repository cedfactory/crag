from . import rtdp, rtstr
import math
import pandas as pd
import numpy as np

from . import utils
from src import logger

class StrategyGridTradingGenericV2(rtstr.RealTimeStrategy):

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

        self.zero_print = False
        self.strategy_id = utils.generate_random_id(4)
        self.grid = GridPosition(self.side,
                                 self.lst_symbols,
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

        self.self_execute_trade_recorder_not_active = True

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols

        ds.fdp_features = {
            "ema10": {"indicator": "ema", "id": "10", "window_size": 10}
        }

        ds.features = self.get_feature_from_fdp_features(ds.fdp_features)
        ds.interval = self.strategy_interval
        self.log("strategy: " + self.get_info())
        self.log("strategy features: " + str(ds.features))
        return ds

    def get_info(self):
        return "StrategyGridTradingGenericV2"

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
        df_price = current_state["prices"].copy()

        if not self.mutiple_strategy:
            del current_state["open_orders"]
            del current_state["open_positions"]
            del current_state["prices"]
            del current_state

        del self.df_price
        self.df_price = df_price
        lst_order_to_execute = []

        for symbol in self.lst_symbols:
            df_current_state = self.grid.set_current_orders_price_to_grid(symbol, df_current_states)
            if symbol in df_price['symbols'].tolist():
                self.grid.set_current_price(df_price.loc[df_price['symbols'] == symbol, 'values'].values[0])
                self.grid.update_unknown_status(symbol, df_current_state)
                self.grid.cross_check_with_current_state(symbol, df_current_state)
                self.grid.update_grid_side(symbol)
            lst_order_to_execute = self.grid.get_order_list(symbol)
            self.grid.set_to_pending_execute_order(symbol, lst_order_to_execute)
            lst_order_to_execute = self.grid.filter_lst_close_execute_order(symbol, lst_order_to_execute)


        if not self.zero_print:
            df_sorted = df_current_state.sort_values(by='price')
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
        for symbol in self.lst_symbols:
            self.grid.save_grid_scenario(symbol, path, cpt)

    def set_normalized_grid_price(self, lst_symbol_plc_endstp):
        return

    def get_info_msg_status(self):
        return ""

    def get_grid(self, cpt):
        # CEDE: MULTI SYMBOL TO BE IMPLEMENTED IF EVER ONE DAY.....
        for symbol in self.lst_symbols:
            return self.grid.get_grid(symbol, cpt)

    def record_status(self):
        if self.self_execute_trade_recorder_not_active:
            return

        if hasattr(self, 'df_price'):
            for symbol in self.lst_symbols:
                df_grid_values = self.grid.get_grid_for_record(symbol)
                self.execute_timer.set_grid_infos(df_grid_values, self.side)

    def update_executed_trade_status(self, lst_orders):
        if lst_orders is None:
            return []
        if len(lst_orders) > 0:
            for symbol in self.lst_symbols:
                self.grid.update_executed_trade_status(symbol, lst_orders)

    def get_strategy_id(self):
        return self.strategy_id

    def set_df_normalize_buying_size(self, df_normalized_buying_size):
        self.df_grid_buying_size = df_normalized_buying_size
        self.grid.set_grid_buying_size(self.df_grid_buying_size)
        for symbol in self.lst_symbols:
            self.grid.cross_check_buying_size(symbol, self.get_grid_buying_min_size(symbol))

    def set_df_buying_size(self, df_symbol_size, cash):
        if not isinstance(df_symbol_size, pd.DataFrame):
            return
        # cash = 10000 # CEDE GRID SCENARIO
        self.df_grid_buying_size = pd.concat([self.df_grid_buying_size, df_symbol_size])
        self.df_grid_buying_size['margin'] = None

        for symbol in self.lst_symbols:
            dol_per_grid = self.grid_margin / (self.nb_grid + 1)
            size = dol_per_grid / ((self.grid_high + self.grid_low )/2)
            size_high = dol_per_grid / self.grid_high
            size_low = dol_per_grid / self.grid_low
            size_high = utils.normalize_size(size_high,
                                             self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol,
                                                                          "sizeMultiplier"].values[0])
            size_low = utils.normalize_size(size_low,
                                            self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol,
                                                                         "sizeMultiplier"].values[0])
            if (self.get_grid_buying_min_size(symbol) <= size_high) \
                    and (self.get_grid_buying_min_size(symbol) <= size_low) \
                    and (dol_per_grid > 5) \
                    and (cash >= self.grid_margin):
                size = (size_high + size_low) / 2
                size = utils.normalize_size(size,
                                           self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol,
                                                                        "sizeMultiplier"].values[0])
                self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol, "strategy_id"] = self.strategy_id
                self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol, "buyingSize"] = size    # CEDE: Average size
                self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol, "margin"] = self.grid_margin
                self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol, "maxSizeToBuy"] = self.nb_grid
                msg = "**" + symbol + "**\n"
                msg += "**cash: " + str(round(cash, 2)) + "**\n"
                msg += "**grid_margin: " + str(round(self.grid_margin, 2)) + "**\n"
                msg += "**nb grid: " + str(self.nb_grid) + "**\n"
                msg += "**steps: " + str((self.grid_high - self.grid_low) / self.nb_grid) + "**\n"
                msg += "**amount buying > 5 usd: " + str(round(size * self.grid_low, 2)) + "**\n"
                msg += "**buying size: " + str(size) + " - $" + str(size * (self.grid_high + self.grid_low )/2) + "**\n"
                msg += "**min size: " + str(self.get_grid_buying_min_size(symbol)) + " - $" + str(self.get_grid_buying_min_size(symbol) * (self.grid_high + self.grid_low )/2) + "**\n"
                msg += "**strategy verified" + "**\n"
                self.log(msg, "GRID SETUP")
            else:
                msg = "**" + symbol + "**\n"
                msg += "**cash: " + str(round(cash, 2)) + "**\n"
                msg += "**grid_margin: " + str(round(self.grid_margin, 2)) + "**\n"
                msg += "**nb grid: " + str(self.nb_grid) + "**\n"
                msg += "**steps: " + str((self.grid_high - self.grid_low) / self.nb_grid) + "**\n"
                msg += "**amount buying > 5 usd: " + str(round(size * self.grid_low, 2)) + "**\n"
                msg += "**buying size: " + str(size) + " - $" + str(size * (self.grid_high + self.grid_low )/2) + "**\n"
                msg += "**min size: " + str(self.get_grid_buying_min_size(symbol)) + " - $" + str(self.get_grid_buying_min_size(symbol) * (self.grid_high + self.grid_low )/2) + "**\n"
                msg += "**strategy stopped : ERROR NOT ENOUGH $ FOR GRID - INCREASE MARGIN OR REDUCE GRID SIZE **\n"
                self.log(msg, "GRID SETUP FAILED")
                print(msg)
                print("GRID SETUP FAILED")
                print("set_df_buying_size")
                exit(2)
        return self.df_grid_buying_size


class GridPosition():
    def __init__(self, side, lst_symbols, grid_high, grid_low, nb_grid, percent_per_grid,
                 nb_position_limits, strategy_id, debug_mode=True, loggers=[]):
        self.grid_side = side
        side_mapping = {
            "long": ("open_long", "close_long"),
            "short": ("open_short", "close_short")
        }
        self.str_open, self.str_close = side_mapping.get(self.grid_side, (None, None))

        self.strategy_id = strategy_id
        self.grid_high = grid_high
        self.grid_low = grid_low
        self.nb_grid = nb_grid
        self.lst_symbols = lst_symbols
        self.str_lst_symbol = ' '.join(map(str, lst_symbols))
        self.percent_per_grid = percent_per_grid

        self.zero_print = debug_mode
        self.loggers = loggers
        self.trend = "FLAT"
        self.grid_position = []
        self.dct_status_info = {}
        self.previous_grid_position = []
        self.current_price = None
        self.diff_position = 0
        self.diff_close_position = 0
        self.diff_open_position = 0
        self.top_grid = False
        self.bottom_grid = False
        self.previous_top_grid = False
        self.previous_bottom_grid = False
        self.percent_per_grid = percent_per_grid
        self.steps = 0
        self.previous_grid_uniq_position = None
        self.grid_uniq_position = None
        self.max_position = 0
        self.min_position = 0
        self.on_edge = False
        self.grid_move = False
        self.msg = ""
        self.df_grid_string = ""
        self.abs = None
        self.closest = None

        self.max_grid_close_order = -1
        self.min_grid_close_order = -1
        self.nb_close_missing = -1
        self.buying_size = 0

        self.remaining_close_to_be_open = 0

        self.max_grid_open_order = -1
        self.min_grid_open_order = -1
        self.nb_open_missing = -1

        self.nb_open_selected_to_be_open = 0
        self.nb_close_selected_to_be_open = 0

        if self.percent_per_grid !=0:
            self.steps = self.grid_high * self.percent_per_grid / 100
            self.nb_grid = int((self.grid_high - self.grid_low) / self.steps)
        # Create a list with nb_grid split between high and low
        self.lst_grid_values = np.linspace(self.grid_high, self.grid_low, self.nb_grid + 1, endpoint=True).tolist()
        self.log("nb_grid: {}".format(self.nb_grid))
        self.log("grid steps: {}".format(self.steps))
        self.log("grid values: {}".format(self.lst_grid_values))

        self.columns = ["grid_id", "close_grid_id", "position", "lst_orderId", "nb_position",
                        "triggered_by", "nb_triggered_by", "bool_position_limits", "previous_side",
                        "side", "previous_status", "status", "changes", "cross_checked", "on_edge",
                        "unknown", "size", "dol_per_grid"]
        self.grid = {key: pd.DataFrame(columns=self.columns) for key in self.lst_symbols}
        for symbol in lst_symbols:
            self.grid[symbol]["position"] = self.lst_grid_values
            # print(self.grid[symbol]["position"].to_list())

            self.grid[symbol]["grid_id"] = np.arange(len(self.grid[symbol]))[::-1]
            if self.grid_side == "long":
                sequence = [-1] + list(np.arange(len(self.grid[symbol]) - 1, 0, -1))
            elif self.grid_side == "short":
                sequence = list(np.arange(len(self.grid[symbol]) - 2, -1, -1)) + [-1]
            self.grid[symbol]['close_grid_id'] = sequence
            del sequence

            self.grid[symbol]["lst_orderId"] = [[]] * len(self.grid[symbol])
            self.grid[symbol]["nb_position"] = 0
            self.grid[symbol]["triggered_by"] = [[]] * len(self.grid[symbol])
            self.grid[symbol]["nb_triggered_by"] = 0
            self.grid[symbol]["bool_position_limits"] = True

            self.grid[symbol]["previous_side"] = False
            self.grid[symbol]["side"] = ""
            self.grid[symbol]["changes"] = True
            self.grid[symbol]["previous_status"] = "empty"
            self.grid[symbol]["status"] = "empty"
            self.grid[symbol]["cross_checked"] = False
            self.grid[symbol]["on_edge"] = False
            self.grid[symbol]["unknown"] = False

        self.nb_open_positions = 0

        self.total_position_opened = 0
        self.total_position_closed = 0

        self.nb_position_limits = nb_position_limits

    def log(self, msg, header="", attachments=[]):
        if self.zero_print:
            return
        for iter_logger in self.loggers:
            iter_logger.log(msg, header=header, author=type(self).__name__, attachments=attachments)

    def get_grid_nb_grid(self):
        return self.nb_grid

    def update_grid_side(self, symbol):
        df = self.grid[symbol]
        position = self.current_price

        self.on_edge = False
        df['on_edge'] = False
        if (df['position'] == position).any():
            self.on_edge = True
            df.loc[df['position'] == position, 'on_edge'] = True
            self.log('PRICE ON GRID EDGE - CROSSING OR NOT CROSSING')
            # Calculate delta
            delta = abs((df.at[0, 'position'] - df.at[1, 'position']) / 2)

            # Retrieve values to avoid repeated lookups
            cross_checked = df.loc[df['position'] == position, 'cross_checked'].values[0]
            side_value = df.loc[df['position'] == position, 'side'].values[0]
            # Define position adjustment mapping
            adjustment = {
                "long": {
                    (False, self.str_open): -delta,
                    (False, self.str_close): delta,
                    (True, self.str_open): delta,
                    (True, self.str_close): -delta
                },
                "short": {
                    (False, self.str_open): delta,
                    (False, self.str_close): -delta,
                    (True, self.str_open): -delta,
                    (True, self.str_close): delta
                }
            }
            # Update position based on the grid_side and conditions
            if self.grid_side in adjustment:
                position += adjustment[self.grid_side][(cross_checked, side_value)]

        df['previous_side'] = df['side']
        side_map = {
            "long": {"greater": self.str_close, "less": self.str_open},
            "short": {"greater": self.str_open, "less": self.str_close}
        }
        df.loc[df['position'] > position, 'side'] = side_map[self.grid_side]['greater']
        df.loc[df['position'] < position, 'side'] = side_map[self.grid_side]['less']

        # Compare if column1 and column2 are the same
        df['changes'] = df['previous_side'] != df['side']

        del position
        del symbol
        del df

    # Function to find the index of the closest value in an array
    def find_closest(self, value, array):
        if self.abs is not None:
            del self.abs
        if self.closest is not None:
            del self.closest
        self.abs = array - value
        self.abs = np.abs(self.abs)
        self.closest = self.abs.argmin()
        # closest = np.abs(array - value).argmin()
        # return closest

    def set_current_orders_price_to_grid(self, symbol, df_current_state):
        if not df_current_state.empty:
            df_current_state = df_current_state[df_current_state['symbol'] == symbol]
            df_grid = self.grid[symbol]
            lst_price = []
            for price in df_current_state['price']:
                self.find_closest(price, df_grid['position'])
                closest_index = self.closest
                closest_value = df_grid.at[closest_index, 'position']
                lst_price.append(closest_value)
            df_current_state['price'] = lst_price
            del lst_price
            # Check if all elements in df_current_state['price'] are in df_grid['position']
            self.s_bool = df_current_state['price'].isin(df_grid['position'])
            self.all_prices_in_grid = self.s_bool.all()
            if not self.zero_print and not self.all_prices_in_grid:
                    # Print the elements that are different
                    different_prices = df_current_state.loc[~df_current_state['price'].isin(df_grid['position']), 'price']
                    self.log("################ WARNING PRICE DIFF WITH ORDER AND GRID ###############################")
                    self.log("Elements in df_current_state['price'] that are not in df_grid['position']:")
                    self.log(different_prices)
            del self.s_bool
            del self.all_prices_in_grid
            del df_grid
        return df_current_state

    def confirm_orderId(self, symbol, grid_id_engaged, list_orderId):
        df_grid = self.grid[symbol]
        lst_orderId_from_grid = df_grid.loc[df_grid['grid_id'] == grid_id_engaged, "lst_orderId"].values[0]
        return all(order_id in list_orderId for order_id in lst_orderId_from_grid)

    def set_current_price(self, price):
        self.current_price = price

    def get_max_price_in_grid(self, symbol):
        self.max_position = max(self.grid[symbol]["position"].to_list())
        return self.max_position

    def update_unknown_status(self, symbol, df_current_state_all):
        df_grid = self.grid[symbol]
        if df_grid['unknown'].any():
            df_current_state = df_current_state_all[df_current_state_all["symbol"] == symbol]
            df_current_state = df_current_state.loc[df_current_state['strategyId'] == self.strategy_id]
            df_filtered = df_grid[df_grid['unknown']]
            lst_grid_id_filtered = df_filtered["grid_id"].to_list()
            lst_current_state_id = df_current_state["gridId"].to_list()
            for grid_id in lst_grid_id_filtered:
                if grid_id in lst_current_state_id:
                    df_filtered_current_state = df_current_state[df_current_state['gridId'] == grid_id]
                    lst_order_id = df_filtered_current_state["orderId"].to_list()
                    side = df_filtered_current_state["side"].iat[0]
                    index = df_grid.index[df_grid["grid_id"] == grid_id].tolist()
                    idx = index[0]
                    df_grid.at[idx, 'status'] = "engaged"
                    df_grid.at[idx, 'lst_orderId'] = lst_order_id
                    df_grid.at[idx, 'side'] = side
                else:
                    index = df_grid.index[df_grid["grid_id"] == grid_id].tolist()
                    idx = index[0]
                    df_grid.at[idx, 'status'] = "on_hold"
                    df_grid.at[idx, 'lst_orderId'] = []
            df_grid["unknown"] = False

    def cross_check_with_current_state(self, symbol, df_current_state_all):
        price = self.current_price
        df_grid = self.grid[symbol]
        df_grid['cross_checked'] = False
        df_grid['previous_status'] = df_grid['status']
        df_current_state = df_current_state_all[df_current_state_all["symbol"] == symbol]
        df_current_state = df_current_state.loc[df_current_state['strategyId'] == self.strategy_id]
        lst_current_state_id = df_current_state["gridId"].to_list()
        list_orderId = df_current_state_all["orderId"].to_list()

        condition_engaged = df_grid['status'] == 'engaged'

        df_grid_engaged = df_grid[condition_engaged]
        lst_engaged = df_grid_engaged["grid_id"].to_list()

        for grid_id_engaged in lst_engaged:
            if grid_id_engaged in lst_current_state_id \
                    and self.confirm_orderId(symbol, grid_id_engaged, list_orderId):
                df_grid.loc[df_grid['grid_id'] == grid_id_engaged, "status"] = "engaged"
            else:
                if df_grid.loc[df_grid["grid_id"] == grid_id_engaged, 'side'].values[0] == self.str_open:
                    triggered_grid_id = df_grid.loc[df_grid["grid_id"] == grid_id_engaged, 'close_grid_id'].values[0]
                    triggered_price = df_grid.loc[df_grid["grid_id"] == triggered_grid_id, 'position'].values[0]  # CEDE -1 case to be added
                    if self.grid_side == "long":
                        status = "triggered" if triggered_price > price else "triggered_below"
                    elif self.grid_side == "short":
                        status = "triggered" if triggered_price < price else "triggered_below"
                    df_grid.loc[df_grid['grid_id'] == grid_id_engaged, "status"] = status
                    index = df_grid.index[df_grid["grid_id"] == grid_id_engaged].tolist()
                    idx = index[0]
                    df_grid.at[idx, 'lst_orderId'] = []
                    df_grid.at[idx, 'triggered_by'] = []
                    df_grid.at[idx, 'nb_position'] = 0
                    self.total_position_opened += 1
                elif df_grid.loc[df_grid["grid_id"] == grid_id_engaged, 'side'].values[0] == self.str_close:
                    df_grid.loc[df_grid['grid_id'] == grid_id_engaged, "status"] = "on_hold"
                    self.total_position_closed += len(df_grid.loc[df_grid['grid_id'] == grid_id_engaged, "lst_orderId"].values[0])
                    index = df_grid.index[df_grid["grid_id"] == grid_id_engaged].tolist()
                    idx = index[0]
                    df_grid.at[idx, 'lst_orderId'] = []
                    df_grid.at[idx, 'triggered_by'] = []
                    self.total_position_closed += df_grid.loc[df_grid["grid_id"] == grid_id_engaged, "nb_position"].values[0]
                    df_grid.at[idx, 'nb_position'] = 0

        del lst_engaged
        del condition_engaged
        del df_grid_engaged

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
        df_grid = self.grid[symbol]

        condition_changes = df_grid['changes']
        condition_on_hold = df_grid['status'] == 'on_hold'
        condition_engaged = df_grid['status'] == 'engaged'
        condition_empty = df_grid['status'] == 'empty'
        condition_triggered = df_grid['status'] == 'triggered'
        condition_triggered_below = df_grid['status'] == 'triggered_below'
        condition_open_long = df_grid['side'] == self.str_open
        condition_close_long = df_grid['side'] == self.str_close
        condition_open_long_positions_limit = df_grid['bool_position_limits'] == False

        ####################################################################################
        df_grid_short_changes = df_grid[condition_changes & condition_close_long]
        lst_open_long_changes = df_grid_short_changes["grid_id"].to_list()

        df_grid_close_long_triggered = df_grid[condition_triggered & condition_close_long]
        lst_open_long_triggered = df_grid_close_long_triggered["grid_id"].to_list()

        open_long_triggered = list(set(lst_open_long_changes) & set(lst_open_long_triggered))
        matching_rows = df_grid[df_grid['grid_id'].isin(open_long_triggered)]
        close_grid_id_list = matching_rows['close_grid_id'].tolist()
        df_grid.loc[df_grid['grid_id'].isin(open_long_triggered), 'changes'] = False
        df_grid.loc[df_grid['grid_id'].isin(open_long_triggered), 'status'] = "on_hold"
        df_grid.loc[df_grid['grid_id'].isin(close_grid_id_list), 'status'] = "pending"

        ####################################################################################
        df_grid_empty = df_grid[condition_empty & condition_open_long]
        lst_open_long_empty = df_grid_empty["grid_id"].to_list()
        df_grid_on_hold = df_grid[condition_on_hold & condition_open_long]
        lst_open_long_on_hold = df_grid_on_hold["grid_id"].to_list()
        df_grid_triggered = df_grid[condition_triggered & condition_open_long]
        lst_open_long_triggered = df_grid_triggered["grid_id"].to_list()
        df_grid_triggered_below = df_grid[condition_triggered_below & condition_open_long]
        lst_open_long_triggered_below = df_grid_triggered_below["grid_id"].to_list()

        lst_open_long = lst_open_long_empty + lst_open_long_on_hold + lst_open_long_triggered + lst_open_long_triggered_below
        lst_open_long = list(set(lst_open_long))

        filtered_triggered_df = df_grid[df_grid["grid_id"].isin(lst_open_long_triggered)]
        lst_close_long = filtered_triggered_df["close_grid_id"].tolist()

        # CEDE Get fake triggered position (price position as new legit position)
        if len(lst_open_long_triggered_below) > 0:
            df_grid_filtered_open_long = df_grid[condition_open_long]
            fake_position = df_grid_filtered_open_long["grid_id"].max() if self.grid_side == "long" else df_grid_filtered_open_long["grid_id"].min()
            for n in range(0, len(lst_open_long_triggered_below), 1):
                long_fake_triggered = fake_position + n if self.grid_side == "long" else fake_position - n
                if long_fake_triggered != -1:
                    open_long_triggered.append(long_fake_triggered)
                    try:
                        close_fake_grid_id = df_grid.loc[df_grid['grid_id'] == long_fake_triggered, 'close_grid_id'].values[0]
                    except:
                        print("toto")
                    close_grid_id_list.append(close_fake_grid_id)
                    df_grid.loc[df_grid['grid_id'] == long_fake_triggered, 'changes'] = False
                    # Check and update the status for long_fake_triggered
                    if not df_grid.loc[df_grid['grid_id'] == long_fake_triggered, 'status'].empty:
                        status_long = df_grid.loc[df_grid['grid_id'] == long_fake_triggered, 'status'].values[0]
                        if status_long != "pending" and status_long != "engaged":
                            df_grid.loc[df_grid['grid_id'] == long_fake_triggered, 'status'] = "on_hold"

                    # Check and update the status for close_fake_grid_id
                    if not df_grid.loc[df_grid['grid_id'] == close_fake_grid_id, 'status'].empty:
                        status_close = df_grid.loc[df_grid['grid_id'] == close_fake_grid_id, 'status'].values[0]
                        if status_close != "pending" and status_close != "engaged":
                            df_grid.loc[df_grid['grid_id'] == close_fake_grid_id, 'status'] = "pending"

        close_grid_id_list = close_grid_id_list + lst_close_long
        open_long_triggered = open_long_triggered + lst_open_long_triggered

        # Get the grid ID to remove based on the grid side
        grid_id_to_remove = df_grid['grid_id'].max() if self.grid_side == "long" else df_grid['grid_id'].min()
        # Remove the grid ID from the list if it exists
        if grid_id_to_remove in lst_open_long:
            lst_open_long.remove(grid_id_to_remove)

        value_to_remove = -1
        if value_to_remove in lst_close_long:
            lst_close_long.remove(value_to_remove)  # CEDE This should never happen

        df_grid_positions_not_in_limits = df_grid[condition_open_long & condition_open_long_positions_limit]
        if len(df_grid_positions_not_in_limits) > 0:
            lst_open_long_positions_not_in_limits = df_grid_positions_not_in_limits["grid_id"].to_list()
            # Filter out items
            lst_open_long = [item for item in lst_open_long if item not in lst_open_long_positions_not_in_limits]

        lst_open_long.sort(reverse=True)
        lst_open_long_linked = [{'triggered_by': None, 'position': item1} for item1 in lst_open_long]

        open_long_triggered.sort(reverse=True)
        close_grid_id_list.sort(reverse=True)
        lst_close_long_linked = [{'triggered_by': item1, 'position': item2} for item1, item2 in zip(open_long_triggered, close_grid_id_list)]

        lst_linked = lst_open_long_linked + lst_close_long_linked

        lst_order_grid_id = lst_open_long + close_grid_id_list
        lst_order = []
        for grid_id, linked_position in zip(lst_order_grid_id, lst_linked):
            if grid_id != -1:
                order_to_execute = {}
                order_to_execute["strategy_id"] = self.strategy_id
                order_to_execute["symbol"] = symbol
                order_to_execute["linked_position"] = linked_position

                if df_grid.loc[df_grid["grid_id"] == grid_id, 'side'].values[0] == "open_long":
                    self.clear_orderId(symbol, grid_id)
                    order_to_execute["type"] = "OPEN_LONG_ORDER"
                elif df_grid.loc[df_grid["grid_id"] == grid_id, 'side'].values[0] == "close_long":
                    order_to_execute["type"] = "CLOSE_LONG_ORDER"
                elif df_grid.loc[df_grid["grid_id"] == grid_id, 'side'].values[0] == "open_short":
                    self.clear_orderId(symbol, grid_id)
                    order_to_execute["type"] = "OPEN_SHORT_ORDER"
                elif df_grid.loc[df_grid["grid_id"] == grid_id, 'side'].values[0] == "close_short":
                    order_to_execute["type"] = "CLOSE_SHORT_ORDER"
                if "type" in order_to_execute:
                    order_to_execute["price"] = df_grid.loc[df_grid["grid_id"] == grid_id, 'position'].values[0]
                    order_to_execute["grid_id"] = grid_id
                    order_to_execute["gross_size"] = df_grid.loc[df_grid["grid_id"] == grid_id, 'size'].values[0]
                    order_to_execute["trade_status"] = "pending"
                    df_grid.loc[df_grid["grid_id"] == grid_id, 'status'] = 'pending'
                    lst_order.append(order_to_execute)
                del order_to_execute

        sorting_order = ['OPEN_LONG_ORDER', 'OPEN_SHORT_ORDER', 'CLOSE_LONG_ORDER', 'CLOSE_SHORT_ORDER']
        sorted_list = sorted(lst_order, key=lambda x: sorting_order.index(x['type']))

        del df_grid
        del lst_order_grid_id
        del sorting_order
        del lst_order

        return sorted_list

    def update_nb_position_limits(self, df):
        df['bool_position_limits'] = df['nb_position'].apply(lambda x: x < self.nb_position_limits)

        exploded_df = df.explode('triggered_by')
        trigger_counts = exploded_df['triggered_by'].value_counts()
        df['nb_triggered_by'] = df['grid_id'].map(trigger_counts).fillna(0).astype(int)
        df['bool_position_limits'] = df['nb_triggered_by'] <= self.nb_position_limits

    def update_executed_trade_status(self, symbol, lst_orders):
        df_grid = self.grid[symbol]
        for order in lst_orders:
            if order["strategy_id"] == self.strategy_id:
                grid_id = order["grid_id"]
                if order["trade_status"] == "SUCCESS":
                    df_grid.loc[df_grid["grid_id"] == grid_id, "status"] = "engaged"
                    current_list = df_grid.loc[df_grid["grid_id"] == grid_id, "lst_orderId"].values[0]
                    if not isinstance(current_list, list):
                        current_list = []
                    # current_list.append(order["orderId"])
                    orderId_current_list = current_list + [order["orderId"]]
                    index = df_grid.index[df_grid["grid_id"] == grid_id].tolist()
                    idx = index[0]
                    df_grid.at[idx, 'lst_orderId'] = orderId_current_list
                    del current_list
                    del orderId_current_list
                    df_grid.loc[df_grid["grid_id"] == grid_id, "nb_position"] = df_grid.loc[df_grid["grid_id"] == grid_id, "nb_position"].values[0] + 1
                    if order["type"] in ["CLOSE_SHORT_ORDER", "CLOSE_LONG_ORDER"]:
                        current_list = df_grid.loc[df_grid["grid_id"] == grid_id, "triggered_by"].values[0]
                        if not isinstance(current_list, list):
                            current_list = []
                        triggered_by_pos = order["linked_position"]["triggered_by"]
                        current_list = current_list + [triggered_by_pos]
                        index = df_grid.index[df_grid["grid_id"] == grid_id].tolist()
                        idx = index[0]
                        df_grid.at[idx, 'triggered_by'] = current_list
                        del current_list
                elif order["trade_status"] == "FAILED" \
                        or order["trade_status"] == "MISSING":
                    if df_grid.loc[df_grid["grid_id"] == grid_id, "status"].values[0] != "engaged":
                        df_grid.loc[df_grid["grid_id"] == grid_id, "status"] = "on_hold"
                elif order["trade_status"] == "UNKNOWN":
                    df_grid.loc[df_grid["grid_id"] == grid_id, "unknown"] = True
        self.update_nb_position_limits(df_grid)

    def set_to_pending_execute_order(self, symbol, lst_order_to_execute):
        """
        df = self.grid[symbol]
        for placed_order in lst_order_to_execute:
            grid_id = placed_order["grid_id"]
            mask = df['grid_id'] == grid_id
            test_mask = mask.any()
            if test_mask:
                del test_mask
                id = mask.idxmax()
                df.at[id , 'status'] = 'pending'
                df.at[id, 'orderId'] = ''
            del mask
            del id
        del df
        """
        return

    def clear_orderId(self, symbol, grid_id):
        df = self.grid[symbol]
        # df.loc[df['grid_id'] == grid_id, 'lst_orderId'] = []
        df.loc[df['grid_id'] == grid_id, 'lst_orderId'] = df.loc[df['grid_id'] == grid_id, 'lst_orderId'].apply(lambda x: [])
        del df

    def print_grid(self):
        # if not self.zero_print:
        for symbol in self.lst_symbols:
            df_grid = self.grid[symbol]
            self.log("\n" + df_grid.to_string(index=False))
            del df_grid

    def filter_lst_close_execute_order(self, symbol, lst_order_to_execute):
        df = self.grid[symbol]

        condition_engaged = df['status'] == 'engaged'
        # condition_open_long = df['side'] == 'open_long'
        condition_close_long = df['side'] == 'close_long'

        df_filtered_previous_close_long = df[condition_close_long & condition_engaged]
        self.nb_open_positions = df["nb_position"].sum()
        count_open_orders = max(len(df_filtered_previous_close_long), self.nb_open_positions)

        open_orders = [order for order in lst_order_to_execute if "open_" in order["type"]]
        count_open_orders_in_lst = len(open_orders)
        count_open_orders += count_open_orders_in_lst

        if count_open_orders <= self.nb_grid:
            return lst_order_to_execute
        else:
            nb_open_to_filter = count_open_orders - self.nb_grid
            sorted_open_orders = sorted(open_orders, key=lambda x: x["grid_id"])
            if nb_open_to_filter < count_open_orders_in_lst:
                nb_open_to_filter = len(open_orders)
            orders_to_drop = sorted_open_orders[:nb_open_to_filter]
            lst_order_to_execute_filtered = [order for order in lst_order_to_execute if order not in orders_to_drop]
            return lst_order_to_execute_filtered

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
        df = self.grid[symbol]
        row_index = df.index[df['grid_id'] == grid_id].tolist()  # Get the index of rows where 'grid_id' equals grid_id
        for index in row_index:
            df.at[index, 'status'] = 'on_hold'  # Update the 'status' column for each matching row
        del df
        del row_index

    def set_grid_positions_to_on_hold(self, lst_pending, lst_filtered):
        resulted_lst = [element for element in lst_pending if element not in lst_filtered]
        for order in resulted_lst:
            self.set_on_hold_from_grid_id(order["symbol"], order["grid_id"])
        del resulted_lst

    def normalize_grid_price(self, symbol, pricePlace, priceEndStep, sizeMultiplier):
        return

    def set_grid_buying_size(self, df_grid_buying_size):
        self.df_grid_buying_size = df_grid_buying_size

        for symbol in self.lst_symbols:
            # Calculate dol_per_grid for the symbol, dividing grid_margin by the number of rows (cells) in grid[symbol]
            grid_margin = self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol, "margin"].values[0]
            pricePlace = self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol, "pricePlace"].values[0]
            priceEndStep = self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol, "priceEndStep"].values[0]
            self.grid[symbol]['position'] = self.grid[symbol]['position'].apply(lambda x: utils.normalize_price(x, pricePlace, priceEndStep))

            num_cells = len(self.grid[symbol])
            self.grid[symbol]["dol_per_grid"] = grid_margin / num_cells

            # Calculate size by dividing dol_per_grid by the position column
            self.grid[symbol]["size"] = self.grid[symbol]["dol_per_grid"] / self.grid[symbol]["position"]

            # Get the sizeMultiplier for the symbol
            sizeMultiplier = self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol, "sizeMultiplier"].values[0]

            # Apply the normalize_size function to each value in the 'size' column
            self.grid[symbol]["size"] = self.grid[symbol]["size"].apply(
                lambda size: utils.normalize_size(size, sizeMultiplier))

            # Fill NaN values in case there are any undefined cells
            self.grid[symbol]["size"].fillna(0, inplace=True)
            self.grid[symbol]["dol_per_grid"].fillna(0, inplace=True)
            self.grid[symbol]["dol_per_grid_verif"] = self.grid[symbol]["size"] * self.grid[symbol]["position"]

            # Check if any value in dol_per_grid_verif is less than 5
            if (self.grid[symbol]["dol_per_grid_verif"] < 5).any():
                print(f"Exiting because dol_per_grid_verif for {symbol} has a value less than $5.")
                exit(33)
            self.grid[symbol] = self.grid[symbol].drop(columns=["dol_per_grid_verif"])
        return

    def cross_check_buying_size(self, symbol, buying_min_size):
        if (self.grid[symbol]["size"] < buying_min_size).any():
            print(f"Exiting because buying_min_size for {symbol} has a value less than buying_min_size {buying_min_size}.")
            exit(44)
        return

    def get_grid_info(self, symbol):
        self.dct_status_info = {}
        return self.dct_status_info

    def dct_change_status(self):
        if self.grid_move:
           self.grid_move = False
           return True
        else:
            return False

    def dct_status_info_to_txt(self, symbol):
        del self.msg
        self.msg = ""
        return self.msg.upper()

    def get_grid_as_str(self, symbol):
        del self.df_grid_string
        df_grid = self.grid[symbol]
        self.df_grid_string = df_grid.to_string(index=False)
        del df_grid
        return self.df_grid_string

    def get_grid(self, symbol, cycle):
        df = self.grid[symbol].copy()
        if isinstance(cycle, int):
            df['cycle'] = cycle

            column_to_move = 'cycle'
            first_column = df.pop(column_to_move)
            df.insert(0, column_to_move, first_column)
        return df

    def get_grid_for_record(self, symbol):
        df = self.grid[symbol].copy()
        df["values"] = df["side"] + "_" + df["status"]
        df.set_index('position', inplace=True)
        df = df[["values"]]
        return df

    def save_grid_scenario(self, symbol, path, cpt):
        if cpt >= 30:
            cpt += 1
            df = self.grid[symbol]
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