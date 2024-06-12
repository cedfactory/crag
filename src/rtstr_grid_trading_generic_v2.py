from . import rtdp, rtstr, rtctrl
import math
import pandas as pd
import numpy as np

from . import utils
from src import logger

class StrategyGridTradingGenericV2(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        if False:
            self.side = "long"
        else:
            self.side = "short"

        self.zero_print = False
        self.grid = GridPosition(self.side,
                                 self.lst_symbols,
                                 self.grid_high, self.grid_low, self.nb_grid, self.percent_per_grid,
                                 self.nb_position_limits,
                                 self.zero_print,
                                 self.loggers)
        if self.percent_per_grid != 0:
            self.nb_grid = self.grid.get_grid_nb_grid()
        self.df_grid_buying_size = pd.DataFrame()
        self.execute_timer = None
        self.df_price = None
        self.mutiple_strategy = False

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

    def condition_for_opening_long_position(self, symbol):
        return False

    def condition_for_opening_short_position(self, symbol):
        return False

    def condition_for_closing_long_position(self, symbol):
        return False

    def condition_for_closing_short_position(self, symbol):
        return False

    def sort_list_symbols(self, lst_symbols):
        self.log("symbol list: ", lst_symbols)
        return lst_symbols

    def need_broker_current_state(self):
        return True

    def set_multiple_strategy(self):
        self.mutiple_strategy = True

    def set_execute_time_recorder(self, execute_timer):
        if self.execute_timer is not None:
            del self.execute_timer
        self.execute_timer = execute_timer
        self.iter_set_broker_current_state = 0

    def set_df_grid_buying_size(self, df_grid_buying_size):
        self.df_grid_buying_size = df_grid_buying_size
        del df_grid_buying_size

    def get_str_current_state_filter(self):
        if self.side == 'long':
            return "short"
        elif self.side == 'short':
            return "long"

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
        df_open_positions = current_state["open_positions"].copy()
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
            # self.execute_timer.set_start_time("rtstr", "set_broker_current_state", "set_current_orders_price_to_grid", self.iter_set_broker_current_state)
            df_current_state = self.grid.set_current_orders_price_to_grid(symbol, df_current_states)
            # self.execute_timer.set_end_time("rtstr", "set_broker_current_state", "set_current_orders_price_to_grid", self.iter_set_broker_current_state)

            # self.execute_timer.set_start_time("rtstr", "set_broker_current_state", "get_grid_buying_size", self.iter_set_broker_current_state)
            self.grid.set_buying_size(self.get_grid_buying_size(symbol))
            # self.execute_timer.set_end_time("rtstr", "set_broker_current_state", "get_grid_buying_size", self.iter_set_broker_current_state)

            # self.execute_timer.set_start_time("rtstr", "set_broker_current_state", "update_nb_open_positions", self.iter_set_broker_current_state)
            self.grid.update_nb_open_positions(symbol, df_open_positions)
            # self.execute_timer.set_end_time("rtstr", "set_broker_current_state", "update_nb_open_positions", self.iter_set_broker_current_state)

            if symbol in df_price['symbols'].tolist():
                self.grid.set_current_price(df_price.loc[df_price['symbols'] == symbol, 'values'].values[0])
                self.grid.update_unknown_status(symbol, df_current_state)

                # self.execute_timer.set_start_time("rtstr", "set_broker_current_state", "cross_check_with_current_state", self.iter_set_broker_current_state)
                self.grid.cross_check_with_current_state(symbol, df_current_state)
                # self.execute_timer.set_end_time("rtstr", "set_broker_current_state", "cross_check_with_current_state", self.iter_set_broker_current_state)

                # self.execute_timer.set_start_time("rtstr", "set_broker_current_state", "update_grid_side", self.iter_set_broker_current_state)
                self.grid.update_grid_side(symbol)
                # self.execute_timer.set_end_time("rtstr", "set_broker_current_state", "update_grid_side", self.iter_set_broker_current_state)
            # else:
                # self.execute_timer.set_time_to_zero("rtstr", "set_broker_current_state", "cross_check_with_current_state", self.iter_set_broker_current_state)
                # self.execute_timer.set_time_to_zero("rtstr", "set_broker_current_state", "update_grid_side", self.iter_set_broker_current_state)

            # self.execute_timer.set_start_time("rtstr", "set_broker_current_state", "get_order_list", self.iter_set_broker_current_state)
            lst_order_to_execute = self.grid.get_order_list(symbol)
            # self.execute_timer.set_end_time("rtstr", "set_broker_current_state", "get_order_list", self.iter_set_broker_current_state)

            # self.execute_timer.set_start_time("rtstr", "set_broker_current_state", "set_to_pending_execute_order", self.iter_set_broker_current_state)
            self.grid.set_to_pending_execute_order(symbol, lst_order_to_execute)
            # self.execute_timer.set_end_time("rtstr", "set_broker_current_state", "set_to_pending_execute_order", self.iter_set_broker_current_state)

            # self.execute_timer.set_start_time("rtstr", "set_broker_current_state", "filter_lst_close_execute_order", self.iter_set_broker_current_state)
            lst_order_to_execute = self.grid.filter_lst_close_execute_order(symbol, lst_order_to_execute)
            # self.execute_timer.set_end_time("rtstr", "set_broker_current_state", "filter_lst_close_execute_order", self.iter_set_broker_current_state)

        if not self.zero_print:
            df_sorted = df_current_state.sort_values(by='price')
            self.log("#############################################################################################")
            self.log("current_state: \n" + df_sorted.to_string(index=False))
            del df_sorted
            self.log("open_positions: \n" + df_open_positions.to_string(index=False))
            self.log("price: \n" + df_price.to_string(index=False))
            lst_order_to_print = []
            for order in lst_order_to_execute:
                lst_order_to_print.append((order["grid_id"], order["price"], order["type"]))
            self.log("order list: \n" + str(lst_order_to_print))
            del lst_order_to_print

        del df_current_states
        del df_open_positions
        del df_price

        self.iter_set_broker_current_state += 1

        return lst_order_to_execute

    def print_grid(self):
        self.grid.print_grid()

    def save_grid_scenario(self, path, cpt):
        for symbol in self.lst_symbols:
            self.grid.save_grid_scenario(symbol, path, cpt)

    def set_normalized_grid_price(self, lst_symbol_plc_endstp):
        for price_plc in lst_symbol_plc_endstp:
            self.grid.normalize_grid_price(price_plc['symbol'], price_plc['pricePlace'], price_plc['priceEndStep'])
        del lst_symbol_plc_endstp

    def activate_grid(self, current_state):
        if not current_state:
            return []
        self.df_prices = current_state["prices"]
        lst_buying_market_order = []
        for symbol in self.lst_symbols:
            lst_buying_market_order = []
            self.buying_size = self.get_grid_buying_size(symbol)
            if symbol in self.df_prices['symbols'].tolist():
                price = self.df_prices.loc[self.df_prices['symbols'] == symbol, 'values'].values[0]
                order = self.grid.get_buying_market_order(symbol, self.buying_size, price)
                lst_buying_market_order.append(order)
        del lst_buying_market_order
        del current_state
        if self.lst_symbols > 0:
            del symbol
            del price
            del order
        return lst_buying_market_order

    def get_info_msg_status(self):
        # CEDE: MULTI SYMBOL TO BE IMPLEMENTED IF EVER ONE DAY.....
        # self.execute_timer.set_start_time("rtstr", "get_info_msg_status", "record_grid_status", self.iter_set_broker_current_state)
        # self.record_grid_status()
        # self.execute_timer.set_end_time("rtstr", "get_info_msg_status", "record_grid_status", self.iter_set_broker_current_state)
        return ""

    def get_grid(self, cpt):
        # CEDE: MULTI SYMBOL TO BE IMPLEMENTED IF EVER ONE DAY.....
        for symbol in self.lst_symbols:
            return self.grid.get_grid(symbol, cpt)

    def record_grid_status(self):
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

class GridPosition():
    def __init__(self, side, lst_symbols, grid_high, grid_low, nb_grid, percent_per_grid, nb_position_limits, debug_mode=True, loggers=[]):
        self.grid_side = side
        if self.grid_side == "long":
            self.str_open = "open_long"
            self.str_close = "close_long"
        elif self.grid_side == "short":
            self.str_open = "open_short"
            self.str_close = "close_short"

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

        self.previous_nb_open_positions = 0
        self.nb_open_positions = 0
        self.nb_open_positions_triggered = 0

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

        self.columns = ["grid_id", "close_grid_id", "position", "lst_orderId", "nb_position", "triggered_by", "nb_triggered_by", "bool_position_limits", "previous_side", "side", "previous_status", "status", "changes", "cross_checked", "on_edge", "unknown"]
        self.grid = {key: pd.DataFrame(columns=self.columns) for key in self.lst_symbols}
        for symbol in lst_symbols:
            self.grid[symbol]["position"] = self.lst_grid_values
            print(self.grid[symbol]["position"].to_list())

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

        self.lst_limit_order_missing = []
        self.nb_open_positions = 0
        self.nb_close_positions = 0
        self.diff_nb_open_positions = 0
        self.diff_nb_close_positions = 0

        self.total_position_opened = 0
        self.total_position_closed = 0

        self.nb_position_limits = nb_position_limits

    def log(self, msg, header="", attachments=[]):
        if self.zero_print:
            return
        for iter_logger in self.loggers:
            iter_logger.log(msg, header=header, author=type(self).__name__, attachments=attachments)

    def update_pending_status_from_current_state(self, symbol, df_current_state):
        # Update grid status from 'pending' status to 'engaged'
        # from previous cycle request to open limit order
        self.lst_limit_order_missing = []
        df = self.grid[symbol]
        # Define a condition
        condition_pending = df['status'] == 'pending'
        # Use boolean indexing to filter the DataFrame
        df_filtered = df[condition_pending]
        lst_grid_id = df_filtered['grid_id'].tolist()
        lst_grid_id.extend(df_filtered['grid_id'].tolist())
        lst_grid_id = list(set(lst_grid_id))
        for grid_id in lst_grid_id:
            side = df.loc[df['grid_id'] == grid_id, "side"].values[0]
            if grid_id in df_current_state["gridId"].tolist():
                if side == df_current_state.loc[df_current_state["gridId"] == grid_id, 'side'].values[0]:
                    df.loc[df['grid_id'] == grid_id, 'status'] = 'engaged'
                    df.loc[df['grid_id'] == grid_id, 'orderId'] = df_current_state.loc[df_current_state["gridId"] == grid_id, 'orderId'].values[0]
                else:
                    # CEDE: This should not be triggered
                    # CEDE UPDATE : Maybe Not
                    self.log("GRID ERROR - open_long vs open_short - grid id: {}".format(grid_id))
            else:
                # CEDE: Executed when high volatility is triggering an limit order just after being raised
                #       and before being recorded by the strategy in the grid process structure
                self.log("GRID ERROR - limit order failed - grid_id missing: " + str(grid_id) + ' side: ' + side)
            del side
        del df
        del condition_pending
        del df_filtered
        del lst_grid_id
        del df_current_state

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
            else:
                raise ValueError(f"Unexpected grid_side: {self.grid_side}")

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
        if len(df_current_state) > 0:
            df_current_state = df_current_state[df_current_state['symbol'] == symbol]
            df_grid = self.grid[symbol]
            # Replace values in df_current_state['price'] with the closest values from df_grid['position']
            # lst_price = [df_grid['position'].iloc[self.find_closest(price, df_grid['position'])] for price in df_current_state['price']]
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
                    exit(0)
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

    def update_unknown_status(self, symbol, df_current_state_all):
        df_grid = self.grid[symbol]
        if df_grid['unknown'].any():
            df_current_state = df_current_state_all[df_current_state_all["symbol"] == symbol]
            if self.grid_side == "long":
                df_current_state = df_current_state[~df_current_state['side'].str.contains("_short")]
            elif self.grid_side == "short":
                df_current_state = df_current_state[~df_current_state['side'].str.contains("_long")]
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
        # Define the substring to filter out based on the grid side
        substring_to_exclude = "_short" if self.grid_side == "long" else "_long"
        # Filter the DataFrame to exclude rows containing the substring
        df_current_state = df_current_state[~df_current_state['side'].str.contains(substring_to_exclude)]
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
                        if triggered_price > price:
                            df_grid.loc[df_grid['grid_id'] == grid_id_engaged, "status"] = "triggered"
                        else:
                            df_grid.loc[df_grid['grid_id'] == grid_id_engaged, "status"] = "triggered_below"
                    elif self.grid_side == "short":
                        if triggered_price < price:
                            df_grid.loc[df_grid['grid_id'] == grid_id_engaged, "status"] = "triggered"
                        else:
                            df_grid.loc[df_grid['grid_id'] == grid_id_engaged, "status"] = "triggered_below"
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

    def set_buying_size(self, buying_size):
        self.buying_size = buying_size

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
        size = self.buying_size
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
                open_long_triggered.append(long_fake_triggered)
                close_fake_grid_id = df_grid.loc[df_grid['grid_id'] == long_fake_triggered, 'close_grid_id'].values[0]
                close_grid_id_list.append(close_fake_grid_id)
                df_grid.loc[df_grid['grid_id'] == long_fake_triggered, 'changes'] = False
                df_grid.loc[df_grid['grid_id'] == long_fake_triggered, 'status'].values[0]
                if df_grid.loc[df_grid['grid_id'] == long_fake_triggered, 'status'].values[0] != "pending" \
                        and df_grid.loc[df_grid['grid_id'] == long_fake_triggered, 'status'].values[0] != "engaged":
                    df_grid.loc[df_grid['grid_id'] == long_fake_triggered, 'status'] = "on_hold"
                if df_grid.loc[df_grid['grid_id'] == close_fake_grid_id, 'status'].values[0] != "pending" \
                        and df_grid.loc[df_grid['grid_id'] == close_fake_grid_id, 'status'].values[0] != "engaged":
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
            order_to_execute = {}
            order_to_execute["symbol"] = symbol
            order_to_execute["gross_size"] = size
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

        # has_false = df['bool_position_limits'].eq(False).any()
        # if has_false:
        #     print("toto")

    def update_executed_trade_status(self, symbol, lst_orders):
        df_grid = self.grid[symbol]
        for order in lst_orders:
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

    def update_nb_open_positions(self, symbol, df_open_positions):
        if len(df_open_positions) > 0:
            df_open_positions_filtered = df_open_positions[(df_open_positions['symbol'] == symbol) & (df_open_positions['holdSide'] == "long")]
            if len(df_open_positions_filtered) > 0:
                sum_available_position = df_open_positions_filtered['total'].sum() if not df_open_positions_filtered.empty else 0
                self.leverage = df_open_positions_filtered['leverage'].iloc[0]
                self.previous_nb_open_positions = self.nb_open_positions
                previous_nb_open_positions = self.previous_nb_open_positions
                self.nb_open_positions = int(sum_available_position / self.buying_size / self.leverage)
                self.nb_open_positions_triggered = self.nb_open_positions - previous_nb_open_positions
            else:
                self.previous_nb_open_positions = self.nb_open_positions
                self.nb_open_positions = 0
                self.nb_open_positions_triggered = 0

    def get_nb_open_positions_from_state(self, symbol):
        row = self.df_nb_open_positions[self.df_nb_open_positions['symbol'] == symbol]
        if not row.empty:
            idx = row.index[0]
            res = row.at[idx, 'nb_open_positions']
            del idx
        else:
            res = 0
        del row
        return res

    def filter_lst_close_execute_order(self, symbol, lst_order_to_execute):
        df = self.grid[symbol]

        condition_engaged = df['status'] == 'engaged'
        # condition_open_long = df['side'] == 'open_long'
        condition_close_long = df['side'] == 'close_long'

        df_filtered_previous_close_long = df[condition_close_long & condition_engaged]
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

    def normalize_grid_price(self, symbol, pricePlace, priceEndStep):
        df = self.grid[symbol]
        df['position'] = df['position'].apply(lambda x: self.normalize_price(x, pricePlace, priceEndStep))
        self.log("grid price normalized: " + str(df['position'].tolist()))
        del df
        del pricePlace
        del priceEndStep

    def normalize_price(self, amount, pricePlace, priceEndStep):
        amount = amount * pow(10, pricePlace)
        amount = math.floor(amount)
        amount = amount * pow(10, -pricePlace)
        amount = round(amount, pricePlace)
        # Calculate the decimal without using %
        decimal_multiplier = priceEndStep * pow(10, -pricePlace)
        decimal = amount - math.floor(round(amount / decimal_multiplier)) * decimal_multiplier
        amount = amount - decimal
        amount = round(amount, pricePlace)
        del pricePlace
        del decimal_multiplier
        del decimal
        return amount

    def get_buying_market_order(self, symbol, size, price):
        order_to_execute = {}
        order_to_execute["symbol"] = symbol
        order_to_execute["gross_size"] = size
        order_to_execute["type"] = "OPEN_LONG"   # OPEN_SHORT for short_grid
        order_to_execute["price"] = price
        order_to_execute["gross_size"] = size
        order_to_execute["grid_id"] = -1
        return order_to_execute

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

            lst_lst_orderId = df_current_state["orderId"].to_list()
            for lst_orderId in lst_lst_orderId:
                if len(lst_orderId) > 1:
                    print("toto")

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