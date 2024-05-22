from . import rtdp, rtstr, rtctrl
import math
import pandas as pd
import numpy as np

from . import utils
from src import logger

class StrategyGridTradingShort(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        self.zero_print = False
        self.grid = GridPosition(self.lst_symbols, self.grid_high, self.grid_low, self.nb_grid, self.percent_per_grid, self.zero_print, self.loggers)
        if self.percent_per_grid != 0:
            self.nb_grid = self.grid.get_grid_nb_grid()
        self.df_grid_buying_size = pd.DataFrame()
        self.dct_info_to_txt = ""
        self.execute_timer = None
        self.df_price = None
        self.mutiple_strategy = False
        self.side = "short"

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
        return "StrategyGridTradingShort"

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
            buying_size = self.get_grid_buying_size(symbol)
            # self.execute_timer.set_end_time("rtstr", "set_broker_current_state", "get_grid_buying_size", self.iter_set_broker_current_state)

            # self.execute_timer.set_start_time("rtstr", "set_broker_current_state", "update_control_multi_position", self.iter_set_broker_current_state)
            self.grid.update_control_multi_position(symbol, df_current_states, df_open_positions)
            # self.execute_timer.set_end_time("rtstr", "set_broker_current_state", "update_control_multi_position", self.iter_set_broker_current_state)

            # self.execute_timer.set_start_time("rtstr", "set_broker_current_state", "update_open_orders", self.iter_set_broker_current_state)
            self.grid.update_open_orders(symbol, df_current_states)
            # self.execute_timer.set_end_time("rtstr", "set_broker_current_state", "update_open_orders", self.iter_set_broker_current_state)

            # self.execute_timer.set_start_time("rtstr", "set_broker_current_state", "update_nb_open_positions", self.iter_set_broker_current_state)
            self.grid.update_nb_open_positions(symbol, df_open_positions, buying_size)
            # self.execute_timer.set_end_time("rtstr", "set_broker_current_state", "update_nb_open_positions", self.iter_set_broker_current_state)

            # self.execute_timer.set_start_time("rtstr", "set_broker_current_state", "update_pending_status_from_current_state", self.iter_set_broker_current_state)
            self.grid.update_pending_status_from_current_state(symbol, df_current_state)
            # self.execute_timer.set_end_time("rtstr", "set_broker_current_state", "update_pending_status_from_current_state", self.iter_set_broker_current_state)

            if symbol in df_price['symbols'].tolist():
                # self.execute_timer.set_start_time("rtstr", "set_broker_current_state", "cross_check_with_current_state", self.iter_set_broker_current_state)
                self.grid.cross_check_with_current_state(symbol, df_current_state)
                # self.execute_timer.set_end_time("rtstr", "set_broker_current_state", "cross_check_with_current_state", self.iter_set_broker_current_state)

                # self.execute_timer.set_start_time("rtstr", "set_broker_current_state", "update_grid_side", self.iter_set_broker_current_state)
                self.grid.update_grid_side(symbol, df_price.loc[df_price['symbols'] == symbol, 'values'].values[0])
                # self.execute_timer.set_end_time("rtstr", "set_broker_current_state", "update_grid_side", self.iter_set_broker_current_state)
            # else:
                # self.execute_timer.set_time_to_zero("rtstr", "set_broker_current_state", "cross_check_with_current_state", self.iter_set_broker_current_state)
                # self.execute_timer.set_time_to_zero("rtstr", "set_broker_current_state", "update_grid_side", self.iter_set_broker_current_state)

            # self.execute_timer.set_start_time("rtstr", "set_broker_current_state", "get_order_list", self.iter_set_broker_current_state)
            lst_order_to_execute = self.grid.get_order_list(symbol, buying_size, df_current_state)
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
            self.grid.print_grid()

        del df_current_states
        del df_open_positions
        del df_price

        self.iter_set_broker_current_state += 1

        return lst_order_to_execute

    def set_normalized_grid_price(self, lst_symbol_plc_endstp):
        for price_plc in lst_symbol_plc_endstp:
            self.grid.normalize_grid_price(price_plc['symbol'], price_plc['pricePlace'], price_plc['priceEndStep'])
        del lst_symbol_plc_endstp

    def activate_grid(self, current_state):
        if not current_state:
            return []
        del self.df_prices
        self.df_prices = current_state["prices"]
        lst_buying_market_order = []
        for symbol in self.lst_symbols:
            lst_buying_market_order = []
            buying_size = self.get_grid_buying_size(symbol)
            if symbol in self.df_prices['symbols'].tolist():
                price = self.df_prices.loc[self.df_prices['symbols'] == symbol, 'values'].values[0]
                order = self.grid.get_buying_market_order(symbol, buying_size, price)
                lst_buying_market_order.append(order)
        del buying_size
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
        self.record_grid_status()
        # self.execute_timer.set_end_time("rtstr", "get_info_msg_status", "record_grid_status", self.iter_set_broker_current_state)

        del self.dct_info_to_txt
        for symbol in self.lst_symbols:
            self.grid.get_grid_info(symbol)
            if self.grid.dct_change_status():
                self.dct_info_to_txt = self.grid.dct_status_info_to_txt(symbol)
                return self.dct_info_to_txt
            else:
                self.dct_info_to_txt = ""
                return None

    def get_grid(self, cpt):
        # CEDE: MULTI SYMBOL TO BE IMPLEMENTED IF EVER ONE DAY.....
        for symbol in self.lst_symbols:
            return self.grid.get_grid(symbol, cpt)

    def record_grid_status(self):
        if hasattr(self, 'df_price'):
            for symbol in self.lst_symbols:
                df_grid_values = self.grid.get_grid_for_record(symbol)
                self.execute_timer.set_grid_infos(df_grid_values, self.side)

class GridPosition():
    def __init__(self, lst_symbols, grid_high, grid_low, nb_grid, percent_per_grid, debug_mode=True, loggers=[]):
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
        self.control_multi_position = self.init_control_multi_position()
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

        self.df_nb_open_positions = pd.DataFrame(columns=['symbol', 'size', 'leverage', 'side', 'positions_size', 'nb_open_positions'])
        self.df_nb_open_positions['symbol'] = self.lst_symbols
        self.df_nb_open_positions['size'] = 0
        self.df_nb_open_positions['leverage'] = 0
        self.df_nb_open_positions['side'] = "SHORT"
        self.df_nb_open_positions['positions_size'] = 0
        self.df_nb_open_positions['nb_open_positions'] = 0
        self.df_nb_open_positions['nb_open_limit_order'] = 0
        self.df_nb_open_positions['nb_previous_open_positions'] = 0
        self.df_nb_open_positions['diff_position'] = 0
        self.df_nb_open_positions['previous_nb_open_positions'] = 0
        self.df_nb_open_positions['previous_nb_open_limit_order'] = 0

        self.max_grid_close_order = -1
        self.min_grid_close_order = -1
        self.nb_close_missing = -1

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

        self.columns = ["grid_id", "position", "orderId", "previous_side", "side", "changes", "status"]
        self.grid = {key: pd.DataFrame(columns=self.columns) for key in self.lst_symbols}
        for symbol in lst_symbols:
            self.grid[symbol]["position"] = self.lst_grid_values
            self.grid[symbol]["grid_id"] = np.arange(len(self.grid[symbol]))[::-1]
            self.grid[symbol]["orderId"] = ""
            self.grid[symbol]["previous_side"] = False
            self.grid[symbol]["side"] = ""
            self.grid[symbol]["changes"] = True
            self.grid[symbol]["status"] = "empty"
            self.grid[symbol]["cross_checked"] = False
            self.grid[symbol]["on_edge"] = False

        self.lst_limit_order_missing = []
        self.nb_open_positions = 0
        self.nb_close_positions = 0
        self.diff_nb_open_positions = 0
        self.diff_nb_close_positions = 0

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

    def update_grid_side(self, symbol, position):
        df = self.grid[symbol]
        self.current_price = position

        self.previous_grid_uniq_position = self.grid_uniq_position
        self.on_edge = False
        df['on_edge'] = False
        if (df['position'] == position).any():
            self.on_edge = True
            df.loc[df['position'] == position, 'on_edge'] = True
            self.log('PRICE ON GRID EDGE - CROSSING OR NOT CROSSING')
            delta = abs((df.at[0, 'position'] - df.at[1,'position']) / 2)
            if df.loc[df['position'] == position, 'cross_checked'].values[0] == False:
                if df.loc[df['position'] == position, 'side'].values[0] == "open_short":
                    position += delta
                else:
                    position -= delta
            else:
                if df.loc[df['position'] == position, 'side'].values[0] == "open_short":
                    position -= delta
                else:
                    position += delta

        df['previous_side'] = df['side']
        # Set the 'side' column based on conditions
        df.loc[df['position'] < position, 'side'] = 'close_short'
        df.loc[df['position'] > position, 'side'] = 'open_short'

        # Compare if column1 and column2 are the same
        df['changes'] = df['previous_side'] != df['side']

        self.previous_top_grid = self.top_grid
        self.top_grid = False
        self.previous_bottom_grid = self.bottom_grid
        self.bottom_grid = False
        if self.current_price > df['position'].max():
            # OUT OF GRID TOP POSITION
            higher_grid_id = df['grid_id'].max()
            lower_grid_id = df['grid_id'].max()
            self.top_grid = True
            self.grid_uniq_position = lower_grid_id
        elif self.current_price < df['position'].min():
            # OUT OF GRID BOTTOM POSITION
            higher_grid_id = df['grid_id'].min()
            lower_grid_id = df['grid_id'].min()
            self.bottom_grid = True
            self.grid_uniq_position = lower_grid_id
        else:
            higher_grid_id = df[df['position'] > self.current_price]['grid_id'].min()
            lower_grid_id = df[df['position'] < self.current_price]['grid_id'].max()
            self.grid_uniq_position = min(higher_grid_id, lower_grid_id)
            self.top_grid = False
            self.bottom_grid = False

        lst_high_low = [higher_grid_id, lower_grid_id]
        self.grid_position = lst_high_low
        del higher_grid_id
        del lower_grid_id
        del lst_high_low

        if (self.previous_grid_uniq_position != self.grid_uniq_position) \
                or self.on_edge \
                or (self.top_grid and (not self.previous_top_grid))\
                or (self.bottom_grid and (not self.previous_bottom_grid))\
                or ((self.diff_close_position != 0)
                    and (self.diff_open_position != 0)):
            self.grid_move = True
        else:
            self.grid_move = False
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

    def cross_check_with_current_state(self, symbol, df_current_state_all):
        df_grid = self.grid[symbol]
        df_grid['cross_checked'] = False
        df_current_state = df_current_state_all[df_current_state_all["symbol"] == symbol]
        df_current_state = df_current_state[~df_current_state['side'].str.contains("_long")]
        lst_df_grid_columns = df_grid.columns.to_list()

        df_current_state_for_merge = df_current_state.copy()
        df_current_state_for_merge.rename(columns={'gridId': 'grid_id'}, inplace=True)
        # df_current_state_for_merge.rename(columns={'price': 'position'}, inplace=True) # CEDE: Not cross check against price

        df_merged = df_grid.merge(df_current_state_for_merge, how='left')
        df_merged['cross_checked'] = df_merged['leverage'].notna()
        df_merged = df_merged[lst_df_grid_columns]
        self.grid[symbol]['cross_checked'] = df_merged['cross_checked']
        del df_merged
        del df_current_state_for_merge
        del lst_df_grid_columns

    def get_order_list(self, symbol, size, df_current_order):
        """
        order_to_execute = {
            "symbol": order.symbol,
            "gross_size": order.gross_size,
            "type": order.type,
            "price": order.price
            "grid_id": grid_id
        }
        """
        df_grid = self.grid[symbol]

        df_filtered_changes = df_grid[df_grid['changes']]
        df_filtered_checked = df_grid[~df_grid['cross_checked']]
        df_filtered_pending = df_grid[df_grid['status'].isin(["pending", "empty", "on_hold"])]

        # lst_filtered_on_edge = df_grid[df_grid['on_edge']]['grid_id'].tolist()

        lst_order_grid_id = df_filtered_changes['grid_id'].tolist() \
                            + df_filtered_checked['grid_id'].tolist() \
                            + df_filtered_pending['grid_id'].tolist()
        lst_order_grid_id = list(set(lst_order_grid_id))

        # TOP GRID CANNOT BE ACTIVATED AS OPEN_SHORT LIMIT ORDER AS PER SPEC
        # MODIF CEDE SPECIFIC LONG VS SHORT
        bottom_grid_id = df_grid["grid_id"].min()
        if (bottom_grid_id in lst_order_grid_id) \
                and (df_grid.loc[df_grid['grid_id'] == bottom_grid_id, 'side'].values[0] == "open_short"):
            lst_order_grid_id.remove(bottom_grid_id)

        # if len(lst_filtered_on_edge) > 0:
        #     lst_order_grid_id = [item for item in lst_filtered_on_edge if item not in lst_order_grid_id]

        # Safety net to avoid any multiple / redundant limit order
        lst_price_existing_orders = df_current_order['price'].tolist()
        lst_gridId_existing_orders = [df_grid.loc[df_grid['position'] == price, 'grid_id'].values[0] for price in lst_price_existing_orders]
        lst_order_grid_id = [grid_id for grid_id in lst_order_grid_id if grid_id not in lst_gridId_existing_orders]

        lst_order = []
        for grid_id in lst_order_grid_id:
            self.clear_orderId(symbol, grid_id)
            order_to_execute = {}
            order_to_execute["symbol"] = symbol
            order_to_execute["gross_size"] = size
            if df_grid.loc[df_grid["grid_id"] == grid_id, 'side'].values[0] == "open_short":
                order_to_execute["type"] = "OPEN_SHORT_ORDER"
            elif df_grid.loc[df_grid["grid_id"] == grid_id, 'side'].values[0] == "close_short":
                order_to_execute["type"] = "CLOSE_SHORT_ORDER"
            elif df_grid.loc[df_grid["grid_id"] == grid_id, 'side'].values[0] == "open_short":
                order_to_execute["type"] = "OPEN_SHORT_ORDER"
            elif df_grid.loc[df_grid["grid_id"] == grid_id, 'side'].values[0] == "close_short":
                order_to_execute["type"] = "CLOSE_SHORT_ORDER"
            if "type" in order_to_execute:
                order_to_execute["price"] = df_grid.loc[df_grid["grid_id"] == grid_id, 'position'].values[0]
                order_to_execute["grid_id"] = grid_id
                lst_order.append(order_to_execute)
            del order_to_execute

        sorting_order = ['OPEN_LONG_ORDER', 'OPEN_SHORT_ORDER', 'CLOSE_LONG_ORDER', 'CLOSE_SHORT_ORDER']
        sorted_list = sorted(lst_order, key=lambda x: sorting_order.index(x['type']))

        del df_grid
        del df_filtered_changes
        del df_filtered_checked
        del df_filtered_pending
        del lst_order_grid_id
        del sorting_order
        del bottom_grid_id
        del lst_price_existing_orders
        del lst_gridId_existing_orders
        del lst_order

        return sorted_list

    def set_to_pending_execute_order(self, symbol, lst_order_to_execute):
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

    def clear_orderId(self, symbol, grid_id):
        df = self.grid[symbol]
        df.loc[df['orderId'] == grid_id, 'orderId'] = ""
        del df

    def print_grid(self):
        if not self.zero_print:
            for symbol in self.lst_symbols:
                df_grid = self.grid[symbol]
                self.log("\n" + df_grid.to_string(index=False))
                del df_grid

    def update_nb_open_positions(self, symbol, df_open_positions, buying_size):
        self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'size'] = buying_size
        if len(df_open_positions) > 0:
            filtered_df = df_open_positions[(df_open_positions['symbol'] == symbol) & (df_open_positions['holdSide'] == "short")]
            self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_previous_open_positions'] = self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_open_positions'].values[0]
            if len(filtered_df) > 0:
                sum_available_position = filtered_df['total'].sum() if not filtered_df.empty else 0
                # sum_available_position = sum_available_position / filtered_df['leverage'].iloc[0]    # Modif CEDE
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'positions_size'] = sum_available_position
                leverage = filtered_df['leverage'].iloc[0]
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'leverage'] = leverage
                nb_open_positions = int(sum_available_position / buying_size / leverage)
                # nb_open_positions = int(sum_available_position / buying_size)
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_open_positions'] = nb_open_positions
                previous_nb_open_positions = self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_previous_open_positions'].values[0]
                self.df_nb_open_positions['previous_nb_open_positions'] = previous_nb_open_positions
                self.diff_open_position = self.nb_open_positions - self.control_multi_position['nb_previous_open_short']
                self.diff_close_position = self.nb_close_positions - self.control_multi_position['nb_previous_close_short']
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'diff_open_position'] = self.diff_open_position
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'diff_close_position'] = self.diff_close_position
                del sum_available_position
                del leverage
                del nb_open_positions
                del previous_nb_open_positions
            else:
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'positions_size'] = 0
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_open_positions'] = 0
            del filtered_df
        else:
            self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'positions_size'] = 0
            self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_open_positions'] = 0
            self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'leverage'] = 0
            previous_nb_open_positions = self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_previous_open_positions'].values[0]
            nb_open_positions = 0
            self.diff_position = nb_open_positions - previous_nb_open_positions
            self.diff_open_position = self.nb_open_positions - self.control_multi_position['nb_previous_open_short']
            self.diff_close_position = self.nb_close_positions - self.control_multi_position['nb_previous_close_short']
            self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'diff_open_position'] = self.diff_open_position
            self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'diff_close_position'] = self.diff_close_position
            del previous_nb_open_positions
            del nb_open_positions

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

    def get_nb_open_positions_from_positions(self, symbol):
        row = self.df_nb_open_positions[self.df_nb_open_positions['symbol'] == symbol]
        if not row.empty:
            res = row['nb_open_positions'].iloc[0]
            del row
            return res
        else:
            del row
            return 0

    def get_nb_open_limit_orders(self, symbol):
        row = self.df_nb_open_positions[self.df_nb_open_positions['symbol'] == symbol]
        if not row.empty:
            res = row['nb_open_limit_order'].iloc[0]
            del row
            return res
        else:
            del row
            return 0

    def filter_lst_close_execute_order(self, symbol, lst_order_to_execute):
        df = self.grid[symbol]
        # nb positions returned open by the broker
        nb_close_selected_to_be_open = self.control_multi_position['nb_open_short_triggered'] + self.remaining_close_to_be_open

        # nb of open limit order returned open by the broker
        nb_open_limit_already_open = self.get_nb_open_limit_orders(symbol)
        df_filtered = df[(df['side'] == 'open_short')]
        nb_open_limit_order = len(df_filtered)
        del df_filtered
        nb_open_selected_to_be_open = nb_open_limit_order - nb_open_limit_already_open
        # nb_open_selected_to_be_open = self.control_multi_position['nb_close_short_triggered']

        self.nb_open_selected_to_be_open = nb_open_selected_to_be_open
        self.nb_close_selected_to_be_open = nb_close_selected_to_be_open

        if nb_close_selected_to_be_open == 0 \
                and nb_open_selected_to_be_open == 0:
            self.trend = "FLAT"
        elif nb_close_selected_to_be_open == 0 \
                and nb_open_selected_to_be_open > 0:
            self.trend = "DOWN"
        elif nb_close_selected_to_be_open > 0 \
             and nb_open_selected_to_be_open == 0:
            self.trend = "UP"
        elif nb_close_selected_to_be_open > 0 \
             and nb_open_selected_to_be_open > 0:
            self.trend = "VOLATILITY"

        filtered_orders = []
        # Filter OPEN orders
        for order in lst_order_to_execute:
            if order["type"] in ["OPEN_LONG_ORDER", "OPEN_SHORT_ORDER"]\
                    and self.filter_open_control_multi_position(order["grid_id"]):
                filtered_orders.append(order)
        # Filter CLOSE orders and sort them by price
        close_orders = sorted(
            (order for order in lst_order_to_execute if order["type"] in ["CLOSE_LONG_ORDER", "CLOSE_SHORT_ORDER"] and self.filter_close_limit_order(order["grid_id"])),
            key=lambda x: x["price"],
            reverse=True
        )

        if self.trend == "VOLATILITY":
            grid_trend_msg = "UP / DOWN HIGH VOLATILITY "
            if len(close_orders) > 0:
                # avoid gaps
                first_to_close = close_orders[0]["grid_id"]
                if first_to_close == 0:
                    filtered_orders.extend(close_orders[1:(nb_close_selected_to_be_open + 1)])
                else:
                    below_first_to_close = first_to_close - 1
                    if df.loc[df["grid_id"] == below_first_to_close, "side"].values[0] == "close_short":
                        filtered_orders.extend(close_orders[:nb_close_selected_to_be_open])
                    else:
                        filtered_orders.extend(close_orders[1:(nb_close_selected_to_be_open+1)])
        # Append a subset of CLOSE orders based on the number of open positions
        elif self.trend == "DOWN":
            grid_trend_msg = "DOWN -> " + " nb_open_selected_to_be_open: " + str(nb_open_selected_to_be_open)
            # filtered_orders.extend(close_orders[:nb_selected_to_be_open])
        elif self.trend == "UP":
            grid_trend_msg = "UP -> " + " nb_close_selected_to_be_open: " + str(nb_close_selected_to_be_open)
            if len(close_orders) > 0:
                # avoid gaps
                first_to_close = close_orders[0]["grid_id"]
                if first_to_close == 0:
                    filtered_orders.extend(close_orders[1:(nb_close_selected_to_be_open + 1)])
                else:
                    below_first_to_close = first_to_close + 1
                    if df.loc[df["grid_id"] == below_first_to_close, "side"].values[0] == "close_short":
                        filtered_orders.extend(close_orders[:(nb_close_selected_to_be_open)])
                    else:
                        filtered_orders.extend(close_orders[1:(nb_close_selected_to_be_open + 1)])
        elif self.trend == "FLAT":
            grid_trend_msg = "FLAT ->" + " nb_close_selected_to_be_open: " + str(nb_close_selected_to_be_open) + "\n"
            grid_trend_msg += "FLAT ->" + " nb_open_selected_to_be_open: " + str(nb_open_selected_to_be_open)
            # filtered_orders.extend(close_orders[:nb_selected_to_be_open])

        self.log(grid_trend_msg)
        del grid_trend_msg

        df_filtered = df[(df['status'] == 'engaged')
                         & (df['side'] == 'open_short')
                         & (df['cross_checked'])]
        lst_open_engaged = df_filtered['grid_id'].tolist()
        del df_filtered

        if len(lst_open_engaged) > 1:
            self.max_grid_open_order = max(lst_open_engaged)
            self.min_grid_open_order = min(lst_open_engaged)

            reference_list = list(range(self.min_grid_open_order, self.max_grid_open_order + 1))
            # Find the missing elements
            missing_elements = [i for i in reference_list if i not in lst_open_engaged]
            self.nb_open_missing = len(missing_elements)
        else:
            if len(lst_open_engaged) == 1:
                self.max_grid_open_order = lst_open_engaged[0]
                self.min_grid_open_order = lst_open_engaged[0]
                self.nb_open_missing = 0
            else:
                self.max_grid_open_order = -1
                self.min_grid_open_order = -1
                self.nb_open_missing = -1

        # fill the gaps
        # get lower close position
        # get higher close position
        # get holes that are not engaged
        # get numbers of close order into filtered_orders
        # replace grid id of holes in the filtered_orders
        df_filtered = df[(df['status'] == 'engaged')
                         & (df['side'] == 'close_short')
                         & (df['cross_checked'])]
        lst_close_engaged = df_filtered['grid_id'].tolist()
        count_lst_close_engaged = len(lst_close_engaged)
        del df_filtered

        if len(lst_close_engaged) > 1:
            self.max_grid_close_order = max(lst_close_engaged)
            self.min_grid_close_order = min(lst_close_engaged)

            reference_list = list(range(self.min_grid_close_order, self.max_grid_close_order + 1))
            # Find the missing elements
            missing_elements = [i for i in reference_list if i not in lst_close_engaged]
            self.nb_close_missing = len(missing_elements)
            if len(missing_elements) > 0 \
                    and len(filtered_orders) > 0:
                filtered_orders = sorted(filtered_orders, key=lambda x: x["grid_id"])
                for order in reversed(filtered_orders):
                    if order["type"] in ["CLOSE_LONG_ORDER", "CLOSE_SHORT_ORDER"] \
                            and (order["grid_id"] > self.max_grid_close_order)\
                            and (len(missing_elements) > 0):
                        order["grid_id"] = missing_elements[0]
                        order["price"] = df.loc[df['grid_id'] == missing_elements[0], 'position'].values[0]
                        del missing_elements[0]
        else:
            if len(lst_close_engaged) == 1:
                self.max_grid_close_order = lst_close_engaged[0]
                self.min_grid_close_order = lst_close_engaged[0]
                self.nb_close_missing = 0
            else:
                self.max_grid_close_order = -1
                self.min_grid_close_order = -1
                self.nb_close_missing = -1

        self.set_grid_positions_to_on_hold(lst_order_to_execute, filtered_orders)
        filtered_orders_to_execute = []
        for order in filtered_orders:
            filtered_orders_to_execute.append(order.copy())
        lst_to_clear = [close_orders, lst_close_engaged, lst_open_engaged, lst_order_to_execute, filtered_orders]
        for lst in lst_to_clear:
            for order in lst:
                if hasattr(order, 'clear') and callable(order.clear):
                    order.clear()
        nb_close_in_order_list = 0
        for order in filtered_orders_to_execute:
            if order["type"] in ["CLOSE_LONG_ORDER", "CLOSE_SHORT_ORDER"]:
                nb_close_in_order_list += 1

        if (nb_close_selected_to_be_open > 0) \
                and (nb_close_in_order_list < self.control_multi_position['nb_open_short_triggered']):
            self.remaining_close_to_be_open += self.control_multi_position['nb_open_short_triggered'] - nb_close_in_order_list
        if nb_close_in_order_list > self.control_multi_position['nb_open_short_triggered']:
            self.remaining_close_to_be_open = self.remaining_close_to_be_open - (nb_close_in_order_list - self.control_multi_position['nb_open_short_triggered'])
        if self.remaining_close_to_be_open < 0:
            self.remaining_close_to_be_open = 0

        count_close_order_in_lst = sum(1 for d in filtered_orders_to_execute if d.get("type") in ["CLOSE_LONG_ORDER", "CLOSE_SHORT_ORDER"])
        if count_close_order_in_lst > 0 \
                and (count_lst_close_engaged + count_close_order_in_lst) > self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_open_positions'].values[0]:
            nb_close_to_drop_from_list = self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_open_positions'].values[0] - (count_lst_close_engaged + count_close_order_in_lst)
            if nb_close_to_drop_from_list > count_close_order_in_lst:
                nb_close_to_drop_from_list = count_close_order_in_lst

            max_drop = nb_close_to_drop_from_list
            dropped_count = 0
            sorted_list = sorted(filtered_orders_to_execute, key=lambda x: x["price"], reverse=True)
            filtered_list = []
            for d in sorted_list:
                if dropped_count < max_drop and d.get("type") in ["CLOSE_LONG_ORDER", "CLOSE_SHORT_ORDER"]:
                    dropped_count += 1
                else:
                    filtered_list.append(d)
            filtered_orders_to_execute = filtered_list
            self.log("************** discrepancy between list of CLOSE and nb_open_positions ******************")
            self.log("nb_open_positions: " + str(self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_open_positions'].values[0]))
            self.log("count_close_order_in_lst: " + str(count_close_order_in_lst))
            self.log("nb_to_close: " + str(nb_close_to_drop_from_list))

        del df
        del close_orders
        del lst_close_engaged
        del lst_open_engaged
        del lst_order_to_execute
        del filtered_orders
        del lst
        del lst_to_clear
        return filtered_orders_to_execute

    def cross_check_order_with_price(self, lst_orders):
        lst_filtered = []
        for order in lst_orders:
            if order["type"] in ["OPEN_SHORT_ORDER"]:
                if order["price"] > self.current_price:
                    lst_filtered.append(order)
            elif order["type"] in ["CLOSE_SHORT_ORDER"]:
                if order["price"] < self.current_price:
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
        order_to_execute["type"] = "OPEN_SHORT"   # OPEN_SHORT for short_grid
        order_to_execute["price"] = price
        order_to_execute["gross_size"] = size
        order_to_execute["grid_id"] = -1
        return order_to_execute

    def init_control_multi_position(self):
        multi_position = {}
        multi_position['lst_previous_close_short'] = []
        multi_position['nb_previous_close_short'] = []
        multi_position['lst_new_close_short'] = []
        multi_position['nb_new_close_short'] = 0
        multi_position['lst_previous_open_short'] = []
        multi_position['nb_previous_open_short'] = []
        multi_position['lst_new_open_short'] = []
        multi_position['nb_new_open_short'] = 0
        multi_position['lst_open_short'] = []
        multi_position['nb_close_short_triggered'] = 0
        multi_position['nb_open_short_triggered'] = 0
        multi_position['lst_close_short_triggered'] = []
        multi_position['nb_current_limit_close'] = 0
        multi_position['nb_current_limit_open'] = 0

        return multi_position

    def update_open_orders(self, symbol, df_current_states):
        previous_nb_open_limit_order = self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_open_limit_order'].values[0]
        self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'previous_nb_open_limit_order'] = previous_nb_open_limit_order
        nb_open_limit_order = len(df_current_states[(df_current_states['symbol'] == symbol) & (df_current_states['side'] == "open_short")])
        self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_open_limit_order'] = nb_open_limit_order

        self.nb_previous_open_positions = self.get_nb_open_positions_from_state(symbol)
        self.nb_previous_open_positions = self.nb_open_positions
        self.nb_open_positions = len(df_current_states[(df_current_states['symbol'] == symbol) & (df_current_states['side'] == "open_short")])
        self.diff_nb_open_positions = self.nb_open_positions - self.nb_previous_open_positions
        self.nb_previous_close_positions = self.nb_close_positions
        self.nb_close_positions = len(df_current_states[(df_current_states['symbol'] == symbol) & (df_current_states['side'] == "close_short")])
        self.diff_nb_close_positions = self.nb_close_positions - self.nb_previous_close_positions
        del previous_nb_open_limit_order
        del nb_open_limit_order

    def update_control_multi_position(self, symbol, df_current_state, df_open_positions):
        self.control_multi_position['nb_current_limit_close'] = len(df_current_state[(df_current_state['side'] == 'close_short')])
        self.control_multi_position['nb_current_limit_open'] = len(df_current_state[(df_current_state['side'] == 'open_short')])

        df = self.grid[symbol]

        condition_engaged = df['status'] == 'engaged'
        condition_pending = df['status'] == 'pending'
        condition_open_short = df['side'] == 'open_short'
        condition_close_short = df['side'] == 'close_short'

        df_filtered_previous_open_short = df[condition_open_short & (condition_engaged | condition_pending)]
        if len(df_filtered_previous_open_short) > 0:
            lst_previous_open_short = df_filtered_previous_open_short['grid_id'].tolist()
        else:
            lst_previous_open_short = []
        self.control_multi_position['nb_previous_open_short'] = len(df_filtered_previous_open_short)
        self.control_multi_position['lst_previous_open_short'] = lst_previous_open_short

        df_filtered_previous_close_short = df[condition_close_short & condition_engaged]
        if len(df_filtered_previous_close_short) > 0:
            lst_previous_close_short = df_filtered_previous_close_short['grid_id'].tolist()
        else:
            lst_previous_close_short = []
        self.control_multi_position['nb_previous_close_short'] = len(df_filtered_previous_close_short)
        self.control_multi_position['lst_previous_close_short'] = lst_previous_close_short

        condition_symbol = df_current_state['symbol'] == symbol
        condition_open_short = df_current_state['side'] == 'open_short'
        condition_close_short = df_current_state['side'] == 'close_short'

        df_filtered_new_open_short = df_current_state[condition_symbol & condition_open_short]
        if len(df_filtered_new_open_short) > 0:
            lst_new_open_short = df_filtered_new_open_short['gridId'].tolist()
        else:
            lst_new_open_short = []
        self.control_multi_position['nb_new_open_short'] = len(df_filtered_new_open_short)
        self.control_multi_position['lst_new_open_short'] = lst_new_open_short

        df_filtered_new_close_short = df_current_state[condition_symbol & condition_close_short]
        if len(df_filtered_new_close_short) > 0:
            lst_new_close_short = df_filtered_new_close_short['gridId'].tolist()
        else:
            lst_new_close_short = []
        self.control_multi_position['nb_new_close_short'] = len(df_filtered_new_close_short)
        self.control_multi_position['lst_new_close_short'] = lst_new_close_short

        lst_open_short_triggered = [element for element in self.control_multi_position['lst_previous_open_short'] if element not in self.control_multi_position['lst_new_open_short']]
        self.control_multi_position['lst_open_short_triggered'] = lst_open_short_triggered
        self.control_multi_position['nb_open_short_triggered'] = len(lst_open_short_triggered)

        lst_close_short_triggered = [element for element in self.control_multi_position['lst_previous_close_short'] if element not in self.control_multi_position['lst_new_close_short']]
        self.control_multi_position['lst_close_short_triggered'] = lst_close_short_triggered
        self.control_multi_position['nb_close_short_triggered'] = len(lst_close_short_triggered)

        self.control_multi_position['lst_open_short'] = utils.drop_largest_items(self.control_multi_position['lst_open_short'],
                                                                                 self.control_multi_position['nb_close_short_triggered'])

        self.control_multi_position['lst_open_short'] = self.control_multi_position['lst_open_short'] + lst_open_short_triggered
        self.control_multi_position['lst_open_short'] = list(set(self.control_multi_position['lst_open_short']))

        self.control_multi_position['lst_open_short'].append(df["grid_id"].min())
        self.control_multi_position['lst_open_short'] = list(set(self.control_multi_position['lst_open_short']))

        if not self.zero_print:
            self.log("global lst_close_short_triggered: " + str(self.control_multi_position['lst_close_short_triggered']))
            self.log("global nb_close_short_triggered: " + str(self.control_multi_position['nb_close_short_triggered']))
            self.log("global lst_open_short_triggered: " + str(self.control_multi_position['lst_open_short_triggered']))
            self.log("global lst_open_short: " + str(self.control_multi_position['lst_open_short']))

        del df
        del condition_engaged
        del condition_pending
        del condition_open_short
        del condition_close_short
        del df_filtered_previous_open_short
        del df_filtered_previous_close_short
        del lst_previous_close_short
        del condition_symbol
        del df_filtered_new_open_short
        del lst_new_open_short
        del df_filtered_new_close_short
        del lst_new_close_short
        del lst_open_short_triggered
        del lst_close_short_triggered
        del df_current_state
        del df_open_positions

    def filter_open_control_multi_position(self, open_grid_id):
        if (open_grid_id == self.grid_uniq_position) \
                and (self.trend != "DOWN"):
            return False
        else:
            if open_grid_id in self.control_multi_position['lst_open_short']:
                self.log("filter_open_control_multi_position - grid id: {}".format(open_grid_id))
                return False
            else:
                return True

    def filter_close_limit_order(self, open_grid_id):
        if len(self.control_multi_position['lst_open_short_triggered']) > 0:
            top_open_triggered = max(self.control_multi_position['lst_open_short_triggered'])
            if open_grid_id >= top_open_triggered:
                return False
        return True

    def get_grid_info(self, symbol):
        df = self.grid[symbol]
        self.dct_status_info.clear()
        del self.dct_status_info
        self.dct_status_info = {}

        # Count of rows with side == 'close_short' and status == 'engaged'
        self.dct_status_info['nb_limit_close'] = len(df[(df['side'] == 'close_short') & (df['status'] == 'engaged')])

        # Count of rows with side == 'open_short' and status == 'engaged'
        self.dct_status_info['nb_limit_open'] = len(df[(df['side'] == 'open_short') & (df['status'] == 'engaged')])

        self.dct_status_info['nb_current_limit_close'] = self.control_multi_position['nb_current_limit_close']
        self.dct_status_info['nb_current_limit_open'] = self.control_multi_position['nb_current_limit_open']

        self.dct_status_info['nb_position'] = self.get_nb_open_positions_from_state(symbol)
        self.dct_status_info['price'] = self.current_price
        self.dct_status_info['grid_position'] = self.grid_position

        self.dct_status_info['previous_grid_uniq_position'] = self.previous_grid_uniq_position
        self.dct_status_info['grid_uniq_position'] = self.grid_uniq_position
        self.dct_status_info['grid_move'] = self.grid_move

        del df
        return self.dct_status_info

    def dct_change_status(self):
        if self.grid_move:
           self.grid_move = False
           return True
        else:
            return False

    def dct_status_info_to_txt(self, symbol):
        del self.msg
        self.msg = "# GRID LONG: " + "\n"
        self.msg += "RANGE FROM: " + str(self.grid_high) + " TO " + str(self.grid_low) + "\n"
        self.msg += "NB_GRID: " + str(self.nb_grid) + "\n"
        # self.msg += "# GRID POSITION: " + "\n"
        self.msg += "**POSITION: " + str(self.grid_uniq_position) + "**\n"
        self.msg += "**PREV POS: " + str(self.previous_grid_uniq_position) + "**\n"
        if self.grid_uniq_position is not None:
            if self.max_position == 0 \
                    and self.min_position == 0:
                self.max_position = self.grid_uniq_position
                self.min_position = self.grid_uniq_position
            else:
                self.max_position = max(self.max_position, self.grid_uniq_position)
                self.min_position = min(self.min_position, self.grid_uniq_position)
                if isinstance(self.max_position, int) \
                        and isinstance(self.min_position, int):
                    self.msg += "**MAX/MIN POS: " + str(self.max_position) + "/" + str(self.min_position) + "**\n"
        if ((self.dct_status_info['grid_position'][0] >= self.nb_grid)
                and (self.dct_status_info['grid_position'][1] >= self.nb_grid)):
            self.msg += "**ABOVE GRID**\n"
        elif ((self.dct_status_info['grid_position'][0] <= 0)
              and (self.dct_status_info['grid_position'][1] <= 0)):
            self.msg += "**BELOW GRID**\n"
        else:
            self.msg += "**pos range: " + str(self.dct_status_info['grid_position'][0]) + " / " + str(self.dct_status_info['grid_position'][1]) + "**\n"
        self.msg += "broker limit_close: " + str(self.dct_status_info['nb_current_limit_close']) + "\n"
        self.msg += "broker limit_open: " + str(self.dct_status_info['nb_current_limit_open']) + "\n"
        self.msg += "grid limit_close: " + str(self.dct_status_info['nb_limit_close']) + "\n"
        self.msg += "grid limit_open: " + str(self.dct_status_info['nb_limit_open']) + "\n"
        self.msg += "total open_position: " + str(self.dct_status_info['nb_position']) + "\n"

        if len(self.previous_grid_position) == 2:
            self.msg += "# TREND:" + "\n"
            if self.previous_grid_position[0] > self.dct_status_info['grid_position'][0] and self.previous_grid_position[1] > self.dct_status_info['grid_position'][1]:
                self.msg += "DOWN" + "\n"
            elif self.previous_grid_position[0] < self.dct_status_info['grid_position'][0] and self.previous_grid_position[1] < self.dct_status_info['grid_position'][1]:
                self.msg += "UP" + "\n"
            elif self.previous_grid_position[0] == self.dct_status_info['grid_position'][0] and self.previous_grid_position[1] == self.dct_status_info['grid_position'][1]:
                self.msg += "FLAT / NO DIRECTION" + "\n"
            else:
                self.msg += "**ERROR DIRECTION" + "**\n"
                df = self.grid[symbol]
                lst_filtered_on_edge = df[df['on_edge']]['grid_id'].tolist()
                if (len(lst_filtered_on_edge) > 0) \
                        or self.on_edge:
                    self.msg += "**PRICE ON EDGE" + "**\n"
                    self.msg += "**VERIFIED" + "**\n"
                    self.msg += "lst_filtered_on_edge: " + ' '.join(map(str, lst_filtered_on_edge)) + "\n"
                else:
                    self.msg += "**WARNING - PRICE ON EDGE" + "**\n"
                    self.msg += "**NOT VERIFIED" + "**\n"
                    self.msg += "lst_filtered_on_edge empty" + ' '.join(map(str, lst_filtered_on_edge)) + "\n"
                del df
                del lst_filtered_on_edge

        if (self.nb_open_selected_to_be_open != 0) \
                and (self.nb_close_selected_to_be_open != 0):
            self.msg += "WARNING HIGH VOLATILITY:" + "\n"
        if self.nb_open_selected_to_be_open != 0:
            self.msg += "DIFF OPENED POSITION: " + str(self.nb_open_selected_to_be_open) + "\n"
        if self.nb_close_selected_to_be_open != 0:
            self.msg += "DIFF CLOSED POSITION: " + str(self.nb_close_selected_to_be_open) + "\n"

        if (self.max_grid_open_order == -1) \
            and (self.min_grid_open_order == -1):
            self.msg += "no open position" + "\n"
        else:
            if self.nb_open_missing > 0:
                self.msg += "open range: " + str(self.min_grid_open_order) + " / " + str(self.max_grid_open_order) + " gaps: " + str(self.nb_open_missing) + "\n"
            else:
                self.msg += "open range: " + str(self.min_grid_open_order) + " / " + str(self.max_grid_open_order) + "\n"

        if (self.max_grid_close_order == -1) \
            and (self.min_grid_close_order == -1) \
            and (self.nb_close_missing == -1):
            self.msg += "no close position" + "\n"
        else:
            if self.nb_close_missing > 0:
                self.msg += "close range: " + str(self.min_grid_close_order) + " / " + str(self.max_grid_close_order) + " gaps: " + str(self.nb_close_missing) + "\n"
            else:
                self.msg += "close range: " + str(self.min_grid_close_order) + " / " + str(self.max_grid_close_order) + "\n"

        self.previous_grid_position = self.dct_status_info['grid_position']

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