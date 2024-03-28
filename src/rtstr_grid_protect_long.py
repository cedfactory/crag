from . import rtdp, rtstr, rtctrl
import pandas as pd
import numpy as np

import math

class StrategyGridTradingProtectLong(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        self.zero_print = False
        self.grid = GridPosition(self.lst_symbols, self.grid_high, self.grid_low, self.nb_split_up, self.nb_split_down, self.percent_per_grid, self.zero_print)
        if self.percent_per_grid != 0:
            self.nb_grid = self.grid.get_grid_nb_grid()
        self.df_grid_buying_size = pd.DataFrame()

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols

        ds.fdp_features = {
            "ema10" : {"indicator": "ema", "id": "10", "window_size": 10}
        }

        ds.features = self.get_feature_from_fdp_features(ds.fdp_features)
        ds.interval = self.strategy_interval
        print("startegy: ", self.get_info())
        print("strategy features: ", ds.features)
        return ds

    def get_info(self):
        return "StrategyGridProtectLong"

    def condition_for_opening_long_position(self, symbol):
        return False

    def condition_for_opening_short_position(self, symbol):
        return False

    def condition_for_closing_long_position(self, symbol):
        return False

    def condition_for_closing_short_position(self, symbol):
        return False

    def sort_list_symbols(self, lst_symbols):
        print("symbol list: ", lst_symbols)
        return lst_symbols

    def need_broker_current_state(self):
        return True

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

        df_current_states = current_state["open_orders"]
        df_open_positions = current_state["open_positions"]
        df_price = current_state["prices"]

        lst_order_to_execute = []

        for symbol in self.lst_symbols:
            df_current_state = self.grid.set_current_orders_price_to_grid(symbol, df_current_states)
            buying_size = self.get_grid_buying_size(symbol)
            self.grid.update_control_multi_position(symbol, df_current_states, df_open_positions)
            self.grid.update_nb_open_positions(symbol, df_open_positions, buying_size)
            self.grid.update_pending_status_from_current_state(symbol, df_current_state)

            price_for_symbol = df_price.loc[df_price['symbols'] == symbol, 'values'].values[0]
            self.grid.cross_check_with_current_state(symbol, df_current_state)
            self.grid.update_grid_side(symbol, price_for_symbol)
            # self.grid.cross_check_with_current_state(symbol, df_current_state)

            lst_order_to_execute = self.grid.get_order_list(symbol, buying_size, df_current_state)
            self.grid.set_to_pending_execute_order(symbol, lst_order_to_execute)
            lst_order_to_execute = self.grid.filter_lst_close_execute_order(symbol, lst_order_to_execute)
            lst_order_to_execute = self.grid.validate_open_close_balance(symbol, lst_order_to_execute)

        if not self.zero_print:
            df_sorted = df_current_state.sort_values(by='price')
            print("#############################################################################################")
            print("current_state: \n", df_sorted.to_string(index=False))
            print("open_positions: \n", df_open_positions.to_string(index=False))
            print("price: \n", df_price.to_string(index=False))
            lst_order_to_print = []
            for order in lst_order_to_execute:
                lst_order_to_print.append((order["grid_id"], order["price"], order["type"]))
            print("order list: \n", lst_order_to_print)
            self.grid.print_grid()
            print("#############################################################################################")

        return lst_order_to_execute

    def set_normalized_grid_price(self, lst_symbol_plc_endstp):
        for price_plc in lst_symbol_plc_endstp:
            symbol = price_plc['symbol']
            pricePlace = price_plc['pricePlace']
            priceEndStep = price_plc['priceEndStep']
            self.grid.normalize_grid_price(symbol, pricePlace, priceEndStep)

    def activate_grid(self, current_state):
        if not current_state:
            return []

        df_prices = current_state["prices"]
        lst_buying_market_order = []
        for symbol in self.lst_symbols:
            lst_buying_market_order = []
            buying_size = self.get_grid_buying_size(symbol)
            price = df_prices.loc[df_prices['symbols'] == symbol, 'values'].values[0]
            order = self.grid.get_buying_market_order(symbol, buying_size, price)
            lst_buying_market_order.append(order)
        return lst_buying_market_order

    def get_info_msg_status(self):
        # CEDE: MULTI SYMBOL TO BE IMPLEMENTED IF EVER ONE DAY.....
        for symbol in self.lst_symbols:
            dct_info = self.grid.get_grid_info(symbol)
            if self.grid.dct_change_status(dct_info):
                return self.grid.dct_status_info_to_txt(dct_info, symbol)
            else:
                return None

    def get_grid(self, cpt):
        # CEDE: MULTI SYMBOL TO BE IMPLEMENTED IF EVER ONE DAY.....
        for symbol in self.lst_symbols:
            return self.grid.get_grid(symbol, cpt)

class GridPosition():
    def __init__(self, lst_symbols, grid_high, grid_low, nb_split_up, nb_split_down, percent_per_grid, debug_mode=True):

        self.grid_high = grid_high
        self.grid_low = grid_low
        self.top_grid = grid_high * 5
        self.nb_grid = nb_split_up
        self.nb_grid = nb_split_down
        self.lst_symbols = lst_symbols
        self.str_lst_symbol = ' '.join(map(str, lst_symbols))
        self.percent_per_grid = percent_per_grid

        self.zero_print = debug_mode
        self.trend = "FLAT"
        self.grid_position = []
        self.dct_info = None
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
        self.on_edge = False
        self.grid_move = False

        self.df_nb_open_positions = pd.DataFrame(columns=['symbol', 'size', 'leverage', 'side', 'positions_size', 'nb_open_positions'])
        self.df_nb_open_positions['symbol'] = self.lst_symbols
        self.df_nb_open_positions['size'] = 0
        self.df_nb_open_positions['leverage'] = 0
        self.df_nb_open_positions['side'] = "LONG"
        self.df_nb_open_positions['positions_size'] = 0
        self.df_nb_open_positions['nb_open_positions'] = 0
        self.df_nb_open_positions['nb_previous_open_positions'] = 0
        self.df_nb_open_positions['diff_position'] = 0
        self.df_nb_open_positions['previous_nb_open_positions'] = 0

        if self.percent_per_grid !=0:
            self.steps = self.top_grid * self.percent_per_grid / 100
            self.nb_grid = int(self.top_grid / self.steps)
        # Create a list with nb_grid split between high and low
        self.lst_grid_values = np.linspace(self.top_grid, 0, self.nb_grid + 1, endpoint=True).tolist()
        if not self.zero_print:
            print("nb_grid: ", self.nb_grid)
            print("grid steps: ", self.steps)
            print("grid values: ", self.lst_grid_values)

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
                    print("GRID ERROR - open_long vs open_short - grid id: ", grid_id)
            else:
                # CEDE: Executed when high volatility is triggering an limit order just after being raised
                #       and before being recorded by the strategy in the grid process structure
                missing_order = {}
                missing_order['grid_id'] = grid_id
                missing_order['side'] = df.loc[df['grid_id'] == grid_id, 'side'].values[0]
                self.lst_limit_order_missing.append(missing_order)
                print("GRID ERROR - limit order failed - grid_id missing: ", grid_id, ' side: ', missing_order['side'])

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
            print('PRICE ON GRID EDGE - CROSSING OR NOT CROSSING')
            delta = abs((df.at[0, 'position'] - df.at[1,'position']) / 2)
            if df.loc[df['position'] == position, 'cross_checked'].values[0] == False:
                if df.loc[df['position'] == position, 'side'].values[0] == "open_long":
                    position -= delta
                else:
                    position += delta
            else:
                if df.loc[df['position'] == position, 'side'].values[0] == "open_long":
                    position += delta
                else:
                    position -= delta

        df['previous_side'] = df['side']
        # Set the 'side' column based on conditions
        df.loc[df['position'] > position, 'side'] = 'close_long'
        df.loc[df['position'] < position, 'side'] = 'open_long'

        if self.diff_position == 0:
            self.trend = "FLAT"
        elif self.diff_position > 0:
            self.trend = "DOWN"
        elif self.diff_position < 0:
            self.trend = "UP"

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

        self.grid_position = [higher_grid_id, lower_grid_id]

        if (self.previous_grid_uniq_position != self.grid_uniq_position) \
                or self.on_edge \
                or (self.top_grid and (not self.previous_top_grid))\
                or (self.bottom_grid and (not self.previous_bottom_grid))\
                or ((self.diff_close_position != 0)
                    and (self.diff_open_position != 0)):
            self.grid_move = True
        else:
            self.grid_move = False

    # Function to find the index of the closest value in an array
    def find_closest(self, value, array):
        return np.abs(array - value).argmin()

    def set_current_orders_price_to_grid(self, symbol, df_current_state):
        if len(df_current_state) > 0:
            df_current_state = df_current_state[df_current_state['symbol'] == symbol]
            df_grid = self.grid[symbol]
            # Replace values in df_current_state['price'] with the closest values from df_grid['position']
            df_current_state['price'] = [df_grid['position'].iloc[self.find_closest(price, df_grid['position'])] for price in df_current_state['price']]
            # Check if all elements in df_current_state['price'] are in df_grid['position']
            all_prices_in_grid = df_current_state['price'].isin(df_grid['position']).all()
            if not all_prices_in_grid:
                # Print the elements that are different
                different_prices = df_current_state.loc[~df_current_state['price'].isin(df_grid['position']), 'price']
                print("################ WARNING PRICE DIFF WITH ORDER AND GRID ###############################")
                print("Elements in df_current_state['price'] that are not in df_grid['position']:")
                print(different_prices)
                exit(0)
        return df_current_state

    def cross_check_with_current_state(self, symbol, df_current_state_all):
        df_grid = self.grid[symbol]
        df_grid['cross_checked'] = False
        df_current_state = df_current_state_all[df_current_state_all["symbol"] == symbol]

        # Iterate over every row using iterrows()
        # Compare values from both DataFrames using iterrows
        for index_grid, row_grid in df_grid.iterrows():
            for index_c_state, row_c_state in df_current_state.iterrows():
                if (row_grid['grid_id'] == row_c_state['gridId']) \
                        or (row_grid['position'] == row_c_state['price']):
                    if row_grid['side'] == row_c_state['side'] \
                            and row_grid['orderId'] == row_c_state['orderId']:
                        df_grid.loc[index_grid, 'cross_checked'] = True

    def validate_open_close_balance(self, symbol, lst_order_to_execute):
        df_grid = self.grid[symbol]
        total_symbol = self.get_nb_open_positions(symbol)

        total_engaged_open_long = len(df_grid[(df_grid['status'] == 'engaged') & (df_grid['side'] == 'open_long')])
        total_pending_open_long = len(df_grid[(df_grid['status'] == 'pending') & (df_grid['side'] == 'open_long')])

        total_engaged_close_long = len(df_grid[(df_grid['status'] == 'engaged') & (df_grid['side'] == 'close_long')])
        total_pending_close_long = len(df_grid[(df_grid['status'] == 'pending') & (df_grid['side'] == 'close_long')])

        if (total_engaged_close_long + total_pending_close_long) < total_symbol:
            print('close vs total opened not balanced')
            print("total_engaged_close_long: ", total_engaged_close_long)
            print("total_pending_close_long: ", total_pending_close_long)
            print("total_engaged_open_long: ", total_engaged_open_long)
            print("total_pending_open_long: ", total_pending_open_long)
            print("total_symbol: ", total_symbol)
            nb_reduce_open_long = total_symbol - total_engaged_close_long - total_pending_close_long
            print("missing ", nb_reduce_open_long, " close_long")
            lst_order_to_execute = self.remove_open_short_order_from_list(symbol, lst_order_to_execute,
                                                                          nb_reduce_open_long)
        return lst_order_to_execute

    def remove_open_short_order_from_list(self, symbol, order_list, x):
        df_grid = self.grid[symbol]
        open_long_orders = [order for order in order_list if order["type"] == "OPEN_LONG_ORDER"]
        open_long_orders.sort(key=lambda order: order["grid_id"], reverse=True)
        indices_to_remove = [order_list.index(order) for order in open_long_orders[:x]]
        lst_grid_id_dropped = []
        for index in sorted(indices_to_remove, reverse=True):
            lst_grid_id_dropped.append(order_list[index]["grid_id"])
            del order_list[index]
        for grid_id in lst_grid_id_dropped:
            df_grid.loc[df_grid['grid_id'] == grid_id, 'status'] = 'on_hold'
        return order_list

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
        df_grid = self.grid[symbol].copy()

        df_filtered_changes = df_grid[df_grid['changes']]
        df_filtered_checked = df_grid[~df_grid['cross_checked']]
        df_filtered_pending = df_grid[df_grid['status'].isin(["pending", "empty"])]

        # lst_filtered_on_edge = df_grid[df_grid['on_edge']]['grid_id'].tolist()

        lst_order_grid_id = df_filtered_changes['grid_id'].tolist() \
                            + df_filtered_checked['grid_id'].tolist() \
                            + df_filtered_pending['grid_id'].tolist()
        lst_order_grid_id = list(set(lst_order_grid_id))

        # TOP GRID CANNOT BE ACTIVATED AS OPEN_LONG LIMIT ORDER AS PER SPEC
        # MODIF CEDE SPECIFIC LONG VS SHORT
        top_grid_id = df_grid["grid_id"].max()
        if (top_grid_id in lst_order_grid_id) \
                and (df_grid.loc[df_grid['grid_id'] == top_grid_id, 'side'].values[0] == "open_long"):
            lst_order_grid_id.remove(top_grid_id)

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
            if df_grid.loc[df_grid["grid_id"] == grid_id, 'side'].values[0] == "open_long":
                order_to_execute["type"] = "OPEN_LONG_ORDER"
            elif df_grid.loc[df_grid["grid_id"] == grid_id, 'side'].values[0] == "close_long":
                order_to_execute["type"] = "CLOSE_LONG_ORDER"
            elif df_grid.loc[df_grid["grid_id"] == grid_id, 'side'].values[0] == "open_short":
                order_to_execute["type"] = "OPEN_SHORT_ORDER"
            elif df_grid.loc[df_grid["grid_id"] == grid_id, 'side'].values[0] == "close_short":
                order_to_execute["type"] = "CLOSE_SHORT_ORDER"
            order_to_execute["price"] = df_grid.loc[df_grid["grid_id"] == grid_id, 'position'].values[0]
            order_to_execute["grid_id"] = grid_id
            lst_order.append(order_to_execute)

        sorting_order = ['OPEN_LONG_ORDER', 'OPEN_SHORT_ORDER', 'CLOSE_LONG_ORDER', 'CLOSE_SHORT_ORDER']
        sorted_list = sorted(lst_order, key=lambda x: sorting_order.index(x['type']))

        return sorted_list

    def set_to_pending_execute_order(self, symbol, lst_order_to_execute):
        df = self.grid[symbol]
        for placed_order in lst_order_to_execute:
            df.loc[df['grid_id'] == placed_order["grid_id"], 'status'] = 'pending'
            df.loc[df['grid_id'] == placed_order["grid_id"], 'orderId'] = ''

    def clear_orderId(self, symbol, grid_id):
        df = self.grid[symbol]
        df.loc[df['orderId'] == grid_id, 'orderId'] = ""

    def print_grid(self):
        for symbol in self.lst_symbols:
            df_grid = self.grid[symbol]
            print(df_grid.to_string(index=False))

    def update_nb_open_positions(self, symbol, df_open_positions, buying_size):
        self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'size'] = buying_size
        if len(df_open_positions) > 0:
            filtered_df = df_open_positions[(df_open_positions['symbol'] == symbol) & (df_open_positions['holdSide'] == "long")]
            self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_previous_open_positions'] = self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_open_positions'].values[0]
            if len(filtered_df) > 0:
                sum_available_position = filtered_df['total'].sum() if not filtered_df.empty else 0
                # sum_available_position = sum_available_position / filtered_df['leverage'].iloc[0]    # Modif CEDE
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'positions_size'] = sum_available_position
                leverage = filtered_df['leverage'].iloc[0]
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'leverage'] = leverage
                # nb_open_positions = int(sum_available_position / buying_size / leverage)
                nb_open_positions = int(sum_available_position / buying_size)
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_open_positions'] = nb_open_positions
                previous_nb_open_positions = self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_previous_open_positions'].values[0]
                self.df_nb_open_positions['previous_nb_open_positions'] = previous_nb_open_positions
                self.diff_position = nb_open_positions - previous_nb_open_positions
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'diff_position'] = self.diff_position
            else:
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'positions_size'] = 0
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_open_positions'] = 0
        else:
            self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'positions_size'] = 0
            self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_open_positions'] = 0
            self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'leverage'] = 0
            previous_nb_open_positions = self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_previous_open_positions'].values[0]
            nb_open_positions = 0
            self.diff_position = nb_open_positions - previous_nb_open_positions
            self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'diff_position'] = self.diff_position

    def get_nb_open_positions(self, symbol):
        row = self.df_nb_open_positions[self.df_nb_open_positions['symbol'] == symbol]
        if not row.empty:
            return row['nb_open_positions'].iloc[0]
        else:
            return 0

    def filter_lst_close_execute_order(self, symbol, lst_order_to_execute):
        nb_open_positions = self.get_nb_open_positions(symbol)
        df = self.grid[symbol]
        df_filtered = df[(df['status'] == 'engaged') & (df['side'] == 'close_long')]
        nb_close_position_already_open = len(df_filtered)
        nb_selected_to_be_open = nb_open_positions - nb_close_position_already_open

        if nb_selected_to_be_open < 0:
            # This case only happen when the limit order is triggered during the grid process
            print("################### GRID WARNING NEGATIVE SELECTED POSITIONS TO OPEN ###################")
            nb_selected_to_be_open = 0

        filtered_orders = []
        # Filter OPEN orders
        for order in lst_order_to_execute:
            if order["type"] in ["OPEN_LONG_ORDER", "OPEN_SHORT_ORDER"]\
                    and self.filter_open_control_multi_position(order["grid_id"]):
                filtered_orders.append(order)
        # Filter CLOSE orders and sort them by price
        close_orders = sorted(
            (order for order in lst_order_to_execute if order["type"] in ["CLOSE_LONG_ORDER", "CLOSE_SHORT_ORDER"] and self.filter_close_limit_order(order["grid_id"])),
            key=lambda x: x["price"]
        )

        if self.diff_close_position < 0 and self.diff_open_position < 0:
            grid_trend_msg = "UP / DOWN HIGH VOLATILITY "
            self.trend = "VOLATILITY"
            nb_selected_to_be_open = abs(self.diff_open_position)
            if len(close_orders) > 0:
                first_to_close = close_orders[0]["grid_id"]
                below_first_to_close = first_to_close - 1
                if df.loc[df["grid_id"] == below_first_to_close, "side"].values[0] == "close_long":
                    filtered_orders.extend(close_orders[:nb_selected_to_be_open])
                else:
                    filtered_orders.extend(close_orders[1:(nb_selected_to_be_open+1)])
        # Append a subset of CLOSE orders based on the number of open positions
        elif self.trend == "DOWN":
            grid_trend_msg = "DOWN -> " + " nb_selected_to_be_open : " + str(nb_selected_to_be_open)
            filtered_orders.extend(close_orders[1:(nb_selected_to_be_open + 1)])
        elif self.trend == "UP":
            grid_trend_msg = "UP -> " + " nb_selected_to_be_open : " + str(nb_selected_to_be_open)
            filtered_orders.extend(close_orders[:nb_selected_to_be_open])
        elif self.trend == "FLAT":
            grid_trend_msg = "FLAT ->" + " nb_selected_to_be_open : " + str(nb_selected_to_be_open)
            filtered_orders.extend(close_orders[:nb_selected_to_be_open])

        if not self.zero_print:
            print(grid_trend_msg)

        self.set_grid_positions_to_on_hold(lst_order_to_execute, filtered_orders)

        return filtered_orders

    def set_on_hold_from_grid_id(self, symbol, grid_id):
        df = self.grid[symbol]
        df.loc[df['grid_id'] == grid_id, 'status'] = 'on_hold'

    def set_grid_positions_to_on_hold(self, lst_pending, lst_filtered):
        resulted_lst = [element for element in lst_pending if element not in lst_filtered]
        for order in resulted_lst:
            self.set_on_hold_from_grid_id(order["symbol"], order["grid_id"])

    def normalize_grid_price(self, symbol, pricePlace, priceEndStep):
        df = self.grid[symbol]
        df['position'] = df['position'].apply(lambda x: self.normalize_price(x, pricePlace, priceEndStep))
        if not self.zero_print:
            print("grid price normalized: ", df['position'].tolist())

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

    def init_control_multi_position(self):
        multi_position = {}
        multi_position['previous_close_long'] = []
        multi_position['new_close_long'] = []
        multi_position['previous_open_long'] = []
        multi_position['new_open_long'] = []
        df_multi_position_status = pd.DataFrame(columns=['open_long', 'close_long'])
        df_multi_position_status['open_long'] = []
        df_multi_position_status['close_long'] = []
        multi_position['df_multi_position_status'] = df_multi_position_status
        multi_position['lst_open_long'] = []
        multi_position['lst_close_long'] = []

        return multi_position

    def update_control_multi_position(self, symbol, df_current_state, df_open_positions):
        self.control_multi_position['previous_close_long'] = self.control_multi_position['new_close_long']
        self.control_multi_position['previous_open_long'] = self.control_multi_position['new_open_long']

        self.control_multi_position['new_close_long'] = df_current_state[df_current_state['side'] == 'close_long']['gridId'].tolist()
        self.control_multi_position['new_open_long'] = df_current_state[df_current_state['side'] == 'open_long']['gridId'].tolist()

        condition_symbol = df_current_state['symbol'] == symbol
        condition_open_long = df_current_state['side'] == 'open_long'
        condition_close_long = df_current_state['side'] == 'close_long'
        df_filtered_open_long = df_current_state[condition_symbol & condition_open_long]
        lst_open_long = df_filtered_open_long['gridId'].tolist()
        df_filtered_close_long = df_current_state[condition_symbol & condition_close_long]
        lst_close_long = df_filtered_close_long['gridId'].tolist()

        lst_open_long_triggered = [element for element in self.control_multi_position['previous_open_long'] if element not in self.control_multi_position['new_open_long']]
        self.control_multi_position['lst_open_long'] = self.control_multi_position['lst_open_long'] + lst_open_long_triggered
        lst_tmp = self.control_multi_position['lst_open_long'].copy()
        for pos in lst_tmp:
            if not(pos in lst_open_long):
                self.control_multi_position['lst_open_long'].remove(pos)
        self.control_multi_position['lst_open_long'] = list(set(self.control_multi_position['lst_open_long']))

        lst_new_close_long_order = [element for element in self.control_multi_position['new_close_long'] if element not in self.control_multi_position['previous_close_long']]
        self.control_multi_position['lst_close_long'] = self.control_multi_position['lst_close_long'] + lst_new_close_long_order
        lst_tmp = self.control_multi_position['lst_close_long'].copy()
        for pos in lst_tmp:
            if not(pos in lst_close_long):
                self.control_multi_position['lst_close_long'].remove(pos)
        self.control_multi_position['lst_close_long'] = list(set(self.control_multi_position['lst_close_long']))

        lst_close_long_triggered = [element for element in self.control_multi_position['previous_close_long'] if element not in self.control_multi_position['new_close_long']]
        if len(lst_close_long_triggered) > 0:
            # Filter rows based on the values to drop and negate the condition with ~
            self.control_multi_position['df_multi_position_status'] = self.control_multi_position['df_multi_position_status'][~self.control_multi_position['df_multi_position_status']['close_long'].isin(lst_close_long_triggered)]

        if self.lst_limit_order_missing:
            self.control_multi_position['lst_open_long'].extend(
                [missing['grid_id'] for missing in self.lst_limit_order_missing if missing['side'] == 'open_long']
            )
            self.control_multi_position['lst_close_long'].extend(
                [missing['grid_id'] for missing in self.lst_limit_order_missing if missing['side'] == 'close_long']
            )

        df = self.grid[symbol]
        if df["grid_id"].max() in self.control_multi_position['lst_open_long']:
            self.control_multi_position['lst_open_long'].remove(df["grid_id"].max())

        if len(self.control_multi_position['lst_close_long']) == len(self.control_multi_position['lst_open_long']):
            # Loop to extend the DataFrame in each iteration
            lst_open_long_tmp = self.control_multi_position['lst_open_long'].copy()
            lst_close_long_tmp = self.control_multi_position['lst_close_long'].copy()
            for open_val, close_val in zip(lst_open_long_tmp, lst_close_long_tmp):
                if not ((self.control_multi_position['df_multi_position_status']['open_long'] == open_val)
                        & (self.control_multi_position['df_multi_position_status']['close_long'] == close_val)).any():
                    # Append a new row self.control_multi_position['df_multi_position_status'] values from the lists
                    try:
                        new_row_index = len(self.control_multi_position['df_multi_position_status'])
                        self.control_multi_position['df_multi_position_status'].loc[new_row_index] = {'open_long': open_val,
                                                                                                      'close_long': close_val}
                    except:
                        print('open_long', open_val)  # CEDE DEBUG
                        print('close_long', close_val)  # CEDE DEBUG
                        print('self.control_multi_position[df_multi_position_status]',
                              self.control_multi_position['df_multi_position_status'])  # CEDE DEBUG
                        exit(0)

                    # Remove the elements from the lists
                    if (len(self.control_multi_position['lst_open_long']) > 0) \
                            and (open_val in self.control_multi_position['lst_open_long']):
                        self.control_multi_position['lst_open_long'].remove(open_val)
                    if (len(self.control_multi_position['lst_close_long']) > 0) \
                            and (close_val in self.control_multi_position['lst_close_long']):
                        self.control_multi_position['lst_close_long'].remove(close_val)

                    if not self.zero_print:
                        print(open_val, " and ", close_val, " dropped from list")
        else:
            if len(self.control_multi_position['lst_close_long']) > 0:
                lst_tmp = self.control_multi_position['lst_close_long'].copy()
                for pos in lst_tmp:
                    open_val = pos - 1
                    while open_val in self.control_multi_position['df_multi_position_status']['open_long'].tolist():
                        open_val -= 1
                    if not(pos in self.control_multi_position['df_multi_position_status']['close_long'].tolist()):
                        try:
                            new_row_index = len(self.control_multi_position['df_multi_position_status'])
                            self.control_multi_position['df_multi_position_status'].loc[new_row_index] = {'open_long': open_val, 'close_long': pos}
                        except:
                            print('open_long', open_val)  # CEDE DEBUG
                            print('close_long', pos)  # CEDE DEBUG
                            print('self.control_multi_position[df_multi_position_status]',
                                  self.control_multi_position['df_multi_position_status'])  # CEDE DEBUG
                            exit(0)
                    if (len(self.control_multi_position['lst_open_long']) > 0) \
                            and (open_val in self.control_multi_position['lst_open_long']):
                        self.control_multi_position['lst_open_long'].remove(open_val)
                    if (len(self.control_multi_position['lst_close_long']) > 0) \
                            and (pos in self.control_multi_position['lst_close_long']):
                        self.control_multi_position['lst_close_long'].remove(pos)

        # reorder df_multi_position_status
        if len(self.control_multi_position['df_multi_position_status']) > 0:
            self.control_multi_position['df_multi_position_status']['open_long'] = sorted(self.control_multi_position['df_multi_position_status']['open_long'].tolist())
            self.control_multi_position['df_multi_position_status']['close_long'] = sorted(self.control_multi_position['df_multi_position_status']['close_long'].tolist())

        self.diff_close_position = len(self.control_multi_position['new_close_long']) - len(self.control_multi_position['previous_close_long'])
        self.diff_open_position = len(self.control_multi_position['new_open_long']) - len(self.control_multi_position['previous_open_long'])

        total_symbol = 0
        if not df_open_positions.empty:
            if symbol in df_open_positions["symbol"].values:
                df_open_positions_filter = df_open_positions[df_open_positions["symbol"] == symbol]
                if not df_open_positions_filter.empty:
                    total_symbol = df_open_positions_filter["total"].sum()

        if (total_symbol == 0) \
                and (len(self.control_multi_position['df_multi_position_status']) > 0):
            self.control_multi_position['df_multi_position_status'] = pd.DataFrame(columns=['open_long', 'close_long'])

        if not self.zero_print:
            print('lst_close_short_triggered: ', lst_close_long_triggered)
            print('global lst_close_long: ', self.control_multi_position['lst_close_long'])
            print('global lst_open_long: ', self.control_multi_position['lst_open_long'])
            print('df_multi_position_status: ')
            print(self.control_multi_position['df_multi_position_status'].to_string(index=False))

    def filter_open_control_multi_position(self, open_grid_id):
        if open_grid_id in self.control_multi_position['lst_open_long'] \
                or open_grid_id in self.control_multi_position['df_multi_position_status']['open_long'].tolist():
            if not self.zero_print:
                print("filter_open_control_multi_position - grid id: ", open_grid_id)
            return False
        else:
            return True

    def filter_close_limit_order(self, open_grid_id):
        # CEDE NOT IN USE YET
        return True

    def get_grid_info(self, symbol):
        df = self.grid[symbol]
        dct_status_info = {}

        # Count of rows with side == 'close_long' and status == 'engaged'
        dct_status_info['nb_limit_close'] = len(df[(df['side'] == 'close_long') & (df['status'] == 'engaged')])

        # Count of rows with side == 'open_long' and status == 'engaged'
        dct_status_info['nb_limit_open'] = len(df[(df['side'] == 'open_long') & (df['status'] == 'engaged')])

        dct_status_info['nb_position'] = self.get_nb_open_positions(symbol)
        dct_status_info['price'] = self.current_price
        dct_status_info['grid_position'] = self.grid_position

        dct_status_info['previous_grid_uniq_position'] = self.previous_grid_uniq_position
        dct_status_info['grid_uniq_position'] = self.grid_uniq_position
        dct_status_info['grid_move'] = self.grid_move

        return dct_status_info

    def dct_change_status(self, dct_info):
        if self.dct_info == None:
            self.dct_info = dct_info
            return False
        self.dct_info = dct_info

        if self.grid_move:
            self.grid_move = False
            return True
        else:
            return False

    def dct_status_info_to_txt(self, dct_info, symbol):
        msg = "# GRID LONG: " + "\n"
        msg += "RANGE FROM: " + str(self.grid_high) + " TO " + str(self.grid_low) + "\n"
        msg += "NB_GRID: " + str(self.nb_grid) + "\n"
        msg += "# GRID POSITION: " + "\n"
        msg += "**POSITION: " + str(self.grid_uniq_position) + "**\n"
        msg += "**PREV POSITION: " + str(self.previous_grid_uniq_position) + "**\n"
        if ((dct_info['grid_position'][0] >= self.nb_grid)
                and (dct_info['grid_position'][1] >= self.nb_grid)):
            msg += "**ABOVE GRID**\n"
        elif ((dct_info['grid_position'][0] <= 0)
              and (dct_info['grid_position'][1] <= 0)):
            msg += "**BELOW GRID**\n"
        else:
            msg += "**position: " + str(dct_info['grid_position'][0]) + " / " + str(dct_info['grid_position'][1]) + "**\n"
        msg += "nb_limit_close: " + str(dct_info['nb_limit_close']) + "\n"
        msg += "nb_limit_open: " + str(dct_info['nb_limit_open']) + "\n"
        msg += "nb_open_position: " + str(dct_info['nb_position']) + "\n"
        msg += "nb_total_opened_positions: " + str(self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_total_opened_positions'].values[0]) + "\n"
        msg += "nb_total_closed_positions: " + str(self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_total_closed_positions'].values[0]) + "\n"

        if len(self.previous_grid_position) == 2:
            msg += "# TREND:" + "\n"
            if self.previous_grid_position[0] > dct_info['grid_position'][0] and self.previous_grid_position[1] > dct_info['grid_position'][1]:
                msg += "DOWN" + "\n"
            elif self.previous_grid_position[0] < dct_info['grid_position'][0] and self.previous_grid_position[1] < dct_info['grid_position'][1]:
                msg += "UP" + "\n"
            elif self.previous_grid_position[0] == dct_info['grid_position'][0] and self.previous_grid_position[1] == dct_info['grid_position'][1]:
                msg += "FLAT / NO DIRECTION" + "\n"
            else:
                msg += "**ERROR DIRECTION" + "**\n"
                df = self.grid[symbol]
                lst_filtered_on_edge = df[df['on_edge']]['grid_id'].tolist()
                if (len(lst_filtered_on_edge) > 0) \
                        or self.on_edge:
                    msg += "**PRICE ON EDGE" + "**\n"
                    msg += "**VERIFIED" + "**\n"
                    msg += "lst_filtered_on_edge: " + ' '.join(map(str, lst_filtered_on_edge)) + "\n"
                else:
                    msg += "**WARNING - PRICE ON EDGE" + "**\n"
                    msg += "**NOT VERIFIED" + "**\n"
                    msg += "lst_filtered_on_edge empty" + ' '.join(map(str, lst_filtered_on_edge)) + "\n"

        if self.diff_position != 0:
            # msg += "# DIFF:" + "\n"
            if self.diff_position > 0:
                msg += "DIFF OPENED POSITION: " + str(self.diff_position) + "\n"
            elif self.diff_position < 0:
                msg += "DIFF CLOSED POSITION: " + str(self.diff_position) + "\n"
            elif self.diff_position == 0:
                msg += "DIFF POSITION: " + str(self.diff_position) + "\n"

        if (self.diff_close_position != 0) \
                and (self.diff_open_position != 0):
            msg += "# WARNING HIGH VOLATILITY:" + "\n"
            msg += "TREND: UP/DOWN" + "\n"
            # msg += "DIFF OPENED POSITION: " + str(self.diff_open_position) + "\n"
            # msg += "DIFF CLOSED POSITION: " + str(self.diff_close_position) + "\n"

        self.previous_grid_position = dct_info['grid_position']

        return msg.upper()

    def get_grid_as_str(self, symbol):
        df_grid = self.grid[symbol]
        return df_grid.to_string(index=False)

    def get_grid(self, symbol, round):
        df = self.grid[symbol].copy()
        df['round'] = round

        column_to_move = 'round'
        first_column = df.pop(column_to_move)
        df.insert(0, column_to_move, first_column)

        return df