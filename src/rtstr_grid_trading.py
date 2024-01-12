from . import rtdp, rtstr, rtctrl
import pandas as pd
import numpy as np

import math

class StrategyGridTrading(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        self.zero_print = False
        self.grid = GridPosition(self.lst_symbols, self.grid_high, self.grid_low, self.nb_grid, self.zero_print)

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
        return "StrategyGridTrading"

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
        df_current_states = current_state["open_orders"]
        df_open_positions = current_state["open_positions"]
        df_price = current_state["prices"]

        lst_order_to_execute = []

        for symbol in self.lst_symbols:
            df_current_state = self.grid.set_current_orders_price_to_grid(symbol, df_current_states)
            buying_size = self.get_grid_buying_size(symbol)
            self.grid.update_control_multi_position(df_current_states)
            self.grid.update_nb_open_positions(symbol, df_open_positions, buying_size)
            self.grid.update_pending_status_from_current_state(symbol, df_current_state)

            price_for_symbol = df_price.loc[df_price['symbols'] == symbol, 'values'].values[0]
            self.grid.update_grid_side(symbol, price_for_symbol, df_current_state)

            df_filtered_current_state = df_current_state[df_current_state['symbol'] == symbol]
            self.grid.cross_check_with_current_state(symbol, df_filtered_current_state)

            lst_order_to_execute = self.grid.get_order_list(symbol, buying_size, df_current_state)
            self.grid.set_to_pending_execute_order(symbol, lst_order_to_execute)
            lst_order_to_execute = self.grid.filter_lst_close_execute_order(symbol, lst_order_to_execute)

        # CEDE DEBUG
        if not self.zero_print:
            df_sorted = df_current_state.sort_values(by='price')
            print("##########################################################################################")
            print("current_state: \n", df_sorted.to_string(index=False))
            print("open_positions: \n", df_open_positions.to_string(index=False))
            print("price: \n", df_price.to_string(index=False))
            lst_order_to_print = []
            for order in lst_order_to_execute:
                lst_order_to_print.append((order["grid_id"], order["price"], order["type"]))
            print("order list: \n", lst_order_to_print)
            self.grid.print_grid()
            print("##########################################################################################")

        return lst_order_to_execute

    def set_normalized_grid_price(self, lst_symbol_plc_endstp):
        for price_plc in lst_symbol_plc_endstp:
            symbol = price_plc['symbol']
            pricePlace = price_plc['pricePlace']
            priceEndStep = price_plc['priceEndStep']
            self.grid.normalize_grid_price(symbol, pricePlace, priceEndStep)

    def activate_grid(self, current_state):
        df_prices = current_state["prices"]
        lst_buying_market_order = []
        for symbol in self.lst_symbols:
            lst_buying_market_order = []
            buying_size = self.get_grid_buying_size(symbol)
            price = df_prices.loc[df_prices['symbols'] == symbol, 'values'].values[0]
            order = self.grid.get_buying_market_order(symbol, buying_size, price)
            lst_buying_market_order.append(order)
        return lst_buying_market_order

class GridPosition():
    def __init__(self, lst_symbols, grid_high, grid_low, nb_grid, debug_mode=True):
        self.grid_high = grid_high
        self.grid_low = grid_low
        self.nb_grid = nb_grid
        self.lst_symbols = lst_symbols

        self.zero_print = debug_mode
        self.trend = "FLAT"

        self.control_multi_position = self.init_control_multi_position()

        self.df_nb_open_positions = pd.DataFrame(columns=['symbol', 'size', 'positions_size', 'nb_open_positions'])
        self.df_nb_open_positions['symbol'] = self.lst_symbols
        self.df_nb_open_positions['size'] = 0
        self.df_nb_open_positions['positions_size'] = 0
        self.df_nb_open_positions['nb_open_positions'] = 0

        # Create a list with nb_grid split between high and low
        self.lst_grid_values = np.linspace(self.grid_high, self.grid_low, self.nb_grid + 1, endpoint=True).tolist()
        if not self.zero_print:
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
        for grid_id in lst_grid_id:
            side = df.loc[df['grid_id'] == grid_id, "side"].values[0]
            if grid_id in df_current_state["gridId"].tolist():
                if side == df_current_state.loc[df_current_state["gridId"] == grid_id, 'side'].values[0]:
                    df.loc[df['grid_id'] == grid_id, 'status'] = 'engaged'
                    df.loc[df['grid_id'] == grid_id, 'orderId'] = df_current_state.loc[df_current_state["gridId"] == grid_id, 'orderId'].values[0]
                else:
                    print("GRID ERROR - open_long vs open_short - grid id: ", grid_id)
            else:
                print("GRID ERROR - limit order failed - grid_id missing: ", grid_id)
                self.lst_limit_order_missing.append(grid_id)

    def update_grid_side(self, symbol, position, df_current_state):
        df = self.grid[symbol]
        if df['previous_side'].all() == False:
            b_init = True
            previous_open_long_count = 0
            previous_close_long_count = 0
        else:
            b_init = False
            action_counts = df['previous_side'].value_counts()
            previous_open_long_count = action_counts.get('open_long', 0)
            previous_close_long_count = action_counts.get('close_long', 0)

        df['on_edge'] = False
        df['previous_side'] = df['side']
        # Set the 'side' column based on conditions
        df.loc[df['position'] > position, 'side'] = 'close_long'
        df.loc[df['position'] < position, 'side'] = 'open_long'

        if (df['position'] == position).any():
            print('PRICE ON GRID EDGE - CROSSING OR NOT CROSSING')
            df.loc[df['position'] == position, 'on_edge'] = True
            df.loc[df['position'] == position, 'side'] = df.loc[df['position'] == position, 'previous_side'].values[0]

        action_counts = df['side'].value_counts()
        open_long_count = action_counts.get('open_long', 0)
        close_long_count = action_counts.get('close_long', 0)

        if not b_init:
            if (open_long_count > previous_open_long_count):
                self.trend = "UP"
            elif (close_long_count > previous_close_long_count):
                self.trend = "DOWN"
            elif (open_long_count == previous_open_long_count) \
                    and (close_long_count == previous_close_long_count):
                self.trend = "FLAT"

        # Compare if column1 and column2 are the same
        df['changes'] = df['previous_side'] != df['side']

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

    def cross_check_with_current_state(self, symbol, df_current_state):
        df_grid = self.grid[symbol]
        df_grid['cross_checked'] = False

        # Iterate over every row using iterrows()
        # Compare values from both DataFrames using iterrows
        for index_grid, row_grid in df_grid.iterrows():
            for index_c_state, row_c_state in df_current_state.iterrows():
                if (row_grid['grid_id'] == row_c_state['gridId']) \
                        or (row_grid['position'] == row_c_state['price']):
                    if row_grid['side'] == row_c_state['side'] \
                            and row_grid['orderId'] == row_c_state['orderId']:
                        df_grid.loc[index_grid, 'cross_checked'] = True

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

        lst_filtered_on_edge = df_grid[df_grid['on_edge']]['grid_id'].tolist()

        lst_order_grid_id = df_filtered_changes['grid_id'].tolist() \
                            + df_filtered_checked['grid_id'].tolist() \
                            + df_filtered_pending['grid_id'].tolist()
        lst_order_grid_id = list(set(lst_order_grid_id))

        if len(lst_filtered_on_edge) > 0:
            lst_order_grid_id = [item for item in lst_filtered_on_edge if item not in lst_order_grid_id]

        lst_price_existing_orders = df_current_order['price'].tolist()
        lst_gridId_existing_orders = []
        # for price in lst_price_existing_orders:
        #     lst_gridId_existing_orders.append(df_grid.loc[df_grid['position'] == price, 'grid_id'].values[0])

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
        # ['symbol', 'size', 'positions_size', 'nb_open_positions']
        self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'size'] = buying_size
        if len(df_open_positions) > 0:
            filtered_df = df_open_positions[df_open_positions['symbol'] == symbol]
            if len(filtered_df):
                # sum_available_position = filtered_df['available'].sum() if not filtered_df.empty else 0
                sum_available_position = filtered_df['total'].sum() if not filtered_df.empty else 0
                sum_available_position = sum_available_position / filtered_df['leverage'].iloc[0]
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'positions_size'] = sum_available_position
                nb_open_positions = int(sum_available_position / buying_size)
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_open_positions'] = nb_open_positions
            else:
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'positions_size'] = 0
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_open_positions'] = 0
        else:
            self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'positions_size'] = 0
            self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_open_positions'] = 0

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
            (order for order in lst_order_to_execute if order["type"] in ["CLOSE_LONG_ORDER", "CLOSE_SHORT_ORDER"] and self.filter_open_limit_order_missing(order["grid_id"])),
            key=lambda x: x["price"]
        )
        # Append a subset of CLOSE orders based on the number of open positions
        if self.trend == "DOWN":
            grid_trend_msg = "DOWN -> " + "nb_selected_to_be_open : " + str(nb_selected_to_be_open)
            filtered_orders.extend(close_orders[1:(nb_selected_to_be_open+1)])
        elif self.trend == "UP":
            grid_trend_msg = "UP -> " + "nb_selected_to_be_open : " + str(nb_selected_to_be_open)
            filtered_orders.extend(close_orders[:nb_selected_to_be_open])
        elif self.trend == "FLAT":
            grid_trend_msg = "FLAT ->" + "nb_selected_to_be_open : " + str(nb_selected_to_be_open)
            filtered_orders.extend(close_orders[:nb_selected_to_be_open])
        else:
            grid_trend_msg = "ERROR -> NO TREND"

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

    def update_control_multi_position(self, df_current_state):
        self.control_multi_position['previous_close_long'] = self.control_multi_position['new_close_long']
        self.control_multi_position['previous_open_long'] = self.control_multi_position['new_open_long']

        self.control_multi_position['new_close_long'] = df_current_state[df_current_state['side'] == 'close_long']['gridId'].tolist()
        self.control_multi_position['new_open_long'] = df_current_state[df_current_state['side'] == 'open_long']['gridId'].tolist()

        lst_open_long_triggered = [element for element in self.control_multi_position['previous_open_long'] if element not in self.control_multi_position['new_open_long']]
        self.control_multi_position['lst_open_long'] = self.control_multi_position['lst_open_long'] + lst_open_long_triggered
        self.control_multi_position['lst_open_long'] = list(set(self.control_multi_position['lst_open_long']))

        lst_new_close_long_order = [element for element in self.control_multi_position['new_close_long'] if element not in self.control_multi_position['previous_close_long']]
        self.control_multi_position['lst_close_long'] = self.control_multi_position['lst_close_long'] + lst_new_close_long_order
        self.control_multi_position['lst_close_long'] = list(set(self.control_multi_position['lst_close_long']))

        lst_close_long_triggered = [element for element in self.control_multi_position['previous_close_long'] if element not in self.control_multi_position['new_close_long']]
        if len(lst_close_long_triggered) > 0:
            # Filter rows based on the values to drop and negate the condition with ~
            self.control_multi_position['df_multi_position_status'] = self.control_multi_position['df_multi_position_status'][~self.control_multi_position['df_multi_position_status']['close_long'].isin(lst_close_long_triggered)]

        if not self.zero_print:
            print('lst_close_long_triggered: ', lst_close_long_triggered)
            print('global lst_close_long: ', self.control_multi_position['lst_close_long'])
            print('global lst_open_long: ', self.control_multi_position['lst_open_long'])

        if (len(self.control_multi_position['lst_open_long']) == 0) and (len(self.control_multi_position['lst_close_long']) == 1):
            # Open order have been triggered before being recorded
            lst_find_gost_order = self.control_multi_position['new_open_long']
            lst_previous_order = self.control_multi_position['lst_open_long'] + self.control_multi_position['df_multi_position_status']['open_long'].tolist()
            lst_result = [element for element in lst_find_gost_order if element not in lst_previous_order]
            lst_result = sorted(lst_result, reverse=True)
            print("########################## lst_result: ", lst_result)
            if len(lst_result) > 0:
                self.control_multi_position['lst_open_long'] = list(lst_result[:1])

        if len(self.control_multi_position['lst_close_long']) == len(self.control_multi_position['lst_open_long']):
            # Loop to extend the DataFrame in each iteration
            lst_open_long_tmp = self.control_multi_position['lst_open_long'].copy()
            lst_close_long_tmp = self.control_multi_position['lst_close_long'].copy()
            for open_val, close_val in zip(lst_open_long_tmp, lst_close_long_tmp):
                if not ((self.control_multi_position['df_multi_position_status']['open_long'] == open_val)
                        & (self.control_multi_position['df_multi_position_status']['close_long'] == close_val)).any():
                    # Append a new row self.control_multi_position['df_multi_position_status'] values from the lists
                    self.control_multi_position['df_multi_position_status'] = self.control_multi_position['df_multi_position_status'].append({'open_long': open_val,
                                                                                                                                              'close_long': close_val},
                                                                                                                                             ignore_index=True)
                    # Remove the elements from the lists
                    self.control_multi_position['lst_open_long'].remove(open_val)
                    self.control_multi_position['lst_close_long'].remove(close_val)
                    if not self.zero_print:
                        print(open_val, " and ", close_val, " dropped from list")

        if not self.zero_print:
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

    def filter_open_limit_order_missing(self, open_grid_id):
        if open_grid_id in self.lst_limit_order_missing:
            if not self.zero_print:
                print("filter_open_limit_order_missing - grid id: ", open_grid_id)
            return False
        else:
            return True
