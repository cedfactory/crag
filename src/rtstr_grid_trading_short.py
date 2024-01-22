from . import rtdp, rtstr, rtctrl
import pandas as pd
import numpy as np

import math

class StrategyGridTradingShort(rtstr.RealTimeStrategy):

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
            self.grid.update_grid_side(symbol, price_for_symbol)

            df_filtered_current_state = df_current_state[df_current_state['symbol'] == symbol]
            self.grid.cross_check_with_current_state(symbol, df_filtered_current_state)

            lst_order_to_execute = self.grid.get_order_list(symbol, buying_size, df_current_state)
            self.grid.set_to_pending_execute_order(symbol, lst_order_to_execute)
            lst_order_to_execute = self.grid.filter_lst_close_execute_order(symbol, lst_order_to_execute)

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
                print("None Print")
                return None

class GridPosition():
    def __init__(self, lst_symbols, grid_high, grid_low, nb_grid, debug_mode=True):
        self.grid_high = grid_high
        self.grid_low = grid_low
        self.nb_grid = nb_grid
        self.lst_symbols = lst_symbols
        self.str_lst_symbol = ' '.join(map(str, lst_symbols))

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

        self.df_nb_open_positions = pd.DataFrame(columns=['symbol', 'size', 'positions_size', 'nb_open_positions'])
        self.df_nb_open_positions['symbol'] = self.lst_symbols
        self.df_nb_open_positions['size'] = 0
        self.df_nb_open_positions['positions_size'] = 0
        self.df_nb_open_positions['nb_open_positions'] = 0
        self.df_nb_open_positions['nb_total_opened_positions'] = 0
        self.df_nb_open_positions['nb_total_closed_positions'] = 0
        self.df_nb_open_positions['nb_previous_open_positions'] = 0

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
                    # CEDE: This should not be triggered
                    print("GRID ERROR - open_short vs open_short - grid id: ", grid_id)
            else:
                # CEDE: Executed when high volatility is triggering an limit order just after being raised
                #       and before being recorded by the strategy in the grid process structure
                missing_order = {}
                missing_order['grid_id'] = grid_id
                missing_order['side'] = df.loc[df['grid_id'] == grid_id, 'side'].values[0]
                self.lst_limit_order_missing.append(missing_order)
                print("GRID ERROR - limit order failed - grid_id missing: ", grid_id, ' side: ', missing_order['side'])

    def update_grid_side(self, symbol, position):
        df = self.grid[symbol]
        self.current_price = position

        df['on_edge'] = False
        df['previous_side'] = df['side']
        # Set the 'side' column based on conditions
        df.loc[df['position'] < position, 'side'] = 'close_short'
        df.loc[df['position'] > position, 'side'] = 'open_short'

        if (df['position'] == position).any():
            print('PRICE ON GRID EDGE - CROSSING OR NOT CROSSING')
            df.loc[df['position'] == position, 'on_edge'] = True
            df.loc[df['position'] == position, 'side'] = df.loc[df['position'] == position, 'previous_side'].values[0]

        if self.diff_position == 0:
            self.trend = "FLAT"
        elif self.diff_position > 0:
            self.trend = "DOWN"
        elif self.diff_position < 0:
            self.trend = "UP"

        # Compare if column1 and column2 are the same
        df['changes'] = df['previous_side'] != df['side']

        higher_grid_id = df[df['position'] > self.current_price]['grid_id'].min()
        lower_grid_id = df[df['position'] < self.current_price]['grid_id'].max()
        self.grid_position = [higher_grid_id, lower_grid_id]

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
            order_to_execute["price"] = df_grid.loc[df_grid["grid_id"] == grid_id, 'position'].values[0]
            order_to_execute["grid_id"] = grid_id
            lst_order.append(order_to_execute)

        sorting_order = ['OPEN_SHORT_ORDER', 'OPEN_SHORT_ORDER', 'CLOSE_SHORT_ORDER', 'CLOSE_SHORT_ORDER']
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
            self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_previous_open_positions'] = self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_open_positions'].values[0]
            if len(filtered_df):
                # sum_available_position = filtered_df['available'].sum() if not filtered_df.empty else 0
                sum_available_position = filtered_df['total'].sum() if not filtered_df.empty else 0
                sum_available_position = sum_available_position / filtered_df['leverage'].iloc[0]
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'positions_size'] = sum_available_position
                nb_open_positions = int(sum_available_position / buying_size)
                self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_open_positions'] = nb_open_positions
                previous_nb_open_positions = self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_previous_open_positions'].values[0]
                self.diff_position = nb_open_positions - previous_nb_open_positions
                if self.diff_position > 0:
                    self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_total_opened_positions'] += self.diff_position
                else:
                    self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_total_closed_positions'] += abs(self.diff_position)
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
        df_filtered = df[(df['status'] == 'engaged') & (df['side'] == 'close_short')]
        nb_close_position_already_open = len(df_filtered)
        nb_selected_to_be_open = nb_open_positions - nb_close_position_already_open

        if nb_selected_to_be_open < 0:
            # This case only happen when the limit order is triggered during the grid process
            print("################### GRID WARNING NEGATIVE SELECTED POSITIONS TO OPEN ###################")
            nb_selected_to_be_open = 0

        filtered_orders = []
        # Filter OPEN orders
        for order in lst_order_to_execute:
            if order["type"] in ["OPEN_SHORT_ORDER", "OPEN_SHORT_ORDER"]\
                    and self.filter_open_control_multi_position(order["grid_id"]):
                filtered_orders.append(order)
        # Filter CLOSE orders and sort them by price
        close_orders = sorted(
            (order for order in lst_order_to_execute if order["type"] in ["CLOSE_SHORT_ORDER", "CLOSE_SHORT_ORDER"] and self.filter_close_limit_order(order["grid_id"])),
            key=lambda x: x["price"],
            reverse=True
        )

        if self.diff_close_position < 0 and self.diff_open_position < 0:
            grid_trend_msg = "UP / DOWN HIGH VOLATILITY "
            self.trend = "VOLATILITY"
            nb_selected_to_be_open = abs(self.diff_open_position)
            list_of_dicts = close_orders[:nb_selected_to_be_open]
            lst_close_order = [d['grid_id'] for d in list_of_dicts]
            if sorted(lst_close_order) != sorted(self.control_multi_position['lst_open_short']):
                filtered_orders.extend(close_orders[:nb_selected_to_be_open])
            else:
                filtered_orders.extend(close_orders[1:(nb_selected_to_be_open+1)])
        # Append a subset of CLOSE orders based on the number of open positions
        elif self.trend == "DOWN":
            grid_trend_msg = "DOWN -> " + " nb_selected_to_be_open : " + str(nb_selected_to_be_open)
            filtered_orders.extend(close_orders[1:(nb_selected_to_be_open+1)])
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
        order_to_execute["type"] = "OPEN_SHORT"   # OPEN_SHORT for short_grid
        order_to_execute["price"] = price
        order_to_execute["gross_size"] = size
        order_to_execute["grid_id"] = -1
        return order_to_execute

    def init_control_multi_position(self):
        multi_position = {}
        multi_position['previous_close_short'] = []
        multi_position['new_close_short'] = []
        multi_position['previous_open_short'] = []
        multi_position['new_open_short'] = []
        df_multi_position_status = pd.DataFrame(columns=['open_short', 'close_short'])
        df_multi_position_status['open_short'] = []
        df_multi_position_status['close_short'] = []
        multi_position['df_multi_position_status'] = df_multi_position_status
        multi_position['lst_open_short'] = []
        multi_position['lst_close_short'] = []

        return multi_position

    def update_control_multi_position(self, df_current_state):
        self.control_multi_position['previous_close_short'] = self.control_multi_position['new_close_short']
        self.control_multi_position['previous_open_short'] = self.control_multi_position['new_open_short']

        self.control_multi_position['new_close_short'] = df_current_state[df_current_state['side'] == 'close_short']['gridId'].tolist()
        self.control_multi_position['new_open_short'] = df_current_state[df_current_state['side'] == 'open_short']['gridId'].tolist()

        lst_open_short_triggered = [element for element in self.control_multi_position['previous_open_short'] if element not in self.control_multi_position['new_open_short']]
        self.control_multi_position['lst_open_short'] = self.control_multi_position['lst_open_short'] + lst_open_short_triggered
        self.control_multi_position['lst_open_short'] = list(set(self.control_multi_position['lst_open_short']))

        lst_new_close_short_order = [element for element in self.control_multi_position['new_close_short'] if element not in self.control_multi_position['previous_close_short']]
        self.control_multi_position['lst_close_short'] = self.control_multi_position['lst_close_short'] + lst_new_close_short_order
        self.control_multi_position['lst_close_short'] = list(set(self.control_multi_position['lst_close_short']))

        lst_close_short_triggered = [element for element in self.control_multi_position['previous_close_short'] if element not in self.control_multi_position['new_close_short']]
        if len(lst_close_short_triggered) > 0:
            # Filter rows based on the values to drop and negate the condition with ~
            self.control_multi_position['df_multi_position_status'] = self.control_multi_position['df_multi_position_status'][~self.control_multi_position['df_multi_position_status']['close_short'].isin(lst_close_short_triggered)]

        if self.lst_limit_order_missing:
            self.control_multi_position['lst_open_short'].extend(
                [missing['grid_id'] for missing in self.lst_limit_order_missing if missing['side'] == 'open_short']
            )
            self.control_multi_position['lst_close_short'].extend(
                [missing['grid_id'] for missing in self.lst_limit_order_missing if missing['side'] == 'close_short']
            )

        if not self.zero_print:
            print('lst_close_short_triggered: ', lst_close_short_triggered)
            print('global lst_close_short: ', self.control_multi_position['lst_close_short'])
            print('global lst_open_short: ', self.control_multi_position['lst_open_short'])

        if len(self.control_multi_position['lst_close_short']) == len(self.control_multi_position['lst_open_short']):
            # Loop to extend the DataFrame in each iteration
            lst_open_short_tmp = self.control_multi_position['lst_open_short'].copy()
            lst_close_short_tmp = self.control_multi_position['lst_close_short'].copy()
            for open_val, close_val in zip(lst_open_short_tmp, lst_close_short_tmp):
                if not ((self.control_multi_position['df_multi_position_status']['open_short'] == open_val)
                        & (self.control_multi_position['df_multi_position_status']['close_short'] == close_val)).any():
                    # Append a new row self.control_multi_position['df_multi_position_status'] values from the lists
                    self.control_multi_position['df_multi_position_status'] = self.control_multi_position['df_multi_position_status'].append({'open_short': open_val,
                                                                                                                                              'close_short': close_val},
                                                                                                                                             ignore_index=True)
                    # Remove the elements from the lists
                    self.control_multi_position['lst_open_short'].remove(open_val)
                    self.control_multi_position['lst_close_short'].remove(close_val)
                    if not self.zero_print:
                        print(open_val, " and ", close_val, " dropped from list")

        # reorder df_multi_position_status
        if len(self.control_multi_position['df_multi_position_status']) > 0:
            self.control_multi_position['df_multi_position_status']['open_short'] = sorted(self.control_multi_position['df_multi_position_status']['open_short'].tolist())
            self.control_multi_position['df_multi_position_status']['close_short'] = sorted(self.control_multi_position['df_multi_position_status']['close_short'].tolist())

        self.diff_close_position = len(self.control_multi_position['new_close_short']) - len(self.control_multi_position['previous_close_short'])
        self.diff_open_position = len(self.control_multi_position['new_open_short']) - len(self.control_multi_position['previous_open_short'])

        if not self.zero_print:
            print('df_multi_position_status: ')
            print(self.control_multi_position['df_multi_position_status'].to_string(index=False))

    def filter_open_control_multi_position(self, open_grid_id):
        if open_grid_id in self.control_multi_position['lst_open_short'] \
                or open_grid_id in self.control_multi_position['df_multi_position_status']['open_short'].tolist():
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
        """
        nb_limit_open = 
        nb_limit_close = 
        nb_position = 
        nb_total_closed = 
        price = 
        grid_position =
        """
        # Count of rows with side == 'close_short' and status == 'engaged'
        dct_status_info['nb_limit_close'] = len(df[(df['side'] == 'close_short') & (df['status'] == 'engaged')])

        # Count of rows with side == 'open_short' and status == 'engaged'
        dct_status_info['nb_limit_open'] = len(df[(df['side'] == 'open_short') & (df['status'] == 'engaged')])

        dct_status_info['nb_position'] = self.get_nb_open_positions(symbol)
        dct_status_info['price'] = self.current_price
        dct_status_info['grid_position'] = self.grid_position

        return dct_status_info

    def dct_change_status(self, dct_info):
        if self.dct_info == None:
            self.dct_info = dct_info
            return False
        self.dct_info = dct_info

        if self.diff_position != 0:
            return True
        else:
            return False

    def dct_status_info_to_txt(self, dct_info, symbol):
        msg = "SYMBOL: " + self.str_lst_symbol + "\n"
        msg += "symbol price: " + str(dct_info['price']) + "\n"
        msg += "GRID INFO: " + "\n"
        msg += "- GRID RANGE: " + str(self.grid_high) + " and " + str(self.grid_low) + "\n"
        msg += "- NB_GRID: " + str(self.nb_grid) + "\n"
        msg += "GRID POSITION: " + "\n"
        msg += "- grid_position: " + str(dct_info['grid_position'][0]) + " / " + str(dct_info['grid_position'][1]) + "\n"
        msg += "- nb_limit_close: " + str(dct_info['nb_limit_close']) + "\n"
        msg += "- nb_limit_open: " + str(dct_info['nb_limit_open']) + "\n"
        msg += "- nb_open_position: " + str(dct_info['nb_position']) + "\n"
        msg += "- nb_total_opened_positions: " + str(self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_total_opened_positions'].values[0]) + "\n"
        msg += "- nb_total_closed_positions: " + str(self.df_nb_open_positions.loc[self.df_nb_open_positions['symbol'] == symbol, 'nb_total_closed_positions'].values[0]) + "\n"


        if len(self.previous_grid_position) == 2:
            msg += "TREND:" + "\n"
            if self.previous_grid_position[0] > dct_info['grid_position'][0] and self.previous_grid_position[1] > dct_info['grid_position'][1]:
                msg += "- DOWN" + "\n"
            elif self.previous_grid_position[0] < dct_info['grid_position'][0] and self.previous_grid_position[1] < dct_info['grid_position'][1]:
                msg += "- UP" + "\n"
            elif self.previous_grid_position[0] == dct_info['grid_position'][0] and self.previous_grid_position[1] == dct_info['grid_position'][1]:
                msg += "- FLAT / NO DIRECTION" + "\n"
            else:
                msg += "- ERROR DIRECTION - PRICE ON GRID EDGE" + "\n"
                df = self.grid[symbol]
                lst_filtered_on_edge = df[df['on_edge']]['grid_id'].tolist()
                msg += "- lst_filtered_on_edge: " + ' '.join(map(str, lst_filtered_on_edge)) + "\n"
                if len(lst_filtered_on_edge) > 0:
                    msg += "- PRICE ON GRID EDGE - VERIFIED"
                else:
                    msg += "- WARNING - PRICE ON GRID EDGE - NOT VERIFIED"

        if self.diff_position != 0:
            msg += "DIFF:" + "\n"
            if self.diff_position > 0:
                msg += "- DIFF OPENED POSITION: " + str(self.diff_position) + "\n"
            elif self.diff_position < 0:
                msg += "- DIFF CLOSED POSITION: " + str(self.diff_position) + "\n"

        if self.diff_close_position != 0 and self.diff_open_position != 0:
            msg += "WARNING HIGH VOLATILITY:" + "\n"
            msg += "- TREND: UP/DOW" + "\n"
            msg += "- DIFF OPENED POSITION: " + str(self.diff_open_position) + "\n"
            msg += "- DIFF CLOSED POSITION: " + str(self.diff_close_position) + "\n"

        msg += "STATUS:" + "\n"
        self.previous_grid_position = dct_info['grid_position']

        return msg.upper()

    def get_grid_as_str(self, symbol):
        df_grid = self.grid[symbol]
        return df_grid.to_string(index=False)