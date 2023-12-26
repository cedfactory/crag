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

        self.grid = GridPosition(self.lst_symbols, self.grid_high, self.grid_low, self.nb_grid)

        self.CEDE_DEBUG = False # CEDE to be removed

        self.zero_print = True

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
        df_current_state = current_state["open_orders"]
        df_open_positions = current_state["open_positions"]
        df_price = current_state["prices"]

        lst_order_to_execute = []

        for symbol in self.lst_symbols:
            buying_size = self.get_grid_buying_size(symbol)
            self.grid.update_nb_open_positions(symbol, df_open_positions, buying_size)
            self.grid.update_pending_status_from_current_state(symbol, df_current_state)

            price_for_symbol = df_price.loc[df_price['symbols'] == symbol, 'values'].values[0]
            self.grid.update_grid_side(symbol, price_for_symbol)

            df_filtered_current_state = df_current_state[df_current_state['symbol'] == symbol]
            self.grid.cross_check_with_current_state(symbol, df_filtered_current_state)

            lst_order_to_execute = self.grid.get_order_list(symbol, buying_size)
            self.grid.set_to_pending_execute_order(symbol, lst_order_to_execute)
            lst_order_to_execute = self.grid.filter_lst_close_execute_order(symbol, lst_order_to_execute)

        # CEDE DEBUG
        if self.CEDE_DEBUG:
            print("price: ", df_price)
            self.grid.print_grid()

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
    def __init__(self, lst_symbols, grid_high, grid_low, nb_grid):
        self.grid_high = grid_high
        self.grid_low = grid_low
        self.nb_grid = nb_grid
        self.lst_symbols = lst_symbols

        self.trend = "FLAT"

        self.df_nb_open_positions = pd.DataFrame(columns=['symbol', 'size', 'positions_size', 'nb_open_positions'])
        self.df_nb_open_positions['symbol'] = self.lst_symbols
        self.df_nb_open_positions['size'] = 0
        self.df_nb_open_positions['positions_size'] = 0
        self.df_nb_open_positions['nb_open_positions'] = 0

        # Create a list with nb_grid split between high and low
        self.lst_grid_values = np.linspace(self.grid_high, self.grid_low, self.nb_grid + 1, endpoint=True).tolist()
        print("grid values: ", self.lst_grid_values)

        self.columns = ["grid_id", "position", "orderId", "previous_side", "side", "changes", "status"]
        self.grid = {key: pd.DataFrame(columns=self.columns) for key in self.lst_symbols}
        for symbol in lst_symbols:
            self.grid[symbol]["position"] = self.lst_grid_values
            self.grid[symbol]["grid_id"] = np.arange(len(self.grid[symbol]))
            self.grid[symbol]["orderId"] = ""
            self.grid[symbol]["previous_side"] = False
            self.grid[symbol]["side"] = ""
            self.grid[symbol]["changes"] = True
            self.grid[symbol]["status"] = "empty"
            self.grid[symbol]["cross_checked"] = False

    def update_pending_status_from_current_state(self, symbol, df_current_state):
        # Update grid status from 'pending' status to 'engaged'
        # from previous cycle request to open limit order
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
                    print("GRID ERROR - open_long vs open_short")
            else:
                print("GRID ERROR - limit order failed")

    def update_grid_side(self, symbol, position):
        df = self.grid[symbol]
        if df['previous_side'].all() == False:
            b_init = True
        else:
            b_init = False
            action_counts = df['previous_side'].value_counts()
            previous_open_long_count = action_counts.get('open_long', 0)
            previous_close_long_count = action_counts.get('close_long', 0)

        df['previous_side'] = df['side']
        # Set the 'side' column based on conditions
        df.loc[df['position'] >= position, 'side'] = 'close_long'
        df.loc[df['position'] < position, 'side'] = 'open_long'

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

    def cross_check_with_current_state(self, symbol, df_current_state):
        df_grid = self.grid[symbol]
        df_grid['cross_checked'] = False
        # Iterate over every row using iterrows()
        # Compare values from both DataFrames using iterrows
        for index_grid, row_grid in df_grid.iterrows():
            for index_c_state, row_c_state in df_current_state.iterrows():
                if row_grid['grid_id'] == row_c_state['gridId']:
                    if row_grid['side'] == row_c_state['side'] \
                            and row_grid['orderId'] == row_c_state['orderId']:
                        df_grid.loc[index_grid, 'cross_checked'] = True

    def get_order_list(self, symbol, size):
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

        lst_order_grid_id = df_filtered_changes['grid_id'].tolist() \
                            + df_filtered_checked['grid_id'].tolist() \
                            + df_filtered_pending['grid_id'].tolist()
        lst_order_grid_id = list(set(lst_order_grid_id))

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

    def clear_orderId(self, symbol, grid_id):
        df = self.grid[symbol]
        df.loc[df['orderId'] == grid_id, 'orderId'] = ""

    def print_grid(self):
        for symbol in self.lst_symbols:
            df_grid = self.grid[symbol]
            print(df_grid.to_string())

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

    def get_nb_open_positions(self, symbol):
        row = self.df_nb_open_positions[self.df_nb_open_positions['symbol'] == symbol]
        if not row.empty:
            return row['nb_open_positions'].iloc[0]
        else:
            return None

    def filter_lst_close_execute_order(self, symbol, lst_order_to_execute):
        nb_open_positions = self.get_nb_open_positions(symbol)
        df = self.grid[symbol]
        df_filtered = df[(df['status'] == 'engaged') & (df['side'] == 'close_long')]
        nb_close_position_already_open = len(df_filtered)
        nb_selected_to_be_open = nb_open_positions - nb_close_position_already_open

        filtered_orders = []
        # Filter OPEN orders
        for order in lst_order_to_execute:
            if order["type"] in ["OPEN_LONG_ORDER", "OPEN_SHORT_ORDER"]:
                filtered_orders.append(order)
        # Filter CLOSE orders and sort them by price
        close_orders = sorted(
            (order for order in lst_order_to_execute if order["type"] in ["CLOSE_LONG_ORDER", "CLOSE_SHORT_ORDER"]),
            key=lambda x: x["price"]
        )
        # Append a subset of CLOSE orders based on the number of open positions
        if self.trend == "DOWN":
            filtered_orders.extend(close_orders[1:(nb_selected_to_be_open+1)])
        else:
            filtered_orders.extend(close_orders[:nb_selected_to_be_open])

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