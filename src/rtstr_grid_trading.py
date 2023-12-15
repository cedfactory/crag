from . import rtdp, rtstr, rtctrl
import pandas as pd
import numpy as np

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

        if self.CEDE_DEBUG:
            print("******************** Init: ********************")
            self.grid.print_grid()
            lst_current_state = ["symbol", "price", "side", "orderId", "grid_id"]
            df_current_state = pd.DataFrame(columns=lst_current_state)

            print("******************** Step 1 - Send price = 42000 ********************")
            print('current state sent from Crag:')
            print(df_current_state.to_string())

            df_price = pd.DataFrame(columns=["symbol", "price"])
            df_price["symbol"] = ["BTC"]
            df_price.loc[df_price["symbol"] == "BTC", "price"] = 42000

            print('current price sent from Crag:')
            print(df_price.to_string())

            lst_order_to_execute = self.set_broker_current_state(df_current_state, df_price)
            print("list order to be performed by Crag -> Broker: [")
            if len(lst_order_to_execute) > 0:
                for order in lst_order_to_execute:
                    print(order)
            print("]")
            self.grid.print_grid()

            print("******************** Step 2 - Cross Check opened positions ********************")

            lst_current_state = ["symbol", "price", "side", "orderId", "grid_id"]
            df_current_state = pd.DataFrame(columns=lst_current_state)

            new_row = {'symbol': 'BTC', 'price': 44000, "side": "close_long", "orderId": 1234456, "grid_id": 0}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 43600, "side": "close_long", "orderId": 1234456, "grid_id": 1}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 43200, "side": "close_long", "orderId": 1234456, "grid_id": 2}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 42800, "side": "close_long", "orderId": 1234456, "grid_id": 3}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 42400, "side": "close_long", "orderId": 1234456, "grid_id": 4}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 42000, "side": "close_long", "orderId": 1234456, "grid_id": 5}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 41600, "side": "open_long", "orderId": 1234456, "grid_id": 6}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 41200, "side": "open_long", "orderId": 1234456, "grid_id": 7}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 40800, "side": "open_long", "orderId": 1234456, "grid_id": 8}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 40400, "side": "open_long", "orderId": 1234456, "grid_id": 9}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 40000, "side": "open_long", "orderId": 1234456, "grid_id": 10}
            df_current_state = df_current_state.append(new_row, ignore_index=True)

            print('current price sent from Crag:')
            print(df_price.to_string())

            df_price = pd.DataFrame(columns=["symbol", "price"])
            df_price["symbol"] = ["BTC"]
            df_price.loc[df_price["symbol"] == "BTC", "price"] = 42000

            print('current price sent from Crag:')
            print(df_price.to_string())

            lst_order_to_execute = self.set_broker_current_state(df_current_state, df_price)
            print("list order to be performed by Crag -> Broker: [")
            if len(lst_order_to_execute) > 0:
                for order in lst_order_to_execute:
                    print(order)
            print("]")
            self.grid.print_grid()

            print("******************** Step 3 - Send changed price = 42600            ********************")
            print("******************** Step 3 - request for new position to be opened ********************")

            df_price = pd.DataFrame(columns=["symbol", "price"])
            df_price["symbol"] = ["BTC"]
            df_price.loc[df_price["symbol"] == "BTC", "price"] = 42600

            lst_current_state = ["symbol", "price", "side", "orderId", "grid_id"]
            df_current_state = pd.DataFrame(columns=lst_current_state)

            new_row = {'symbol': 'BTC', 'price': 44000, "side": "close_long", "orderId": 1234456, "grid_id": 0}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 43600, "side": "close_long", "orderId": 1234456, "grid_id": 1}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 43200, "side": "close_long", "orderId": 1234456, "grid_id": 2}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 42800, "side": "close_long", "orderId": 1234456, "grid_id": 3}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            # new_row = {'symbol': 'BTC', 'price': 42400, "side": "close_long", "orderId": 1234456, "grid_id": 4}
            # df_current_state = df_current_state.append(new_row, ignore_index=True)
            # new_row = {'symbol': 'BTC', 'price': 42000, "side": "close_long", "orderId": 1234456, "grid_id": 5}
            # df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 41600, "side": "open_long", "orderId": 1234456, "grid_id": 6}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 41200, "side": "open_long", "orderId": 1234456, "grid_id": 7}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 40800, "side": "open_long", "orderId": 1234456, "grid_id": 8}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 40400, "side": "open_long", "orderId": 1234456, "grid_id": 9}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 40000, "side": "open_long", "orderId": 1234456, "grid_id": 10}
            df_current_state = df_current_state.append(new_row, ignore_index=True)

            print('current price sent from Crag:')
            print(df_price.to_string())

            lst_order_to_execute = self.set_broker_current_state(df_current_state, df_price)
            print("list order to be performed by Crag -> Broker: [")
            if len(lst_order_to_execute) > 0:
                for order in lst_order_to_execute:
                    print(order)
            print("]")
            self.grid.print_grid()

            print("******************** Step 4 - Cross check if new position have been opened ********************")

            df_price = pd.DataFrame(columns=["symbol", "price"])
            df_price["symbol"] = ["BTC"]
            df_price.loc[df_price["symbol"] == "BTC", "price"] = 42600

            lst_current_state = ["symbol", "price", "side", "orderId", "grid_id"]
            df_current_state = pd.DataFrame(columns=lst_current_state)

            new_row = {'symbol': 'BTC', 'price': 44000, "side": "close_long", "orderId": 1234456, "grid_id": 0}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 43600, "side": "close_long", "orderId": 1234456, "grid_id": 1}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 43200, "side": "close_long", "orderId": 1234456, "grid_id": 2}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 42800, "side": "close_long", "orderId": 1234456, "grid_id": 3}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 42400, "side": "open_long", "orderId": 654987, "grid_id": 4}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 42000, "side": "open_long", "orderId": 789456, "grid_id": 5}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 41600, "side": "open_long", "orderId": 1234456, "grid_id": 6}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 41200, "side": "open_long", "orderId": 1234456, "grid_id": 7}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 40800, "side": "open_long", "orderId": 1234456, "grid_id": 8}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 40400, "side": "open_long", "orderId": 1234456, "grid_id": 9}
            df_current_state = df_current_state.append(new_row, ignore_index=True)
            new_row = {'symbol': 'BTC', 'price': 40000, "side": "open_long", "orderId": 1234456, "grid_id": 10}
            df_current_state = df_current_state.append(new_row, ignore_index=True)

            print('current price sent from Crag:')
            print(df_price.to_string())

            lst_order_to_execute = self.set_broker_current_state(df_current_state, df_price)
            print("list order to be performed by Crag -> Broker: [")
            if len(lst_order_to_execute) > 0:
                for order in lst_order_to_execute:
                    print(order)
            print("]")
            self.grid.print_grid()

            print("******************** Step 5 - End ********************")


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
            "prices": df_prices
        }
        """
        df_current_state = current_state["open_orders"]
        df_price = current_state["prices"]

        for symbol in self.lst_symbols:
            self.grid.update_pending_status_from_current_state(symbol, df_current_state)

            price_for_symbol = df_price.loc[df_price['symbols'] == symbol, 'values'].values[0]
            self.grid.update_grid_side(symbol, price_for_symbol)

            df_filtered_current_state = df_current_state[df_current_state['symbol'] == symbol]
            self.grid.cross_check_with_current_state(symbol, df_filtered_current_state)

            lst_order_to_execute = self.grid.get_order_list(symbol, self.get_grid_buying_size(symbol))
            self.grid.set_to_pending_execute_order(symbol, lst_order_to_execute)

        return lst_order_to_execute

class GridPosition():
    def __init__(self, lst_symbols, grid_high, grid_low, nb_grid):
        self.grid_high = grid_high
        self.grid_low = grid_low
        self.nb_grid = nb_grid
        self.lst_symbols = lst_symbols

        # Create a list with nb_grid split between high and low
        # self.lst_grid_values = np.linspace(self.grid_high, self.grid_low, self.nb_grid, endpoint=False).tolist()
        self.lst_grid_values = np.linspace(self.grid_high, self.grid_low, self.nb_grid + 1, endpoint=True).tolist()
        print(self.lst_grid_values)

        self.columns = ["grid_id", "position", "orderId", "previous_side", "side", "changes", "status"]
        self.grid = {key: pd.DataFrame(columns=self.columns) for key in self.lst_symbols}
        for symbol in lst_symbols:
            self.grid[symbol]["position"] = self.lst_grid_values
            self.grid[symbol]["grid_id"] = np.arange(len(self.grid[symbol]))
            self.grid[symbol]["orderId"] = ""
            self.grid[symbol]["previous_side"] = ""
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
            if grid_id in df_current_state["grid_id"].tolist():
                if side == df_current_state.loc[df_current_state["grid_id"] == grid_id, 'side'].values[0]:
                    df.loc[df['grid_id'] == grid_id, 'status'] = 'engaged'
                    df.loc[df['grid_id'] == grid_id, 'orderId'] = df_current_state.loc[df_current_state["grid_id"] == grid_id, 'orderId'].values[0]
                else:
                    print("GRID ERROR - open_long vs open_short")
            else:
                print("GRID ERROR - limit order failed")

    def update_grid_side(self, symbol, position):
        df = self.grid[symbol]
        df['previous_side'] = df['side']
        # Set the 'side' column based on conditions
        df.loc[df['position'] >= position, 'side'] = 'close_long'
        df.loc[df['position'] < position, 'side'] = 'open_long'

        # Compare if column1 and column2 are the same
        df['changes'] = df['previous_side'] != df['side']


    def cross_check_with_current_state(self, symbol, df_current_state):
        df_grid = self.grid[symbol]
        df_grid['cross_checked'] = False
        # Iterate over every row using iterrows()
        # Compare values from both DataFrames using iterrows
        for index_grid, row_grid in df_grid.iterrows():
            for index_c_state, row_c_state in df_current_state.iterrows():
                if row_grid['grid_id'] == row_c_state['grid_id']:
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

