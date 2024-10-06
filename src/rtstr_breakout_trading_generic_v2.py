from . import rtdp, rtstr
import math
import pandas as pd
import numpy as np

import heapq

import copy
import os
import glob

import pprint
from . import utils
from src import logger

class StrategyBreakoutTradingGenericV2(rtstr.RealTimeStrategy):

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

        self.zero_print = True
        self.strategy_id = utils.generate_random_id(4)
        self.grid = GridPosition(self.side,
                                 self.lst_symbols,
                                 self.grid_high, self.grid_low, self.nb_grid, self.percent_per_grid,
                                 self.nb_position_limits,
                                 self.strategy_id,
                                 self.zero_print,
                                 self.loggers)
        self.df_grid_buying_size = pd.DataFrame()
        self.execute_timer = None

        self.df_price = None
        self.mutiple_strategy = False

        self.backup_grid = None

        self.self_execute_trade_recorder_not_active = True

    def get_data_description(self):
        ds = {}
        return ds

    def get_info(self):
        return "StrategyBreakoutTradingGenericV2"

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

        # df_current_states = current_state["open_orders"].copy()
        # df_open_positions = current_state["open_positions"].copy()
        df_open_triggers = current_state["triggers"].copy()
        df_price = current_state["prices"].copy()

        if not self.mutiple_strategy:
            del current_state["open_orders"]
            del current_state["open_positions"]
            del current_state["triggers"]
            del current_state["prices"]
            del current_state

        del self.df_price
        self.df_price = df_price
        self.grid.set_prices(self.df_price)
        lst_order_to_execute = []

        for symbol in self.lst_symbols:
            # self.print_debug_grid(symbol)
            # df_open_triggers = self.grid.set_current_orders_price_to_grid(symbol, df_open_triggers)
            if symbol in df_price['symbols'].tolist():
                self.grid.set_triggers_executed(symbol, df_open_triggers)
                self.grid.set_current_price(df_price.loc[df_price['symbols'] == symbol, 'values'].values[0])
                self.grid.update_unknown_status(symbol)
                self.grid.cross_check_with_current_state(symbol)
                self.grid.update_grid_side(symbol)
            lst_order_to_execute = self.grid.get_order_list(symbol)
        del df_price

        # self.iter_set_broker_current_state += 1

        return lst_order_to_execute

    def print_debug_grid(self, symbol):
        lst_trend = ["up", "down"]
        exclude_columns = ['unknown', 'status']

        # Initialize backup_grid if it's None
        if self.backup_grid is None:
            # Create a deep copy of the grid as backup
            self.backup_grid = copy.deepcopy(self.grid.grid)

            # Delete all the trend.txt files in the 'debug_backup_grid' directory
            backup_dir = 'debug_backup_grid'
            if os.path.exists(backup_dir):
                # Use glob to find all .txt files in the directory and remove them
                for file in glob.glob(os.path.join(backup_dir, "*.txt")):
                    os.remove(file)

            return
        else:
            # Iterate over trends and check if the backup does not match the current grid
            for trend in lst_trend:
                # Create a copy of the DataFrame without the excluded columns
                df_to_save = self.grid.grid[symbol][trend].drop(columns=exclude_columns).copy()
                df_backup_to_save = self.backup_grid[symbol][trend].drop(columns=exclude_columns).copy()

                # Use DataFrame.equals() to check if DataFrames are exactly the same
                if not df_backup_to_save.equals(df_to_save):
                    # Create the directory if it doesn't exist
                    os.makedirs('debug_backup_grid', exist_ok=True)

                    # Open the file in append mode
                    backup_file = f'debug_backup_grid/{trend}.txt'
                    with open(backup_file, 'a') as f:
                        # Write additional values
                        f.write(f"\n---\n")
                        f.write(f"Trend: {trend}\n")
                        f.write(f"nb_open_triggers_long: {self.grid.trend_data[trend]['nb_open_triggers_long']}\n")
                        f.write(f"nb_executed_trigger_long: {self.grid.trend_data[trend]['nb_executed_trigger_long']}\n")
                        f.write(f"nb_open_trailers_long: {self.grid.trend_data[trend]['nb_open_trailers_long']}\n")
                        f.write(f"sum_long: {self.grid.trend_data[trend]['nb_open_triggers_long'] + self.grid.trend_data[trend]['nb_executed_trigger_long'] + self.grid.trend_data[trend]['nb_open_trailers_long']}\n")
                        f.write(f"nb_open_triggers_short: {self.grid.trend_data[trend]['nb_open_triggers_short']}\n")
                        f.write(f"nb_executed_trigger_short: {self.grid.trend_data[trend]['nb_executed_trigger_short']}\n")
                        f.write(f"nb_open_trailers_short: {self.grid.trend_data[trend]['nb_open_trailers_short']}\n")
                        f.write(f"sum_short: {self.grid.trend_data[trend]['nb_open_triggers_short'] + self.grid.trend_data[trend]['nb_executed_trigger_short'] + self.grid.trend_data[trend]['nb_open_trailers_short']}\n")
                        try:
                            # Append the filtered DataFrame to the file
                            if isinstance(self.grid.trend_data[trend]['df_open_triggers'], pd.DataFrame) \
                                    and not self.grid.trend_data[trend]['df_open_triggers'].empty:
                                f.write("df_open_triggers" + "\n")
                                f.write(self.grid.trend_data[trend]['df_open_triggers'].to_string() + "\n")
                            if isinstance(self.grid.trend_data[trend]['df_open_trailers'], pd.DataFrame) \
                                    and not self.grid.trend_data[trend]['df_open_trailers'].empty:
                                f.write("df_open_trailers" + "\n")
                                f.write(self.grid.trend_data[trend]['df_open_TP_order'].to_string() + "\n")
                            if isinstance(self.grid.trend_data[trend]['df_open_TP_order'], pd.DataFrame) \
                                    and not self.grid.trend_data[trend]['df_open_SL_order'].empty:
                                f.write("\n" + "df_open_TP_order" + "\n")
                                f.write(self.grid.trend_data[trend]['df_open_TP_order'].to_string() + "\n")
                            f.write("\n" + "df_open_SL_order" + "\n")
                            if isinstance(self.grid.trend_data[trend]['df_open_SL_order'], pd.DataFrame) \
                                    and not self.grid.trend_data[trend]['df_open_SL_order'].empty:
                                f.write(self.grid.trend_data[trend]['df_open_SL_order'].to_string() + "\n")
                            f.write("\n" + "df_executed_trigger_debug" + "\n")
                            if isinstance(self.grid.trend_data[trend]['df_executed_trigger_debug'], pd.DataFrame) \
                                    and not self.grid.trend_data[trend]['df_executed_trigger_debug'].empty:
                                f.write("\n" + "df_executed_trigger_debug" + "\n")
                                f.write(self.grid.trend_data[trend]['df_executed_trigger_debug'].to_string() + "\n")
                            f.write("\n")
                            f.write(df_to_save.to_string())  # Write full DataFrame to file
                            f.write("\n---\n")  # Add a separator line
                        except:
                            print("toto")

            # Update the backup after processing
            self.backup_grid = copy.deepcopy(self.grid.grid)

        return

    def print_grid(self):
        self.grid.print_grid()

    def save_grid_scenario(self, path, cpt):
        for symbol in self.lst_symbols:
            self.grid.save_grid_scenario(symbol, path, cpt)

    def set_normalized_grid_price(self, lst_symbol_plc_endstp):
        for price_plc in lst_symbol_plc_endstp:
            if price_plc['symbol'] in self.lst_symbols:
                self.grid.normalize_grid_price(price_plc['symbol'], price_plc['pricePlace'], price_plc['priceEndStep'], price_plc['sizeMultiplier'])

    def get_info_msg_status(self):
        return ""

    def get_grid(self, cpt):
        # CEDE: MULTI SYMBOL TO BE IMPLEMENTED IF EVER ONE DAY.....
        for symbol in self.lst_symbols:
            return self.grid.get_grid(symbol, cpt)

    def record_status(self):
        if self.self_execute_trade_recorder_not_active:
            return

    def update_executed_trade_status(self, lst_orders):
        if lst_orders is None \
                or (len(lst_orders) == 0):
            return
        else:
            for symbol in self.lst_symbols:
                self.grid.update_executed_trade_status(symbol, lst_orders)

    def get_strategy_id(self):
        return self.strategy_id

    def set_breakout_grid_buying_size(self, symbol, dol_per_grid):
        self.grid.set_breakout_grid_buying_size(symbol, dol_per_grid)

    def set_df_buying_size(self, df_symbol_size, cash):
        if not isinstance(df_symbol_size, pd.DataFrame):
            return
        # cash = 10000 # CEDE GRID SCENARIO
        self.df_grid_buying_size = pd.concat([self.df_grid_buying_size, df_symbol_size])
        self.df_grid_buying_size['margin'] = None
        for symbol in self.lst_symbols:
            dol_per_grid = self.grid_margin / self.nb_grid
            size = dol_per_grid
            if (self.get_grid_buying_min_size(symbol) <= (size / self.grid.get_max_price_in_grid())) \
                    and (size > 5) and \
                    (cash >= self.grid_margin):

                self.set_breakout_grid_buying_size(symbol, dol_per_grid)

                self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol, "strategy_id"] = self.strategy_id
                self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol, "buyingSize"] = size
                self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol, "margin"] = self.grid_margin
                self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol, "maxSizeToBuy"] = self.nb_grid
                msg = "**" + symbol + "**\n"
                msg += "**cash: " + str(round(cash, 2)) + "**\n"
                msg += "**grid_margin: " + str(round(self.grid_margin, 2)) + "**\n"
                msg += "**nb grid: " + str(self.nb_grid) + "**\n"
                msg += "**amount buying > 5 usd: " + str(round(size, 2)) + "**\n"
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
                exit(22)
        return self.df_grid_buying_size

class GridPosition():
    def __init__(self, side, lst_symbols, grid_high, grid_low, nb_grid, percent_per_grid, nb_position_limits, strategy_id, debug_mode=True, loggers=[]):
        self.grid_side = side
        self.side_mapping = {
            "up": "trigger_long",
            "down": "trigger_short"
        }

        self.lst_trend = ["up", "down"]
        self.trend_data = {}
        self.df_previous_open_trailers = {}
        for trend in self.lst_trend:
            self.df_previous_open_trailers[trend] = None

        self.str_open, self.str_close = self.side_mapping.get(self.grid_side, (None, None))

        self.df_price = None

        self.strategy_id = strategy_id
        self.grid_high = grid_high
        self.grid_low = grid_low
        self.nb_grid = nb_grid
        self.lst_symbols = lst_symbols
        self.percent_per_grid = percent_per_grid

        self.lst_recoded_trigger_executed = []

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
        self.steps = 0
        self.max_position = 0
        self.min_position = 0
        self.grid_move = False
        self.msg = ""
        self.df_grid_string = ""
        self.abs = None
        self.closest = None

        self.previous_nb_open_positions = 0

        self.record_trigger_opened = []

        self.max_grid_close_order = -1
        self.min_grid_close_order = -1
        self.nb_close_missing = -1

        self.remaining_close_to_be_open = 0

        self.max_grid_open_order = -1
        self.min_grid_open_order = -1
        self.nb_open_missing = -1

        self.nb_grid_up = 100
        self.nb_grid_down = 100
        grid_calculator = GridCalculator(self.grid_high, self.grid_low, self.percent_per_grid, self.nb_grid_up, self.nb_grid_down)
        lst_grid_values_up, lst_grid_values_down = grid_calculator.calculate_grid_values()

        self.dct_lst_grid_values = {"up": list(reversed(lst_grid_values_up)),
                                    "down": lst_grid_values_down}

        # self.lst_grid_values = np.linspace(self.grid_high, self.grid_low, self.nb_grid + 1, endpoint=True).tolist()

        self.columns = ["grid_id", "position",
                        "side",
                        "orderId_trigger_long", "status_trigger_long", "trigger_long",
                        "orderId_trailer_long", "status_trailer_long", "trailer_long",
                        "status_long",
                        "orderId_trigger_short", "status_trigger_short", "trigger_short",
                        "orderId_trailer_short", "status_trailer_short", "trailer_short",
                        "status_short",
                        "status", "unknown"]
        grid_break_out = {key: pd.DataFrame(columns=self.columns) for key in self.lst_trend}
        self.grid = {key: grid_break_out for key in self.lst_symbols}
        self.max_position = 0
        for symbol in self.lst_symbols:
            for trend in self.lst_trend:
                self.grid[symbol][trend]["position"] = self.dct_lst_grid_values[trend]
                print(self.grid[symbol][trend]["position"].to_list())

                self.max = max(self.grid[symbol][trend]["position"].to_list())
                self.max_position = max(self.max, self.max_position)

                self.grid[symbol][trend]["grid_id"] = self.grid[symbol][trend].index
                for col in ["status_trigger_long", "status_trailer_long",
                            "status_trigger_short", "status_trailer_short",
                            "status_long", "status_short", "status"]:
                    self.grid[symbol][trend][col] = "empty"
                for col in ["orderId_trigger_long", "orderId_trailer_long",
                            "orderId_trigger_short", "orderId_trailer_short",
                            "side"]:
                    self.grid[symbol][trend][col] = ""
                for col in ["trigger_long", "trailer_long",
                            "trigger_short", "trailer_short",
                            "unknown"]:
                    self.grid[symbol][trend][col] = False

    def log(self, msg, header="", attachments=[]):
        if self.zero_print:
            return
        for iter_logger in self.loggers:
            iter_logger.log(msg, header=header, author=type(self).__name__, attachments=attachments)

    def set_breakout_grid_buying_size(self, symbol, dol_per_grid):
        for trend in self.lst_trend:
            df_grid = self.grid[symbol][trend]
            df_grid["buying_size"] = dol_per_grid / df_grid["position"]

    def set_prices(self, df):
        self.df_price = df

    def get_max_price_in_grid(self):
        return self.max_position

    def get_opposite_trend(self, trend):
        return "down" if trend == "up" else "up"

    def update_grid_side(self, symbol):
        for trend in self.lst_trend:
            df = self.grid[symbol][trend]

            df.loc[df['position'] > self.current_price, 'side'] = self.side_mapping["up"]
            df.loc[df['position'] < self.current_price, 'side'] = self.side_mapping["down"]
            df.loc[df['position'] == self.current_price, 'side'] = "on_edge"

            differences = df['position'].diff().dropna()
            diff = differences.mean() / 3
            df.loc[(df['position'] >= self.current_price - abs(diff)) & (df['position'] <= self.current_price + abs(diff)), 'side'] = "on_edge"

        del symbol
        del df

    # Function to find the index of the closest value in an array
    def find_closest(self, value, array):
        self.abs = None
        self.closest = None

        self.abs = np.abs(array - value)
        self.closest = self.abs.argmin()
        # closest = np.abs(array - value).argmin()
        # return closest

    def set_current_orders_price_to_grid(self, symbol, df_open_triggers):
        if not df_open_triggers.empty:
            df_open_triggers = df_open_triggers[df_open_triggers["symbol"] == symbol]
            df_open_triggers = df_open_triggers.loc[df_open_triggers['strategyId'] == self.strategy_id]
            df_grid_concat = pd.DataFrame()
            for trend in self.lst_trend:
                df_grid = self.grid[symbol][trend]
                df_grid_concat = pd.concat([df_grid_concat, df_grid], axis=0)
            df_grid = df_grid_concat
            lst_price = []
            for price in df_open_triggers['triggerPrice']:
                self.find_closest(price, df_grid['position'])
                closest_value = df_grid.at[self.closest, 'position']
                lst_price.append(closest_value)
            df_open_triggers['triggerPrice'] = lst_price
            del lst_price
            # Check if all elements in df_current_state['price'] are in df_grid['position']
            self.s_bool = df_open_triggers['price'].isin(df_grid['position'])
            self.all_prices_in_grid = self.s_bool.all()
            if not self.zero_print and not self.all_prices_in_grid:
                # Print the elements that are different
                different_prices = df_open_triggers.loc[~df_open_triggers['price'].isin(df_grid['position']), 'price']
                self.log("################ WARNING PRICE DIFF WITH ORDER AND GRID ###############################")
                self.log("Elements in df_current_state['price'] that are not in df_grid['position']:")
                self.log(different_prices)
            del self.s_bool
            del self.all_prices_in_grid
            del df_grid
        return df_open_triggers

    def set_current_price(self, price):
        self.current_price = price

    def update_unknown_status(self, symbol):
        for trend in self.lst_trend:
            df_grid = self.grid[symbol][trend]
            if df_grid['unknown'].any():
                condition = df_grid['unknown']
                df_grid.loc[condition, ["status"]] = ['empty']
                df_grid.loc[condition, ['orderId_trigger_short', 'trigger_short', 'status_trigger_short']] = ['',
                                                                                                              False,
                                                                                                              'empty']
                df_grid.loc[condition, ['orderId_trigger_long', 'trigger_long', 'status_trigger_long']] = ['',
                                                                                                           False,
                                                                                                           'empty']
            df_grid["unknown"] = False

    def cross_check_with_current_state(self, symbol):
        for trend in self.lst_trend:
            df_grid = self.grid[symbol][trend]
            if self.trend_data[trend]['nb_executed_trailer'] > 0:
                for orderId in self.trend_data[trend]['lst_orderId_executed_trailer']:
                    # Use boolean indexing to find and update the rows
                    condition = df_grid['orderId_trailer_short'] == orderId
                    df_grid.loc[condition, ['orderId_trailer_short', 'trailer_short', 'status_trailer_short', "status"]] = ['',
                                                                                                                            False,
                                                                                                                            'empty',
                                                                                                                            'empty']
                    df_grid.loc[condition, ['orderId_trigger_short', 'trigger_short', 'status_trigger_short']] = ['',
                                                                                                                  False,
                                                                                                                  'empty']
                    condition = df_grid['orderId_trailer_long'] == orderId
                    df_grid.loc[condition, ['orderId_trailer_long', 'trailer_long', 'status_trailer_long', "status"]] = ['',
                                                                                                                         False,
                                                                                                                         'empty',
                                                                                                                         'empty']
                    df_grid.loc[condition, ['orderId_trigger_long', 'trigger_long', 'status_trigger_long']] = ['',
                                                                                                               False,
                                                                                                               'empty']
            if self.trend_data[trend]['nb_new_open_trailers'] > 0:
                for orderId in self.trend_data[trend]['lst_orderId_new_open_trailers']:
                    condition_trailer = self.trend_data[trend]['df_new_open_trailers']["orderId"] == orderId
                    side = self.trend_data[trend]['df_new_open_trailers'].loc[condition_trailer, "side"].values[0]
                    gridId = self.trend_data[trend]['df_new_open_trailers'].loc[condition_trailer, "gridId"].values[0]
                    condition_grid = df_grid['grid_id'] == int(gridId)
                    if side == "sell":
                        df_grid.loc[condition_grid, ['orderId_trailer_short', 'trailer_short', 'status_trailer_short', "status"]] = [orderId,
                                                                                                                                     True,
                                                                                                                                     'engaged',
                                                                                                                                     'engaged']
                        df_grid.loc[condition_grid, ['orderId_trigger_short', 'trigger_short', 'status_trigger_short', "status"]] = ["",
                                                                                                                                     False,
                                                                                                                                     'empty',
                                                                                                                                     'engaged']

                    elif side == "buy":
                        df_grid.loc[condition_grid, ['orderId_trailer_long', 'trailer_long', 'status_trailer_long', "status"]] = [orderId,
                                                                                                                                  True,
                                                                                                                                  'engaged',
                                                                                                                                  'engaged']

                        df_grid.loc[condition_grid, ['orderId_trigger_long', 'trigger_long', 'status_trigger_long', "status"]] = ["",
                                                                                                                                  False,
                                                                                                                                  'empty',
                                                                                                                                  'engaged']
    def clear_executed_trigger(self, symbol):
        for trend in self.lst_trend:
            df_grid = self.grid[symbol][trend]
            if self.trend_data[trend]['nb_executed_trigger'] > 0:
                for orderId in self.trend_data[trend]['lst_orderId_executed_trigger']:
                    # Use boolean indexing to find and update the rows
                    condition = df_grid['orderId_trigger_short'] == orderId
                    df_grid.loc[condition, ['orderId_trigger_short', 'trigger_short', 'status_trigger_short']] = ['',
                                                                                                                  False,
                                                                                                                  'empty']
                    condition = df_grid['orderId_trigger_long'] == orderId
                    df_grid.loc[condition, ['orderId_trigger_long', 'trigger_long', 'status_trigger_long']] = ['',
                                                                                                               False,
                                                                                                               'empty']

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
        lst_of_lst_order = []
        for trend in self.lst_trend:
            df_grid = self.grid[symbol][trend]
            lst_order_trailer = []
            if self.trend_data[trend]['nb_executed_trigger'] > 0:
                if len(self.lst_recoded_trigger_executed) > 0:
                    # Create orders based on executed triggers
                    lst_order_trailer = self.get_order_trailer_list(symbol, df_grid, trend, self.lst_recoded_trigger_executed)

                    return lst_order_trailer + self.lst_of_data_record

            condition_0 = df_grid['grid_id'] != 0
            condition_trigger_long = df_grid['status_trigger_long'] == 'engaged'
            condition_trigger_short = df_grid['status_trigger_short'] == 'engaged'
            condition_trigger_long_free = df_grid['status_trigger_long'] == 'empty'
            condition_trigger_short_free = df_grid['status_trigger_short'] == 'empty'
            condition_trailer_long_free = df_grid['status_trailer_long'] == 'empty'
            condition_trailer_short_free = df_grid['status_trailer_short'] == 'empty'
            condition_side_long = df_grid['side'] == 'trigger_long'
            condition_side_short = df_grid['side'] == 'trigger_short'
            condition_not_on_edge = df_grid['side'] != 'on_edge'

            df_grid_cancel_long = df_grid[condition_0
                                          & condition_trigger_long
                                          & condition_side_short].copy()
            df_grid_cancel_short = df_grid[condition_0
                                          & condition_trigger_short
                                          & condition_side_long].copy()
            if len(df_grid_cancel_long) or len(df_grid_cancel_short):
                lst_grid_id_cancel_long = df_grid_cancel_long["grid_id"].to_list()
                lst_grid_id_cancel_short = df_grid_cancel_short["grid_id"].to_list()

                lst_order_cancel_long = self.get_order_cancel_list(symbol, df_grid, trend, lst_grid_id_cancel_long)
                lst_order_cancel_short = self.get_order_cancel_list(symbol, df_grid, trend, lst_grid_id_cancel_short)

                return lst_order_cancel_long + lst_order_cancel_short

            self.get_free_spot_trigger(df_grid, trend)
            # nb_trigger_engaged = nb_trigger_engaged + nb_trigger_triggered + nb_trailer
            nb_trigger_engaged_long = self.trend_data[trend]['nb_open_triggers_long'] \
                                      + self.trend_data[trend]['nb_executed_trigger_long'] \
                                      + self.trend_data[trend]['nb_open_trailers_long']
            nb_trigger_engaged_short = self.trend_data[trend]['nb_open_triggers_short'] \
                                       + self.trend_data[trend]['nb_executed_trigger_short'] \
                                       + self.trend_data[trend]['nb_open_trailers_short']

            if nb_trigger_engaged_short < self.nb_grid:
                # raise trigger nb_diff
                nb_trigger_to_raise = self.nb_grid - nb_trigger_engaged_short

                df_grid_free_spot_short = df_grid[condition_0
                                                  & condition_side_short
                                                  & condition_trigger_short_free
                                                  & condition_trailer_short_free
                                                  & condition_not_on_edge].copy()
                lst_grid_free_spot_short = df_grid_free_spot_short['grid_id'].to_list()
                if trend == "up":
                    lst_grid_free_spot_short = heapq.nlargest(nb_trigger_to_raise, lst_grid_free_spot_short)
                elif trend == "down":
                    lst_grid_free_spot_short = heapq.nsmallest(nb_trigger_to_raise, lst_grid_free_spot_short)

                lst_order_short = self.get_order_trigger_list(symbol, df_grid, trend, lst_grid_free_spot_short)
                lst_of_lst_order += lst_order_short

            elif nb_trigger_engaged_short == self.nb_grid \
                    and self.nb_free_spot_short > 0:
                # cancel existing triggers (nb_free_spot_short) + raise new trigger at free spot
                nb_trigger_to_cancel = min(self.nb_free_spot_short, self.trend_data[trend]['nb_open_triggers_short'])
                df_grid_cancel_short = df_grid[condition_0
                                               & condition_trigger_short].copy()
                lst_grid_cancel_short = df_grid_cancel_short['grid_id'].to_list()
                if trend == "up":
                    lst_grid_cancel_short = heapq.nsmallest(nb_trigger_to_cancel, lst_grid_cancel_short)
                elif trend == "down":
                    lst_grid_cancel_short = heapq.nlargest(nb_trigger_to_cancel, lst_grid_cancel_short)
                lst_order_cancel = self.get_order_cancel_list(symbol, df_grid, trend, lst_grid_cancel_short)
                lst_of_lst_order += lst_order_cancel

                nb_trigger_to_raise = min(self.nb_free_spot_short, self.trend_data[trend]['nb_open_triggers_short'])
                """
                df_grid_free_spot_short = df_grid[condition_0
                                                  & condition_side_short
                                                  & condition_trigger_short_free].copy()
                lst_grid_free_spot_short = df_grid_free_spot_short['grid_id'].to_list()
                """
                lst_grid_free_spot_short = self.lst_grid_free_spot_short.copy()
                if trend == "up":
                    lst_grid_free_spot_short = heapq.nlargest(nb_trigger_to_raise, lst_grid_free_spot_short)
                elif trend == "down":
                    lst_grid_free_spot_short = heapq.nsmallest(nb_trigger_to_raise, lst_grid_free_spot_short)
                lst_order_short = self.get_order_trigger_list(symbol, df_grid, trend, lst_grid_free_spot_short)

                lst_of_lst_order += lst_order_short

            if nb_trigger_engaged_long < self.nb_grid:
                # raise trigger nb_diff
                nb_trigger_to_raise = self.nb_grid - nb_trigger_engaged_long

                df_grid_free_spot_long = df_grid[condition_0
                                                 & condition_side_long
                                                 & condition_trigger_long_free
                                                 & condition_trailer_long_free
                                                 & condition_not_on_edge].copy()
                lst_grid_free_spot_long = df_grid_free_spot_long['grid_id'].to_list()
                if trend == "up":
                    lst_grid_free_spot_long = heapq.nsmallest(nb_trigger_to_raise, lst_grid_free_spot_long)
                elif trend == "down":
                    lst_grid_free_spot_long = heapq.nlargest(nb_trigger_to_raise, lst_grid_free_spot_long)

                lst_order_long = self.get_order_trigger_list(symbol, df_grid, trend, lst_grid_free_spot_long)
                lst_of_lst_order += lst_order_long

            elif nb_trigger_engaged_long == self.nb_grid \
                    and self.nb_free_spot_long > 0:
                # cancel existing triggers (nb_free_spot_long) + raise new trigger at free spot
                nb_trigger_to_cancel = min(self.nb_free_spot_long, self.trend_data[trend]['nb_open_triggers_long'])
                df_grid_cancel_long = df_grid[condition_0
                                              & condition_trigger_long].copy()
                lst_grid_cancel_long = df_grid_cancel_long['grid_id'].to_list()
                if trend == "up":
                    lst_grid_cancel_long = heapq.nlargest(nb_trigger_to_cancel, lst_grid_cancel_long)
                elif trend == "down":
                    lst_grid_cancel_long = heapq.nsmallest(nb_trigger_to_cancel, lst_grid_cancel_long)
                lst_order_cancel = self.get_order_cancel_list(symbol, df_grid, trend, lst_grid_cancel_long)
                lst_of_lst_order += lst_order_cancel

                nb_trigger_to_raise = min(self.nb_free_spot_long, self.trend_data[trend]['nb_open_triggers_long'])
                """
                df_grid_free_spot_long = df_grid[condition_0
                                                 & condition_side_long
                                                 & condition_trigger_long_free].copy()
                lst_grid_free_spot_long = df_grid_free_spot_long['grid_id'].to_list()
                """
                lst_grid_free_spot_long = self.lst_grid_free_spot_long.copy()
                if trend == "up":
                    lst_grid_free_spot_long = heapq.nsmallest(nb_trigger_to_raise, lst_grid_free_spot_long)
                elif trend == "down":
                    lst_grid_free_spot_long = heapq.nlargest(nb_trigger_to_raise, lst_grid_free_spot_long)
                lst_order_long = self.get_order_trigger_list(symbol, df_grid, trend, lst_grid_free_spot_long)
                lst_of_lst_order += lst_order_long

        lst_of_lst_order += self.lst_of_data_record
        lst_of_lst_order += self.lst_cancel_order_sltp # CEDE to be confirmed

        return lst_of_lst_order

    def get_order_trailer_list(self, symbol, df_grid, trend, lst_recoded_trigger_executed):
        lst_order = []
        for orderId in lst_recoded_trigger_executed:
            order_to_execute = {
                "strategy_id": self.strategy_id,
                "symbol": symbol,
                "trend": trend,
                "trigger_type": "SL_TP_TRAILER",
                "triggered_orderId": orderId
            }
            condition_long = df_grid['orderId_trigger_long'] == orderId
            condition_short = df_grid['orderId_trigger_short'] == orderId

            if condition_long.any():
                order_to_execute["type"] = "OPEN_LONG_ORDER"
                trigger_price = df_grid.loc[condition_long, 'position'].values[0]
                grid_id = df_grid.loc[condition_long, 'grid_id'].values[0]
                size = df_grid.loc[condition_long, 'buying_size'].values[0]
            elif condition_short.any():
                order_to_execute["type"] = "OPEN_SHORT_ORDER"
                trigger_price = df_grid.loc[condition_short, 'position'].values[0]
                grid_id = df_grid.loc[condition_short, 'grid_id'].values[0]
                size = df_grid.loc[condition_short, 'buying_size'].values[0]

            if "type" in order_to_execute:
                order_to_execute.update({
                    "trigger_price": trigger_price,
                    "grid_id": grid_id,
                    "gross_size": size,
                    "trade_status": "pending",
                    "range_rate": self.percent_per_grid
                })
                lst_order.append(order_to_execute)

        return lst_order

    def get_order_trigger_list(self, symbol, df_grid, trend, lst_grid_id):
        # Create orders
        lst_order = []
        for grid_id in lst_grid_id:
            order_to_execute = {
                "strategy_id": self.strategy_id,
                "symbol": symbol,
                "trend": trend,
                "trigger_type": "TRIGGER"
            }
            side = df_grid.loc[df_grid["grid_id"] == grid_id, 'side'].values[0]
            if side == "trigger_long":
                order_to_execute["type"] = "OPEN_LONG_ORDER"
                if trend == "up":
                    order_to_execute["SL"] = self.grid_high
                elif trend == "down":
                    order_to_execute["TP"] = self.grid_low
            elif side == "trigger_short":
                order_to_execute["type"] = "OPEN_SHORT_ORDER"
                if trend == "up":
                    order_to_execute["TP"] = self.grid_high
                elif trend == "down":
                    order_to_execute["SL"] = self.grid_low

            if "type" in order_to_execute:
                order_to_execute["trigger_price"] = df_grid.loc[df_grid["grid_id"] == grid_id, 'position'].values[0]
                order_to_execute["gross_size"] = df_grid.loc[df_grid["grid_id"] == grid_id, 'buying_size'].values[0]
                order_to_execute["grid_id"] = grid_id
                order_to_execute["trade_status"] = "pending"
                order_to_execute["range_rate"] = self.percent_per_grid
                df_grid.loc[df_grid["grid_id"] == grid_id, 'status'] = 'pending'
                lst_order.append(order_to_execute)

        return lst_order

    def get_order_cancel_list(self, symbol, df_grid, trend, lst_grid_id):
        lst_cancel_order = []
        for grid_id_val in lst_grid_id:
            # Create orders
            orderId = self.get_orderId(df_grid, grid_id_val)
            if orderId != "" \
                    and orderId != None:
                order_to_execute = {
                    "strategy_id": self.strategy_id,
                    "grid_id": grid_id_val,
                    "symbol": symbol,
                    "orderId": orderId,
                    "trend": trend,
                    "trade_status": "pending",
                    "type": "CANCEL_ORDER",
                    "trigger_type": "CANCEL_TRIGGER"
                }
                lst_cancel_order.append(order_to_execute)

        return lst_cancel_order

    def get_grid_id(self, df_grid, orderId):
        # Check for the orderId in 'orderId_trigger_short'
        short_match = df_grid.loc[df_grid['orderId_trigger_short'] == orderId, 'grid_id']

        # Check for the orderId in 'orderId_trigger_long'
        long_match = df_grid.loc[df_grid['orderId_trigger_long'] == orderId, 'grid_id']

        if not short_match.empty:
            return_val = short_match.values[0]
            if isinstance(return_val, str):
                return_val = int(return_val)
            return return_val
        elif not long_match.empty:
            return_val = long_match.values[0]
            if isinstance(return_val, str):
                return_val = int(return_val)
            return return_val
        else:
            return None

    def get_orderId(self, df_grid, grid_id):
        columns = ['orderId_trigger_short', 'orderId_trigger_long', 'orderId_trailer_short', 'orderId_trailer_long']

        for column in columns:
            match = df_grid.loc[df_grid['grid_id'] == grid_id, column]
            if not match.empty \
                    and str(match.values[0]) != '':
                return str(match.values[0])

        return None

    def get_free_spot_trigger(self, df_grid, trend):
        condition_0 = df_grid['grid_id'] != 0
        condition_trailer_long_free = df_grid['status_trailer_long'] == 'empty'
        condition_trailer_short_free = df_grid['status_trailer_short'] == 'empty'
        condition_trigger_long_free = df_grid['status_trigger_long'] == 'empty'
        condition_trigger_short_free = df_grid['status_trigger_short'] == 'empty'
        condition_side_long = df_grid['side'] == 'trigger_long'
        condition_side_short = df_grid['side'] == 'trigger_short'
        condition_side_on_edge = df_grid['side'] != 'on_edge'

        if trend == "up":
            # free_spot_long
            df_grid_free_spot_long = df_grid[condition_0
                                             & condition_side_on_edge
                                             & condition_side_long
                                             & condition_trigger_long_free
                                             & condition_trailer_long_free
                                             & condition_trigger_short_free
                                             & condition_trailer_short_free].copy()
            lst_grid_free_spot_long = df_grid_free_spot_long['grid_id'].to_list()

            # df_grid_engaged_long = df_grid[condition_trigger_long].copy()
            # trigger_grid_id_max_long = max(df_grid_engaged_long['grid_id'].to_list())

            if len(self.trend_data[trend]['lst_gridId_open_triggers_long']) == 0:
                self.lst_grid_free_spot_long = heapq.nsmallest(self.nb_grid, lst_grid_free_spot_long)
            else:
                trigger_grid_id_max_long = max(self.trend_data[trend]['lst_gridId_open_triggers_long'])
                self.lst_grid_free_spot_long = [grid_id for grid_id in lst_grid_free_spot_long if grid_id <= trigger_grid_id_max_long]
            self.nb_free_spot_long = len(self.lst_grid_free_spot_long)

            # free_spot_short
            df_grid_free_spot_short = df_grid[condition_0
                                              & condition_side_on_edge
                                              & condition_side_short
                                              & condition_trigger_short_free
                                              & condition_trailer_short_free
                                              & condition_trigger_long_free
                                              & condition_trailer_long_free].copy()
            lst_grid_free_spot_short = df_grid_free_spot_short['grid_id'].to_list()

            # df_grid_engaged_short = df_grid[condition_trigger_short].copy()
            # trigger_grid_id_min_short = min(df_grid_engaged_short['grid_id'].to_list())

            if len(self.trend_data[trend]['lst_gridId_open_triggers_short']) == 0:
                self.lst_grid_free_spot_short = heapq.nlargest(self.nb_grid, lst_grid_free_spot_short)
            else:
                trigger_grid_id_min_short = min(self.trend_data[trend]['lst_gridId_open_triggers_short'])
                self.lst_grid_free_spot_short = [grid_id for grid_id in lst_grid_free_spot_short if grid_id >= trigger_grid_id_min_short]
            self.nb_free_spot_short = len(self.lst_grid_free_spot_short)

        elif trend == "down":
            # free_spot_long
            df_grid_free_spot_long = df_grid[condition_0
                                              & condition_side_on_edge
                                              & condition_side_long
                                              & condition_trigger_short_free
                                              & condition_trailer_short_free
                                              & condition_trigger_long_free
                                              & condition_trailer_long_free].copy()
            lst_grid_free_spot_long = df_grid_free_spot_long['grid_id'].to_list()

            # df_grid_engaged_long = df_grid[condition_trigger_long].copy()
            # trigger_grid_id_min_long = min(df_grid_engaged_long['grid_id'].to_list())

            if len(self.trend_data[trend]['lst_gridId_open_triggers_long']) == 0:
                self.lst_grid_free_spot_long = heapq.nlargest(self.nb_grid, lst_grid_free_spot_long)
            else:
                trigger_grid_id_min_long = min(self.trend_data[trend]['lst_gridId_open_triggers_long'])
                self.lst_grid_free_spot_long = [grid_id for grid_id in lst_grid_free_spot_long if grid_id >= trigger_grid_id_min_long]

            self.nb_free_spot_long = len(self.lst_grid_free_spot_long)
            if self.nb_free_spot_long > 0:
                print("toto")

            # free_spot_short
            df_grid_free_spot_short = df_grid[condition_0
                                              & condition_side_on_edge
                                              & condition_side_short
                                              & condition_trigger_short_free
                                              & condition_trailer_short_free
                                              & condition_trigger_long_free
                                              & condition_trailer_long_free].copy()
            lst_grid_free_spot_short = df_grid_free_spot_short['grid_id'].to_list()

            # df_grid_engaged_short = df_grid[condition_trigger_short].copy()
            # trigger_grid_id_max_short = max(df_grid_engaged_short['grid_id'].to_list())

            if len(self.trend_data[trend]['lst_gridId_open_triggers_short']) == 0:
                self.lst_grid_free_spot_short = heapq.nsmallest(self.nb_grid, lst_grid_free_spot_short)
            else:
                trigger_grid_id_max_short = max(self.trend_data[trend]['lst_gridId_open_triggers_short'])
                self.lst_grid_free_spot_short = [grid_id for grid_id in lst_grid_free_spot_short if grid_id <= trigger_grid_id_max_short]
            self.nb_free_spot_short = len(self.lst_grid_free_spot_short)

    def update_executed_trade_status(self, symbol, lst_orders):
        self.record_trigger_opened = []
        if len(lst_orders) > 0:
            for trend in self.lst_trend:
                df_grid = self.grid[symbol][trend]
                for order in lst_orders:
                    if "strategy_id" in order \
                            and order["strategy_id"] == self.strategy_id \
                            and "trend" in order \
                            and trend == order["trend"]:
                        grid_id = order["grid_id"]
                        if "trade_status" in order \
                                and order["trade_status"] == "SUCCESS":
                            if "trigger_type" in order \
                                    and order["trigger_type"] == "TRIGGER":
                                order_trigger = {}
                                order_trigger["trigger_type"] = "TRIGGER"
                                order_trigger["trade_status"] = "SUCCESS"
                                order_trigger["strategy_id"] = self.strategy_id
                                order_trigger["grid_id"] = grid_id
                                order_trigger["orderId"] = order["orderId"]
                                order_trigger["trend"] = trend
                                df_grid.loc[df_grid["grid_id"] == grid_id, "status"] = "engaged"
                                if "LONG" in order["type"]:
                                    df_grid.loc[df_grid["grid_id"] == grid_id, "orderId_trigger_long"] = order["orderId"]
                                    df_grid.loc[df_grid["grid_id"] == grid_id, "trigger_long"] = True
                                    df_grid.loc[df_grid["grid_id"] == grid_id, "status_trigger_long"] = "engaged"
                                    order_trigger["trigger_side"] = "long"
                                elif "SHORT" in order["type"]:
                                    df_grid.loc[df_grid["grid_id"] == grid_id, "orderId_trigger_short"] = order["orderId"]
                                    df_grid.loc[df_grid["grid_id"] == grid_id, "trigger_short"] = True
                                    df_grid.loc[df_grid["grid_id"] == grid_id, "status_trigger_short"] = "engaged"
                                    order_trigger["trigger_side"] = "short"
                                self.record_trigger_opened.append(order_trigger)
                            elif "trigger_type" in order \
                                    and order["trigger_type"] == "SL_TP_TRAILER":
                                order_trailer = {}
                                order_trailer["trigger_type"] = "SL_TP_TRAILER"
                                order_trailer["trade_status"] = "SUCCESS"
                                order_trailer["strategy_id"] = self.strategy_id
                                order_trailer["grid_id"] = grid_id
                                order_trailer["orderId"] = order["orderId"]
                                order_trailer["trend"] = trend
                                if "LONG" in order["type"] \
                                        and "orderId" in order \
                                        and "triggered_orderId" in order:
                                    order_trailer["trigger_side"] = "long"
                                    condition = df_grid['orderId_trigger_long'] == order["triggered_orderId"]
                                    df_grid.loc[condition, ['orderId_trigger_long', 'trigger_long', 'status_trigger_long']] = ['',
                                                                                                                               False,
                                                                                                                               'empty']
                                    df_grid.loc[condition, ['orderId_trailer_long', 'trailer_long', 'status_trailer_long']] = [order["orderId"],
                                                                                                                               True,
                                                                                                                               'engaged']
                                elif "SHORT" in order["type"] \
                                        and "orderId" in order \
                                        and "triggered_orderId" in order:
                                    order_trailer["trigger_side"] = "short"
                                    condition = df_grid['orderId_trigger_short'] == order["triggered_orderId"]
                                    df_grid.loc[condition, ['orderId_trigger_short', 'trigger_short', 'status_trigger_short']] = ['',
                                                                                                                                  False,
                                                                                                                                  'empty']
                                    df_grid.loc[condition, ['orderId_trailer_short', 'trailer_short', 'status_trailer_short']] = [order["orderId"],
                                                                                                                                  True,
                                                                                                                                  'engaged']
                                self.record_trigger_opened.append(order_trigger)

                                if order["triggered_orderId"] in self.lst_recoded_trigger_executed:
                                    print("orderId triggered: ", self.lst_recoded_trigger_executed)
                                    self.lst_recoded_trigger_executed.remove(order["triggered_orderId"])
                                    print("remaining trigger orderId triggered: ", self.lst_recoded_trigger_executed)
                                else:
                                    print("trigger orderId triggered missing")
                            elif "trigger_type" in order \
                                    and order["trigger_type"] == "CANCEL_TRIGGER":
                                condition = df_grid['orderId_trigger_short'] == order["orderId"]
                                df_grid.loc[condition, ['orderId_trigger_short', 'trigger_short', 'status_trigger_short', 'status']] = ['',
                                                                                                                                        False,
                                                                                                                                        'empty',
                                                                                                                                        'empty']
                                condition = df_grid['orderId_trigger_long'] == order["orderId"]
                                df_grid.loc[condition, ['orderId_trigger_long', 'trigger_long', 'status_trigger_long', 'status']] = ['',
                                                                                                                                     False,
                                                                                                                                     'empty',
                                                                                                                                     'empty']
                        elif order["trade_status"] == "FAILED" \
                                or order["trade_status"] == "MISSING":
                            if df_grid.loc[df_grid["grid_id"] == grid_id, "status"].values[0] != "engaged":
                                df_grid.loc[df_grid["grid_id"] == grid_id, "status"] = "empty"
                                df_grid.loc[df_grid["grid_id"] == grid_id, "orderId"] = ""
                            if "LONG" in order["type"]:
                                df_grid.loc[df_grid["grid_id"] == grid_id, "nb_position_long"] = 0
                                df_grid.loc[df_grid["grid_id"] == grid_id, "triggered_long"] = False
                                df_grid.loc[df_grid["grid_id"] == grid_id, "orderId_long"] = ""
                            elif "SHORT" in order["type"]:
                                df_grid.loc[df_grid["grid_id"] == grid_id, "nb_position_short"] = 0
                                df_grid.loc[df_grid["grid_id"] == grid_id, "triggered_short"] = False
                                df_grid.loc[df_grid["grid_id"] == grid_id, "orderId_short"] = ""
                        elif order["trade_status"] == "UNKNOWN":
                            df_grid.loc[df_grid["grid_id"] == grid_id, "unknown"] = True

    def clear_orderId(self, symbol, grid_id, trend):
        df = self.grid[symbol][trend]
        df.loc[df['grid_id'] == grid_id, 'orderId'] = ""
        del df

    def print_grid(self):
        if self.zero_print:
            return
        for symbol in self.lst_symbols:
            df_grid = self.grid[symbol]
            self.log("\n" + df_grid.to_string(index=False))
            del df_grid

    def normalize_grid_price(self, symbol, pricePlace, priceEndStep, sizeMultiplier):
        for trend in self.lst_trend:
            df = self.grid[symbol][trend]
            df['position'] = df['position'].apply(lambda x: utils.normalize_price(x, pricePlace, priceEndStep))
            # self.log("grid price normalized: " + str(df['position'].tolist()))

            df['buying_size'] = df['buying_size'].apply(lambda x: utils.normalize_size(x, sizeMultiplier))
            # self.log("grid size normalized: " + str(df['buying_size'].tolist()))

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

    def set_triggers_executed(self, symbol, df_open_triggers):
        df_grid_tend = self.grid[symbol]
        condition_strategy_id = df_open_triggers['strategyId'] == self.strategy_id
        condition_trigger_symbol = df_open_triggers['symbol'] == symbol

        # Define conditions for strategyId update
        condition_tp = df_open_triggers["planType"] == "profit_plan"
        condition_sl = df_open_triggers["planType"] == "loss_plan"
        condition_trend = df_open_triggers["trend"].isna()
        condition_strategyid = df_open_triggers["strategyId"].isna()
        df_open_triggers['triggerPrice'] = df_open_triggers['triggerPrice'].astype(float)
        condition_up = df_open_triggers["triggerPrice"] == self.grid_high
        condition_down = df_open_triggers["triggerPrice"] == self.grid_low

        combined_condition = (condition_sl | condition_tp) & condition_trend & condition_strategyid

        lst_order_Ids_new_SL_TP = df_open_triggers.loc[combined_condition, 'orderId'].tolist()

        # Update strategyId for the SL condition
        df_open_triggers.loc[combined_condition, 'strategyId'] = self.strategy_id

        # Define combined conditions for triggerPrice and planType
        condition_grid_high = (condition_up) & (df_open_triggers["planType"].isin(["profit_plan", "loss_plan"])) & condition_trend
        condition_grid_low = (condition_down) & (df_open_triggers["planType"].isin(["profit_plan", "loss_plan"])) & condition_trend

        # Update trend based on combined conditions
        df_open_triggers.loc[condition_grid_high, "trend"] = 'up'
        df_open_triggers.loc[condition_grid_low, "trend"] = 'down'

        self.lst_of_data_record = []
        if len(lst_order_Ids_new_SL_TP) > 0:
            condition_new_SL_TP = df_open_triggers["orderId"].isin(lst_order_Ids_new_SL_TP)
            # Extract matching rows
            matching_rows = df_open_triggers.loc[condition_new_SL_TP, ['gridId', 'orderId', 'trend', 'strategyId']]
            # Convert to list of dictionaries
            list_of_dicts = matching_rows.to_dict(orient='records')
            for item in list_of_dicts:
                item['type'] = 'RECORD_DATA'
            self.lst_of_data_record = list_of_dicts

        df_open_triggers = df_open_triggers[condition_strategy_id & condition_trigger_symbol]
        for trend in self.lst_trend:
            self.trend_data[trend] = {}
            condition_trigger_trend = df_open_triggers['trend'] == trend
            condition_trigger_trailer_open = df_open_triggers['planStatus'] != 'executed'
            condition_trigger_not_canceled = df_open_triggers['planStatus'] != 'cancelled'
            condition_trigger_canceled = df_open_triggers['planStatus'] == 'cancelled'
            condition_trigger_trailer_executed = df_open_triggers['planStatus'] == 'executed'
            condition_trigger_plan = df_open_triggers['planType'] == 'normal_plan'
            condition_trigger_plan_TP = df_open_triggers['planType'] == 'profit_plan'
            condition_trigger_plan_SL = df_open_triggers['planType'] == 'loss_plan'
            condition_trigger_moving_plan = df_open_triggers['planType'] == 'moving_plan'
            condition_profit_loss = df_open_triggers['planType'] == 'profit_loss'
            condition_long = df_open_triggers['side'] == 'buy'
            condition_short = df_open_triggers['side'] == 'sell'

            self.trend_data[trend]['df_canceled_triggers'] = df_open_triggers[condition_trigger_trailer_open
                                                                              & condition_trigger_plan
                                                                              & condition_trigger_trend
                                                                              & condition_trigger_canceled].copy()
            if len(self.trend_data[trend]['df_canceled_triggers']) > 0:
                df_grid = df_grid_tend[trend]
                lst_canceled_order_id = self.trend_data[trend]['df_canceled_triggers']["orderId"].to_list()
                for canceled_order_id in lst_canceled_order_id:
                    condition = df_grid['orderId_trigger_short'] == canceled_order_id
                    df_grid.loc[condition, ['orderId_trigger_short', 'trigger_short', 'status_trigger_short', 'status']] = ['',
                                                                                                                            False,
                                                                                                                            'empty',
                                                                                                                            'empty']
                    condition = df_grid['orderId_trigger_long'] == canceled_order_id
                    df_grid.loc[condition, ['orderId_trigger_long', 'trigger_long', 'status_trigger_long', 'status']] = ['',
                                                                                                                         False,
                                                                                                                         'empty',
                                                                                                                         'empty']

            self.trend_data[trend]['df_open_triggers'] = df_open_triggers[condition_trigger_trailer_open
                                                                          & condition_trigger_not_canceled
                                                                          & condition_trigger_plan
                                                                          & condition_trigger_trend].copy()
            self.trend_data[trend]['nb_open_triggers'] = len(self.trend_data[trend]['df_open_triggers'])
            self.trend_data[trend]['lst_gridId_open_triggers'] = list(map(int, self.trend_data[trend]['df_open_triggers']["gridId"].to_list()))
            self.trend_data[trend]['lst_orderId_open_triggers'] = self.trend_data[trend]['df_open_triggers']["orderId"].to_list()

            self.trend_data[trend]['df_open_triggers_long'] = df_open_triggers[condition_long
                                                                               & condition_trigger_trailer_open
                                                                               & condition_trigger_not_canceled
                                                                               & condition_trigger_plan
                                                                               & condition_trigger_trend].copy()
            self.trend_data[trend]['nb_open_triggers_long'] = len(self.trend_data[trend]['df_open_triggers_long'])
            self.trend_data[trend]['lst_gridId_open_triggers_long'] = list(map(int, self.trend_data[trend]['df_open_triggers_long']["gridId"].to_list()))
            self.trend_data[trend]['lst_orderId_open_triggers_long'] = self.trend_data[trend]['df_open_triggers_long']["orderId"].to_list()

            self.trend_data[trend]['df_open_triggers_short'] = df_open_triggers[condition_short
                                                                                & condition_trigger_trailer_open
                                                                                & condition_trigger_not_canceled
                                                                                & condition_trigger_plan
                                                                                & condition_trigger_trend].copy()
            self.trend_data[trend]['nb_open_triggers_short'] = len(self.trend_data[trend]['df_open_triggers_short'])
            self.trend_data[trend]['lst_gridId_open_triggers_short'] = list(map(int, self.trend_data[trend]['df_open_triggers_short']["gridId"].to_list()))
            self.trend_data[trend]['lst_orderId_open_triggers_short'] = self.trend_data[trend]['df_open_triggers_short']["orderId"].to_list()

            self.trend_data[trend]['df_open_trailers'] = df_open_triggers[condition_trigger_moving_plan
                                                                          & condition_trigger_trend].copy()
            self.trend_data[trend]['nb_open_trailers'] = len(self.trend_data[trend]['df_open_trailers'])
            self.trend_data[trend]['lst_gridId_open_trailers'] = list(map(int, self.trend_data[trend]['df_open_trailers']["gridId"].to_list()))
            self.trend_data[trend]['lst_orderId_open_trailers'] = self.trend_data[trend]['df_open_trailers']["orderId"].to_list()

            self.trend_data[trend]['df_open_trailers_long'] = df_open_triggers[condition_long
                                                                               & condition_trigger_moving_plan
                                                                               & condition_trigger_trend].copy()
            self.trend_data[trend]['nb_open_trailers_long'] = len(self.trend_data[trend]['df_open_trailers_long'])
            self.trend_data[trend]['lst_gridId_open_trailers_long'] = list(map(int, self.trend_data[trend]['df_open_trailers_long']["gridId"].to_list()))
            self.trend_data[trend]['lst_orderId_open_trailers_long'] = self.trend_data[trend]['df_open_trailers_long']["orderId"].to_list()

            self.trend_data[trend]['df_open_trailers_short'] = df_open_triggers[condition_short
                                                                                & condition_trigger_moving_plan
                                                                                & condition_trigger_trend].copy()
            self.trend_data[trend]['nb_open_trailers_short'] = len(self.trend_data[trend]['df_open_trailers_short'])
            self.trend_data[trend]['lst_gridId_open_trailers_short'] = list(map(int, self.trend_data[trend]['df_open_trailers_short']["gridId"].to_list()))
            self.trend_data[trend]['lst_orderId_open_trailers_short'] = self.trend_data[trend]['df_open_trailers_short']["orderId"].to_list()

            if self.df_previous_open_trailers.get(trend) is not None:
                nb_open_trailers = self.trend_data[trend]['nb_open_trailers']
                len_previous_open_trailers = len(self.df_previous_open_trailers[trend])

                if nb_open_trailers > len_previous_open_trailers:
                    self.trend_data[trend]['nb_new_open_trailers'] = nb_open_trailers - len_previous_open_trailers
                else:
                    self.trend_data[trend]['nb_new_open_trailers'] = 0

                # Find new rows in df_open_trailers that are not in df_previous_open_trailers
                df_open_trailers = self.trend_data[trend]['df_open_trailers']
                df_previous_open_trailers = self.df_previous_open_trailers[trend]

                # Assuming there's a unique identifier column, e.g., 'id'
                new_open_trailers = df_open_trailers.loc[~df_open_trailers['orderId'].isin(df_previous_open_trailers['orderId'])]

                # Assign the new rows to df_new_open_trailers
                self.trend_data[trend]['df_new_open_trailers'] = new_open_trailers.copy()
                self.trend_data[trend]['lst_gridId_new_open_trailers'] = list(map(int, self.trend_data[trend]['df_new_open_trailers']["gridId"].to_list()))
                self.trend_data[trend]['lst_orderId_new_open_trailers'] = self.trend_data[trend]['df_new_open_trailers']["orderId"].to_list()
            else:
                self.trend_data[trend]['nb_new_open_trailers'] = 0
                self.trend_data[trend]['df_new_open_trailers'] = pd.DataFrame()

            lst_fast_trigger_triggered_order_id = []
            lst_fast_trigger_triggered_grid_id = []
            lst_fast_trigger_triggered_order_id_long = []
            lst_fast_trigger_triggered_grid_id_long = []
            lst_fast_trigger_triggered_order_id_short = []
            lst_fast_trigger_triggered_grid_id_short = []
            for order in self.record_trigger_opened:
                if (order['strategy_id'] == self.strategy_id
                        and order['trend'] == trend
                        and order['trade_status'] == "SUCCESS"
                        and order['trigger_type'] == "TRIGGER"
                        and not (order["orderId"] in df_open_triggers['orderId'].to_list())):
                    lst_fast_trigger_triggered_order_id.append(order["orderId"])
                    lst_fast_trigger_triggered_grid_id.append(order["grid_id"])
                    if order['trigger_side'] == "long":
                        lst_fast_trigger_triggered_order_id_long.append(order["orderId"])
                        lst_fast_trigger_triggered_grid_id_long.append(order["grid_id"])
                    elif order['trigger_side'] == "short":
                        lst_fast_trigger_triggered_order_id_short.append(order["orderId"])
                        lst_fast_trigger_triggered_grid_id_short.append(order["grid_id"])
                    print("===========================    WARNING FAST TRIGGER TRIGGERED   ================================")

            self.df_previous_open_trailers[trend] = self.trend_data[trend]['df_open_trailers'].copy()

            self.trend_data[trend]['df_executed_trigger'] = df_open_triggers[condition_trigger_trailer_executed
                                                                             & condition_trigger_trend
                                                                             & condition_trigger_plan].copy()

            self.trend_data[trend]['df_executed_trigger_debug'] = self.trend_data[trend]['df_executed_trigger']

            self.trend_data[trend]['nb_executed_trigger'] = len(self.trend_data[trend]['df_executed_trigger']) + len(lst_fast_trigger_triggered_order_id)
            self.trend_data[trend]['lst_gridId_executed_trigger'] = list(map(int, self.trend_data[trend]['df_executed_trigger']["gridId"].to_list())) + lst_fast_trigger_triggered_grid_id
            self.trend_data[trend]['lst_orderId_executed_trigger'] = self.trend_data[trend]['df_executed_trigger']["orderId"].to_list() + lst_fast_trigger_triggered_order_id

            self.trend_data[trend]['df_executed_trigger_long'] = df_open_triggers[condition_long
                                                                                  & condition_trigger_trailer_executed
                                                                                  & condition_trigger_trend
                                                                                  & condition_trigger_plan].copy()
            self.trend_data[trend]['nb_executed_trigger_long'] = len(self.trend_data[trend]['df_executed_trigger_long']) + len(lst_fast_trigger_triggered_order_id_long)
            self.trend_data[trend]['lst_gridId_executed_trigger_long'] = list(map(int, self.trend_data[trend]['df_executed_trigger_long']["gridId"].to_list())) + lst_fast_trigger_triggered_grid_id_long
            self.trend_data[trend]['lst_orderId_executed_trigger_long'] = self.trend_data[trend]['df_executed_trigger_long']["orderId"].to_list() + lst_fast_trigger_triggered_order_id_long

            self.trend_data[trend]['df_executed_trigger_short'] = df_open_triggers[condition_short
                                                                                  & condition_trigger_trailer_executed
                                                                                  & condition_trigger_trend
                                                                                  & condition_trigger_plan].copy()
            self.trend_data[trend]['nb_executed_trigger_short'] = len(self.trend_data[trend]['df_executed_trigger_short']) + len(lst_fast_trigger_triggered_order_id_short)
            self.trend_data[trend]['lst_gridId_executed_trigger_short'] = list(map(int, self.trend_data[trend]['df_executed_trigger_short']["gridId"].to_list())) + lst_fast_trigger_triggered_grid_id_short
            self.trend_data[trend]['lst_orderId_executed_trigger_short'] = self.trend_data[trend]['df_executed_trigger_short']["orderId"].to_list() + lst_fast_trigger_triggered_order_id_short

            if len(self.trend_data[trend]['lst_orderId_executed_trigger']) > 0:
                self.lst_recoded_trigger_executed += self.trend_data[trend]['lst_orderId_executed_trigger']
                print(trend, " - add orderId to lst_recoded_trigger_executed ", self.trend_data[trend]['lst_orderId_executed_trigger'])

            self.trend_data[trend]['df_executed_trailer'] = df_open_triggers[condition_trigger_trailer_executed
                                                                             & condition_trigger_trend
                                                                             & condition_profit_loss].copy()
            self.trend_data[trend]['nb_executed_trailer'] = len(self.trend_data[trend]['df_executed_trailer'])
            self.trend_data[trend]['lst_gridId_executed_trailer'] = list(map(int, self.trend_data[trend]['df_executed_trailer']["gridId"].to_list()))
            self.trend_data[trend]['lst_orderId_executed_trailer'] = self.trend_data[trend]['df_executed_trailer']["orderId"].to_list()
            if len(self.trend_data[trend]['lst_orderId_executed_trailer']):
                print("nb executed trailers: ", len(self.trend_data[trend]['lst_orderId_executed_trailer']))

            # SL TP Orders
            self.lst_cancel_order_sltp = []
            nb_new_SLTP = len(self.lst_of_data_record)
            if nb_new_SLTP == 0 \
                    and self.trend_data[trend]['nb_executed_trailer'] == 0 \
                    and self.trend_data[trend]['nb_executed_trigger'] == 0:
                condition_trigger_moving_plan_long = df_open_triggers['side'] == 'buy'
                condition_trigger_moving_plan_short = df_open_triggers['side'] == 'sell'
                self.trend_data[trend]['df_open_SL_order'] = df_open_triggers[condition_trigger_plan_SL
                                                                              & condition_trigger_trend].copy()
                self.trend_data[trend]['nb_open_SL_order'] = len(self.trend_data[trend]['df_open_SL_order'])

                self.trend_data[trend]['df_open_TP_order'] = df_open_triggers[condition_trigger_plan_TP
                                                                              & condition_trigger_trend].copy()
                self.trend_data[trend]['nb_open_TP_order'] = len(self.trend_data[trend]['df_open_TP_order'])

                self.trend_data[trend]['df_open_trailers_order_long'] = df_open_triggers[condition_trigger_moving_plan
                                                                                         & condition_trigger_moving_plan_long
                                                                                         & condition_trigger_trend].copy()
                self.trend_data[trend]['nb_open_trailers_order_long'] = len(self.trend_data[trend]['df_open_trailers_order_long'])

                self.trend_data[trend]['df_open_trailers_order_short'] = df_open_triggers[condition_trigger_moving_plan
                                                                                          & condition_trigger_moving_plan_short
                                                                                          & condition_trigger_trend].copy()
                self.trend_data[trend]['nb_open_trailers_order_short'] = len(self.trend_data[trend]['df_open_trailers_order_short'])

                if trend == "up":
                    # Cancel SL orders
                    if self.trend_data[trend]['nb_open_SL_order'] > self.trend_data[trend]['nb_open_trailers_order_long'] \
                            and self.trend_data[trend]['nb_new_open_trailers'] == 0:
                        nb_to_cancel = self.trend_data[trend]['nb_open_SL_order'] - self.trend_data[trend]['nb_open_trailers_order_long']
                        df_sl = self.trend_data[trend]['df_open_SL_order']
                        df_trailers_long = self.trend_data[trend]['df_open_trailers_order_long']
                        self.lst_cancel_order_sltp += self.get_orders_to_cancel(df_sl, df_trailers_long, nb_to_cancel)

                    # Cancel TP orders
                    if self.trend_data[trend]['nb_open_TP_order'] > self.trend_data[trend]['nb_open_trailers_order_short'] \
                            and self.trend_data[trend]['nb_new_open_trailers'] == 0:
                        nb_to_cancel = self.trend_data[trend]['nb_open_TP_order'] - self.trend_data[trend]['nb_open_trailers_order_short']
                        df_tp = self.trend_data[trend]['df_open_TP_order']
                        df_trailers_short = self.trend_data[trend]['df_open_trailers_order_short']
                        self.lst_cancel_order_sltp += self.get_orders_to_cancel(df_tp, df_trailers_short, nb_to_cancel)

                elif trend == "down":
                    # Cancel SL orders
                    if self.trend_data[trend]['nb_open_SL_order'] > self.trend_data[trend]['nb_open_trailers_order_short'] \
                            and self.trend_data[trend]['nb_new_open_trailers'] == 0:
                        nb_to_cancel = self.trend_data[trend]['nb_open_SL_order'] - self.trend_data[trend]['nb_open_trailers_order_short']
                        df_sl = self.trend_data[trend]['df_open_SL_order']
                        df_trailers_short = self.trend_data[trend]['df_open_trailers_order_short']
                        self.lst_cancel_order_sltp += self.get_orders_to_cancel(df_sl, df_trailers_short, nb_to_cancel)

                    # Cancel TP orders
                    if self.trend_data[trend]['nb_open_TP_order'] > self.trend_data[trend]['nb_open_trailers_order_long'] \
                            and self.trend_data[trend]['nb_new_open_trailers'] == 0:
                        nb_to_cancel = self.trend_data[trend]['nb_open_TP_order'] - self.trend_data[trend]['nb_open_trailers_order_long']
                        df_tp = self.trend_data[trend]['df_open_TP_order']
                        df_trailers_long = self.trend_data[trend]['df_open_trailers_order_long']
                        self.lst_cancel_order_sltp += self.get_orders_to_cancel(df_tp, df_trailers_long, nb_to_cancel)

                if len(self.lst_cancel_order_sltp) > 0:
                    list_of_dicts = self.transform_order_ids_to_dict(symbol, self.lst_cancel_order_sltp)
                    for order in list_of_dicts:
                        if order["orderId"] in self.trend_data[trend]['df_open_SL_order']["orderId"].to_list():
                            order["planType"] = "loss_plan"
                        elif order["orderId"] in self.trend_data[trend]['df_open_TP_order']["orderId"].to_list():
                            order["planType"] = "profit_plan"
                        else:
                            order["planType"] = ""
                    self.lst_cancel_order_sltp = list_of_dicts
            else:
                condition_trigger_moving_plan_long = df_open_triggers['side'] == 'buy'
                condition_trigger_moving_plan_short = df_open_triggers['side'] == 'sell'
                self.trend_data[trend]['df_open_SL_order'] = df_open_triggers[condition_trigger_plan_SL
                                                                              & condition_trigger_trend].copy()
                self.trend_data[trend]['nb_open_SL_order'] = len(self.trend_data[trend]['df_open_SL_order'])

                self.trend_data[trend]['df_open_TP_order'] = df_open_triggers[condition_trigger_plan_TP
                                                                              & condition_trigger_trend].copy()
                self.trend_data[trend]['nb_open_TP_order'] = len(self.trend_data[trend]['df_open_TP_order'])

                self.trend_data[trend]['df_open_trailers_order_long'] = df_open_triggers[condition_trigger_moving_plan
                                                                                         & condition_trigger_moving_plan_long
                                                                                         & condition_trigger_trend].copy()
                self.trend_data[trend]['nb_open_trailers_order_long'] = len(self.trend_data[trend]['df_open_trailers_order_long'])

                self.trend_data[trend]['df_open_trailers_order_short'] = df_open_triggers[condition_trigger_moving_plan
                                                                                          & condition_trigger_moving_plan_short
                                                                                          & condition_trigger_trend].copy()
                self.trend_data[trend]['nb_open_trailers_order_short'] = len(self.trend_data[trend]['df_open_trailers_order_short'])

    def transform_order_ids_to_dict(self, symbol, order_ids):
        if symbol == "XRPUSDT":
            symbol = "XRP"  # CEDE SYMBOL ISSUE to be fixed
        return [{'symbol': symbol, 'orderId': order_id, 'type': 'CANCEL_SLTP'} for order_id in order_ids]

    def get_orders_to_cancel(self, df_orders, df_trailers, nb_to_cancel):
        if len(df_trailers) == 0:
            return df_orders['orderId'].tolist()
        df_orders = df_orders.copy()
        df_orders['size'] = df_orders['size'].astype(int)
        df_trailers = df_trailers.copy()
        df_trailers['size'] = df_trailers['size'].astype(int)

        # Identify orders with different "size" values
        differing_orders = df_orders[~df_orders['size'].isin(df_trailers['size'])]

        if len(differing_orders) >= nb_to_cancel:
            # If we have enough differing orders, take the required number
            orders_to_cancel = differing_orders.head(nb_to_cancel)
        else:
            # Otherwise, take all differing orders and fill the rest with the smallest "size" orders
            orders_to_cancel = differing_orders
            remaining_to_cancel = nb_to_cancel - len(differing_orders)
            same_size_orders = df_orders[df_orders['size'].isin(df_trailers['size'])]
            smallest_orders = same_size_orders.nsmallest(remaining_to_cancel, 'size')
            orders_to_cancel = pd.concat([orders_to_cancel, smallest_orders])

        return orders_to_cancel['orderId'].tolist()
    
class GridCalculator:
    def __init__(self, grid_high, grid_low, percent_per_grid, nb_grid_up, nb_grid_down):
        self.grid_high = grid_high
        self.grid_low = grid_low
        self.percent_per_grid = percent_per_grid
        self.nb_grid_up = nb_grid_up
        self.nb_grid_down = nb_grid_down

    def calculate_grid_values(self):
        # Calculate lst_grid_values_up
        lst_grid_values_up = np.linspace(
            self.grid_high + self.grid_high * self.percent_per_grid / 100 * self.nb_grid_up,
            self.grid_high,
            self.nb_grid_up + 1,
            endpoint=True
        ).tolist()

        # Calculate the step based on the percentage per grid
        step_down = 1 - self.percent_per_grid / 100

        # Calculate lst_grid_values_down using geometric progression
        lst_grid_values_down = [self.grid_low * (step_down ** i) for i in range(self.nb_grid_down + 1)]

        return lst_grid_values_up, lst_grid_values_down
