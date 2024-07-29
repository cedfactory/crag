from . import rtdp, rtstr, rtctrl
import math
import pandas as pd
import numpy as np

from . import utils
from src import logger

class StrategyBreakoutTradingGenericV2(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        self.side = "long"
        if params:
            self.side = params.get("type", self.side)
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
        return "StrategyBreakoutTradingGenericV2"

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
        df_open_positions = current_state["open_positions"].copy()
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
            # df_open_triggers = self.grid.set_current_orders_price_to_grid(symbol, df_open_triggers)
            self.grid.set_buying_size(self.get_grid_buying_size(symbol, self.strategy_id))
            if symbol in df_price['symbols'].tolist():
                self.grid.set_triggers_executed(symbol, df_open_triggers)
                self.grid.set_current_price(df_price.loc[df_price['symbols'] == symbol, 'values'].values[0])
                self.grid.update_unknown_status(symbol)
                self.grid.cross_check_with_current_state(symbol)
                self.grid.update_grid_side(symbol)
            lst_order_to_execute = self.grid.get_order_list(symbol)

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
        self.df_price = current_state["prices"]
        self.grid.set_prices(self.df_price)
        lst_buying_market_order = []
        for symbol in self.lst_symbols:
            lst_buying_market_order = []
            self.buying_size = self.get_grid_buying_size(symbol, self.strategy_id)
            if symbol in self.df_price['symbols'].tolist():
                price = self.df_price.loc[self.df_price['symbols'] == symbol, 'values'].values[0]
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
        for symbol in df_symbol_size['symbol'].tolist():
            dol_per_grid = self.grid_margin / self.nb_grid
            size = dol_per_grid
            if (self.get_grid_buying_min_size(symbol) <= size)\
                    and (size > 5) \
                    and (cash >= self.grid_margin):

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
                exit(2)
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
        self.str_lst_symbol = ' '.join(map(str, lst_symbols))
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

        if False:
            self.debug_cpt = 0 # CEDE For Test

        self.max_grid_close_order = -1
        self.min_grid_close_order = -1
        self.nb_close_missing = -1
        self.buying_size = 0

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

        self.columns = ["grid_id", "close_grid_id", "position",
                        "side", "changes", "previous_side",
                        "orderId_trigger_long", "status_trigger_long", "trigger_long",
                        "orderId_trailer_long", "status_trailer_long", "trailer_long",
                        "status_long",
                        "orderId_trigger_short", "status_trigger_short", "trigger_short",
                        "orderId_trailer_short", "status_trailer_short", "trailer_short",
                        "status_short",
                        "status",
                        "cross_checked", "unknown"]
        grid_break_out = {key: pd.DataFrame(columns=self.columns) for key in self.lst_trend}
        self.grid = {key: grid_break_out for key in self.lst_symbols}
        for symbol in lst_symbols:
            for trend in self.lst_trend:
                self.grid[symbol][trend]["position"] = self.dct_lst_grid_values[trend]
                print(self.grid[symbol][trend]["position"].to_list())

                self.grid[symbol][trend]["grid_id"] = self.grid[symbol][trend].index
                sequence = self.grid[symbol][trend].index.tolist()[1:] + [-1]
                self.grid[symbol][trend]['close_grid_id'] = sequence
                del sequence
                for col in ["status_trigger_long", "status_trailer_long",
                            "status_trigger_short", "status_trailer_short",
                            "status_long", "status_short", "status"]:
                    self.grid[symbol][trend][col] = "empty"
                for col in ["orderId_trigger_long", "orderId_trailer_long",
                            "orderId_trigger_short", "orderId_trailer_short",
                            "side", "changes", "previous_side"]:
                    self.grid[symbol][trend][col] = ""
                for col in ["trigger_long", "trailer_long",
                            "trigger_short", "trailer_short",
                            "cross_checked", "unknown"]:
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

    def get_opposite_trend(self, trend):
        return "down" if trend == "up" else "up"

    def update_grid_side(self, symbol):
        for trend in self.lst_trend:
            df = self.grid[symbol][trend]

            df['previous_side'] = df['side']
            df.loc[df['position'] > self.current_price, 'side'] = self.side_mapping["up"]
            df.loc[df['position'] < self.current_price, 'side'] = self.side_mapping["down"]
            df.loc[df['position'] == self.current_price, 'side'] = "on_edge"

            # Compare if column1 and column2 are the same
            df['changes'] = df['previous_side'] != df['side']

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
        lst_of_lst_order = []
        size = self.buying_size
        for trend in self.lst_trend:
            df_grid = self.grid[symbol][trend]
            if self.trend_data[trend]['nb_executed_trigger'] > 0:
                lst_order = []

                # if len(self.trend_data[trend]['lst_orderId_executed_trigger']) > 0:
                if len(self.lst_recoded_trigger_executed) > 0:
                    # Create orders based on executed triggers
                    # for orderId in self.trend_data[trend]['lst_orderId_executed_trigger']:
                    for orderId in self.lst_recoded_trigger_executed:
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
                            df_grid.loc[df_grid["grid_id"] == grid_id, 'status'] = 'pending'
                            lst_order.append(order_to_execute)
                lst_of_lst_order = lst_of_lst_order + lst_order
            if self.get_cancel_trigger(df_grid, trend):
                # Create orders
                lst_order = []
                for orderId in self.lst_cancel_trigger_orderId:
                    grid_id_val = self.get_grid_id(df_grid, orderId)
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
                    lst_order.append(order_to_execute)
                lst_of_lst_order = lst_order
            else:
                df_grid = df_grid.drop(index=0).reset_index(drop=True)

                # Define conditions
                condition_side_long = df_grid['side'] == 'trigger_long'
                condition_side_short = df_grid['side'] == 'trigger_short'
                condition_empty_trigger_long = df_grid['status_trigger_long'] == 'empty'
                condition_empty_trigger_short = df_grid['status_trigger_short'] == 'empty'
                condition_empty_trailer_long = df_grid['status_trailer_long'] == 'empty'
                condition_empty_trailer_short = df_grid['status_trailer_short'] == 'empty'

                # Combine conditions
                condition_empty_long = condition_empty_trigger_long & condition_empty_trailer_long
                condition_empty_short = condition_empty_trigger_short & condition_empty_trailer_short

                # Calculate engaged and available grid slots
                nb_engaged_long = df_grid['trigger_long'].sum() + df_grid['trailer_long'].sum()
                nb_engaged_short = df_grid['trigger_short'].sum() + df_grid['trailer_short'].sum()

                # Get smallest grid IDs for long triggers
                df_trigger_long = df_grid[condition_side_long & condition_empty_long]
                n_long = self.nb_grid - nb_engaged_long
                if trend == "up":
                    df_trigger_long = utils.keep_n_smallest(df_trigger_long, "grid_id", n_long)
                elif trend == "down":
                    df_trigger_long = utils.keep_n_highest(df_trigger_long, "grid_id", n_long)
                lst_trigger_long = df_trigger_long["grid_id"].to_list()

                # Get smallest grid IDs for short triggers
                df_trigger_short = df_grid[condition_side_short & condition_empty_short]
                n_short = self.nb_grid - nb_engaged_short
                if trend == "up":
                    df_trigger_short = utils.keep_n_highest(df_trigger_short, "grid_id", n_short)
                elif trend == "down":
                    df_trigger_short = utils.keep_n_smallest(df_trigger_short, "grid_id", n_short)
                lst_trigger_short = df_trigger_short["grid_id"].to_list()

                # Combine and deduplicate grid IDs
                lst_order_grid_id = list(set(lst_trigger_long + lst_trigger_short))

                # Create orders
                lst_order = []
                for grid_id in lst_order_grid_id:
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
                lst_of_lst_order = lst_of_lst_order + lst_order

        lst_order = lst_of_lst_order + self.lst_of_data_record + self.lst_cancel_order_sltp
        """
        sorting_order = ['OPEN_LONG_ORDER', 'OPEN_SHORT_ORDER',
                         'CLOSE_LONG_ORDER', 'CLOSE_SHORT_ORDER',
                         "CANCEL_ORDER", "RECORD_DATA", "CANCEL_SLTP"]
        sorted_list = sorted(lst_order, key=lambda x: sorting_order.index(x['type']))
        """
        sorted_list = lst_order
        del df_grid
        # del sorting_order
        del lst_order
        del lst_of_lst_order

        return sorted_list

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

    def get_cancel_trigger(self, df_grid, trend):
        test_CANCEL_TRIGGER = False
        if test_CANCEL_TRIGGER:     # CEDE TO test CANCEL TRIGGER
            self.debug_cpt += 1
            if self.debug_cpt > 3:
                self.lst_cancel_trigger_orderId = self.trend_data[trend]['lst_gridId_open_triggers']
                return True

        self.lst_cancel_trigger_orderId = []
        condition_0 = df_grid['grid_id'] != 0
        condition_trigger_long = df_grid['status_trigger_long'] == 'engaged'
        condition_trigger_short = df_grid['status_trigger_short'] == 'engaged'
        condition_side_long = df_grid['side'] == 'trigger_long'
        condition_side_short = df_grid['side'] == 'trigger_short'

        df_grid_above_long = df_grid[condition_trigger_long
                                     & condition_side_short
                                     & condition_0].copy()
        if len(df_grid_above_long) > 0:
            self.lst_cancel_trigger_orderId += df_grid_above_long["orderId_trigger_long"].to_list()
        df_grid_below_short = df_grid[condition_trigger_short
                                      & condition_side_long
                                      & condition_0].copy()
        if len(df_grid_below_short) > 0:
            self.lst_cancel_trigger_orderId += df_grid_above_long["orderId_trigger_short"].to_list()
        nb_cancel_position = 0
        if len(self.lst_cancel_trigger_orderId) == nb_cancel_position:
            if trend == "up":
                # UP + LONG
                nb_open_triggers_long = df_grid['trigger_long'].sum()
                # if True \
                #         or self.trend_data[trend]['nb_open_triggers_long'] == self.nb_grid:
                if self.trend_data[trend]['nb_open_triggers_long'] > nb_cancel_position:
                    continue_cancel_case = False
                    condition_0 = df_grid['grid_id'] != 0
                    condition_engaged_trigger_long = df_grid['status_trigger_long'] == 'engaged'
                    condition_empty_trailer_long = df_grid['status_trailer_long'] == 'empty'
                    df_trigger_engaged_grid_id_min_long = df_grid[condition_engaged_trigger_long
                                                                  & condition_empty_trailer_long
                                                                  & condition_0].copy()
                    nb_trigger_engaged_grid_id_min_long = len(df_trigger_engaged_grid_id_min_long)
                    trigger_grid_id_min_long = df_trigger_engaged_grid_id_min_long['grid_id'].astype(int).min()

                    if len(self.trend_data[trend]['lst_gridId_open_triggers_long']) > 0:
                        trigger_grid_id_min_long = min(self.trend_data[trend]['lst_gridId_open_triggers_long'])

                    condition_empty_trigger_long = df_grid['status_trigger_long'] == 'empty'
                    condition_empty_trigger_short = df_grid['status_trigger_short'] == 'empty'
                    condition_side_long = df_grid['side'] == 'trigger_long'
                    df_grid_empty_long = df_grid[condition_empty_trigger_long
                                                 & condition_empty_trigger_short
                                                 & condition_side_long
                                                 & condition_0].copy()
                    if len(df_grid_empty_long) > 0:
                        grid_id_min_long = df_grid_empty_long['grid_id'].astype(int).min()
                    else:
                        continue_cancel_case = True

                    if not continue_cancel_case \
                            and trigger_grid_id_min_long > grid_id_min_long:
                        trigger_grid_id_max_long = str(pd.Series(self.trend_data[trend]['lst_gridId_open_triggers_long']).astype(int).max())
                        if len(self.trend_data[trend]['df_open_triggers_long'].loc[self.trend_data[trend]['df_open_triggers_long']["gridId"] == trigger_grid_id_max_long, "orderId"]) > 0:
                            cancel_orderId = self.trend_data[trend]['df_open_triggers_long'].loc[self.trend_data[trend]['df_open_triggers_long']["gridId"] == trigger_grid_id_max_long, "orderId"].values[0]
                            self.lst_cancel_trigger_orderId.append(cancel_orderId)

                # UP + SHORT
                nb_open_triggers_short = df_grid['trigger_short'].sum()
                # if True \
                #         or self.trend_data[trend]['nb_open_triggers_short'] == self.nb_grid:
                if self.trend_data[trend]['nb_open_triggers_short'] > nb_cancel_position:
                    continue_cancel_case = False
                    condition_0 = df_grid['grid_id'] != 0
                    condition_engaged_trigger_short = df_grid['status_trigger_short'] == 'engaged'
                    condition_not_engaged_trailer_short = df_grid['status_trailer_short'] == 'empty'
                    df_trigger_engaged_grid_id_max_short = df_grid[condition_engaged_trigger_short
                                                                   & condition_not_engaged_trailer_short
                                                                   & condition_0].copy()
                    nb_trigger_engaged_grid_id_max_short = len(df_trigger_engaged_grid_id_max_short)
                    trigger_grid_id_max_short = df_trigger_engaged_grid_id_max_short['grid_id'].astype(int).max()

                    if len(self.trend_data[trend]['lst_gridId_open_triggers_short']) > 0:
                        trigger_grid_id_max_short = max(self.trend_data[trend]['lst_gridId_open_triggers_short'])

                    condition_empty_trigger_short = df_grid['status_trigger_short'] == 'empty'
                    condition_empty_trigger_long = df_grid['status_trigger_long'] == 'empty'
                    condition_side_short = df_grid['side'] == 'trigger_short'
                    df_grid_empty_short = df_grid[condition_empty_trigger_short
                                                  & condition_empty_trigger_long
                                                  & condition_side_short
                                                  & condition_0].copy()
                    if len(df_grid_empty_short) > 0:
                        grid_id_max_short = df_grid_empty_short['grid_id'].astype(int).max()
                    else:
                        continue_cancel_case = True

                    if not continue_cancel_case \
                            and trigger_grid_id_max_short < grid_id_max_short:
                        trigger_grid_id_max_short = str(pd.Series(self.trend_data[trend]['lst_gridId_open_triggers_short']).astype(int).min())
                        if len(self.trend_data[trend]['df_open_triggers_short'].loc[self.trend_data[trend]['df_open_triggers_short']["gridId"] == trigger_grid_id_max_short, "orderId"]) > 0:
                            cancel_orderId = self.trend_data[trend]['df_open_triggers_short'].loc[self.trend_data[trend]['df_open_triggers_short']["gridId"] == trigger_grid_id_max_short, "orderId"].values[0]
                            self.lst_cancel_trigger_orderId.append(cancel_orderId)
            elif trend == "down":
                # DOWN + LONG
                nb_open_triggers_long = df_grid['trigger_long'].sum()
                # if True \
                #         or self.trend_data[trend]['nb_open_triggers_long'] == self.nb_grid:
                if self.trend_data[trend]['nb_open_triggers_long'] > nb_cancel_position:
                    continue_cancel_case = False
                    condition_0 = df_grid['grid_id'] != 0
                    condition_engaged_trigger_long = df_grid['status_trigger_long'] == 'engaged'
                    condition_not_engaged_trailer_long = df_grid['status_trailer_long'] == 'empty'
                    df_trigger_engaged_grid_id_max_long = df_grid[condition_engaged_trigger_long
                                                                  & condition_not_engaged_trailer_long
                                                                  & condition_0].copy()
                    nb_trigger_engaged_grid_id_max_long = len(df_trigger_engaged_grid_id_max_long)
                    trigger_grid_id_max_long = df_trigger_engaged_grid_id_max_long['grid_id'].astype(int).max()

                    if len(self.trend_data[trend]['lst_gridId_open_triggers_long']) > 0:
                        trigger_grid_id_max_long = max(self.trend_data[trend]['lst_gridId_open_triggers_long'])

                    condition_empty_trigger_long = df_grid['status_trigger_long'] == 'empty'
                    condition_empty_trigger_short = df_grid['status_trigger_short'] == 'empty'
                    condition_side_long = df_grid['side'] == 'trigger_long'
                    df_grid_empty_long = df_grid[condition_empty_trigger_long
                                                 & condition_empty_trigger_short
                                                 & condition_side_long
                                                 & condition_0].copy()
                    if len(df_grid_empty_long) > 0:
                        grid_id_max_long = df_grid_empty_long['grid_id'].astype(int).max()
                    else:
                        continue_cancel_case = True

                    if not continue_cancel_case \
                            and trigger_grid_id_max_long < grid_id_max_long:
                        trigger_grid_id_min_long = str(pd.Series(self.trend_data[trend]['lst_gridId_open_triggers_long']).astype(int).min())
                        if len(self.trend_data[trend]['df_open_triggers_long'].loc[self.trend_data[trend]['df_open_triggers_long']["gridId"] == trigger_grid_id_min_long, "orderId"]) > 0:
                            cancel_orderId = self.trend_data[trend]['df_open_triggers_long'].loc[self.trend_data[trend]['df_open_triggers_long']["gridId"] == trigger_grid_id_min_long, "orderId"].values[0]
                            self.lst_cancel_trigger_orderId.append(cancel_orderId)
                # DOWN + SHORT
                nb_open_triggers_short = df_grid['trigger_short'].sum()
                # if True \
                #         or self.trend_data[trend]['nb_open_triggers_short'] == self.nb_grid:
                if self.trend_data[trend]['nb_open_triggers_short'] > nb_cancel_position:
                    continue_cancel_case = False
                    condition_0 = df_grid['grid_id'] != 0
                    condition_engaged_trigger_short = df_grid['status_trigger_short'] == 'engaged'
                    condition_not_engaged_trailer_short = df_grid['status_trailer_short'] != 'empty'
                    df_trigger_engaged_grid_id_min_short = df_grid[condition_engaged_trigger_short
                                                                   & condition_not_engaged_trailer_short
                                                                   & condition_0].copy()
                    nb_trigger_engaged_grid_id_min_short = len(df_trigger_engaged_grid_id_min_short)
                    trigger_grid_id_min_short = df_trigger_engaged_grid_id_min_short['grid_id'].astype(int).min()

                    if len(self.trend_data[trend]['lst_gridId_open_triggers_short']) > 0:
                        trigger_grid_id_min_short = min(self.trend_data[trend]['lst_gridId_open_triggers_short'])

                    condition_empty_trigger_short = df_grid['status_trigger_short'] == 'empty'
                    condition_empty_trigger_long = df_grid['status_trigger_long'] == 'empty'
                    condition_side_short = df_grid['side'] == 'trigger_short'
                    df_grid_empty_short = df_grid[condition_empty_trigger_short
                                                  & condition_empty_trigger_long
                                                  & condition_side_short
                                                  & condition_0].copy()
                    if len(df_grid_empty_short) > 0:
                        grid_id_min_short = df_grid_empty_short['grid_id'].astype(int).min()
                    else:
                        continue_cancel_case = True

                    if not continue_cancel_case \
                            and trigger_grid_id_min_short > grid_id_min_short:
                        trigger_grid_id_max_short = str(pd.Series(self.trend_data[trend]['lst_gridId_open_triggers_short']).astype(int).max())
                        if len(self.trend_data[trend]['df_open_triggers_short'].loc[self.trend_data[trend]['df_open_triggers_short']["gridId"] == trigger_grid_id_max_short, "orderId"]) > 0:
                            cancel_orderId = self.trend_data[trend]['df_open_triggers_short'].loc[self.trend_data[trend]['df_open_triggers_short']["gridId"] == trigger_grid_id_max_short, "orderId"].values[0]
                            self.lst_cancel_trigger_orderId.append(cancel_orderId)

        if len(self.lst_cancel_trigger_orderId):
            print("cancel list: ", len(self.lst_cancel_trigger_orderId))
            print(self.lst_cancel_trigger_orderId)
        return len(self.lst_cancel_trigger_orderId) != 0

    def update_executed_trade_status(self, symbol, lst_orders):
        if len(lst_orders) > 0:
            for trend in self.lst_trend:
                df_grid = self.grid[symbol][trend]
                for order in lst_orders:
                    if "trigger_type" in order \
                            and order["trigger_type"] == "SL_TP_TRAILER":
                        print("toto")

                    if "strategy_id" in order \
                            and order["strategy_id"] == self.strategy_id \
                            and "trend" in order \
                            and trend == order["trend"]:
                        grid_id = order["grid_id"]
                        if "trade_status" in order \
                                and order["trade_status"] == "SUCCESS":
                            if "trigger_type" in order \
                                    and order["trigger_type"] == "TRIGGER":
                                df_grid.loc[df_grid["grid_id"] == grid_id, "status"] = "engaged"
                                if "LONG" in order["type"]:
                                    df_grid.loc[df_grid["grid_id"] == grid_id, "orderId_trigger_long"] = order["orderId"]
                                    df_grid.loc[df_grid["grid_id"] == grid_id, "trigger_long"] = True
                                    df_grid.loc[df_grid["grid_id"] == grid_id, "status_trigger_long"] = "engaged"
                                elif "SHORT" in order["type"]:
                                    df_grid.loc[df_grid["grid_id"] == grid_id, "orderId_trigger_short"] = order["orderId"]
                                    df_grid.loc[df_grid["grid_id"] == grid_id, "trigger_short"] = True
                                    df_grid.loc[df_grid["grid_id"] == grid_id, "status_trigger_short"] = "engaged"
                            elif "trigger_type" in order \
                                    and order["trigger_type"] == "SL_TP_TRAILER":
                                if "LONG" in order["type"] \
                                        and "orderId" in order \
                                        and "triggered_orderId" in order:
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
                                    condition = df_grid['orderId_trigger_short'] == order["triggered_orderId"]
                                    df_grid.loc[condition, ['orderId_trigger_short', 'trigger_short', 'status_trigger_short']] = ['',
                                                                                                                                  False,
                                                                                                                                  'empty']
                                    df_grid.loc[condition, ['orderId_trailer_short', 'trailer_short', 'status_trailer_short']] = [order["orderId"],
                                                                                                                                  True,
                                                                                                                                  'engaged']
                                if order["triggered_orderId"] in self.lst_recoded_trigger_executed:
                                    print("orderId triggered: ", self.lst_recoded_trigger_executed)
                                    self.lst_recoded_trigger_executed.remove(order["triggered_orderId"])
                                    print("remaining trigger orderId triggered: ", self.lst_recoded_trigger_executed)
                                else:
                                    print("trigger orderId triggered missing")
                            elif "trigger_type" in order \
                                    and order["trigger_type"] == "CANCEL_TRIGGER":
                                if False:
                                    self.debug_cpt = 0 # CEDE DEBUG
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
        # if not self.zero_print:
        for symbol in self.lst_symbols:
            df_grid = self.grid[symbol]
            self.log("\n" + df_grid.to_string(index=False))
            del df_grid

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
        for trend in self.lst_trend:
            df = self.grid[symbol][trend]
            df['position'] = df['position'].apply(lambda x: self.normalize_price(x, pricePlace, priceEndStep))
            self.log("grid price normalized: " + str(df['position'].tolist()))
            df['buying_size'] = df['buying_size'].apply(lambda x: self.normalize_price(x, pricePlace, priceEndStep))
            df['buying_size'] = df['buying_size'].astype(int)
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

    def set_triggers_executed(self, symbol, df_open_triggers):
        condition_strategy_id = df_open_triggers['strategyId'] == self.strategy_id
        if not symbol.endswith('USDT'): # CEDE: to be fixed
            symbol += 'USDT'
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
            condition_trigger_trailer_executed = df_open_triggers['planStatus'] == 'executed'
            condition_trigger_plan = df_open_triggers['planType'] == 'normal_plan'
            condition_trigger_plan_TP = df_open_triggers['planType'] == 'profit_plan'
            condition_trigger_plan_SL = df_open_triggers['planType'] == 'loss_plan'
            condition_trigger_moving_plan = df_open_triggers['planType'] == 'moving_plan'
            condition_profit_loss = df_open_triggers['planType'] == 'profit_loss'
            condition_long = df_open_triggers['side'] == 'buy'
            condition_short = df_open_triggers['side'] == 'sell'

            self.trend_data[trend]['df_open_triggers'] = df_open_triggers[condition_trigger_trailer_open
                                                                          & condition_trigger_plan
                                                                          & condition_trigger_trend].copy()
            self.trend_data[trend]['nb_open_triggers'] = len(self.trend_data[trend]['df_open_triggers'])
            self.trend_data[trend]['lst_gridId_open_triggers'] = list(map(int, self.trend_data[trend]['df_open_triggers']["gridId"].to_list()))
            self.trend_data[trend]['lst_orderId_open_triggers'] = self.trend_data[trend]['df_open_triggers']["orderId"].to_list()

            self.trend_data[trend]['df_open_triggers_long'] = df_open_triggers[condition_long
                                                                               & condition_trigger_trailer_open
                                                                               & condition_trigger_plan
                                                                               & condition_trigger_trend].copy()
            self.trend_data[trend]['nb_open_triggers_long'] = len(self.trend_data[trend]['df_open_triggers_long'])
            self.trend_data[trend]['lst_gridId_open_triggers_long'] = list(map(int, self.trend_data[trend]['df_open_triggers_long']["gridId"].to_list()))
            self.trend_data[trend]['lst_orderId_open_triggers_long'] = self.trend_data[trend]['df_open_triggers_long']["orderId"].to_list()

            self.trend_data[trend]['df_open_triggers_short'] = df_open_triggers[condition_short
                                                                                & condition_trigger_trailer_open
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

            self.df_previous_open_trailers[trend] = self.trend_data[trend]['df_open_trailers'].copy()

            self.trend_data[trend]['df_executed_trigger'] = df_open_triggers[condition_trigger_trailer_executed
                                                                             & condition_trigger_trend
                                                                             & condition_trigger_plan].copy()
            self.trend_data[trend]['nb_executed_trigger'] = len(self.trend_data[trend]['df_executed_trigger'])
            self.trend_data[trend]['lst_gridId_executed_trigger'] = list(map(int, self.trend_data[trend]['df_executed_trigger']["gridId"].to_list()))
            self.trend_data[trend]['lst_orderId_executed_trigger'] = self.trend_data[trend]['df_executed_trigger']["orderId"].to_list()

            if len(self.trend_data[trend]['lst_orderId_executed_trigger']) > 0:
                self.lst_recoded_trigger_executed += self.trend_data[trend]['lst_orderId_executed_trigger']
                print(trend, " - add orderId to lst_recoded_trigger_executed", self.trend_data[trend]['lst_orderId_executed_trigger'])

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
            if nb_new_SLTP == 0:
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
            try:
                smallest_orders = same_size_orders.nsmallest(remaining_to_cancel, 'size')
            except:
                print("toto")
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
