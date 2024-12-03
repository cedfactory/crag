from . import rtdp, rtstr
import math
import pandas as pd
import numpy as np

import copy

import os
import glob

from . import utils
from src import logger

class StrategyReversedGridTradingGenericV3(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.grid_high = 0
        self.grid_low = 0
        self.nb_grid = 0
        self.grid_margin = 0
        self.percent_per_grid = 0
        self.percent_trade_per_grid = 0
        self.offload = False
        self.nb_position_limits = 1
        self.amount = 0
        self.nb_rows_to_cancel = 0
        self.side = ""
        self.id = ""
        if params:
            self.id = params.get("id", self.id)
            self.side = params.get("type", self.side)
            self.lst_symbols = [params.get("strategy_symbol", self.lst_symbols)]
            self.symbol = params.get("strategy_symbol", self.lst_symbols)

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
            self.amount = params.get("amount", self.amount)
            if isinstance(self.amount, str):
                if len(self.amount) > 0:
                    self.amount = float(self.amount)
                elif len(self.amount) == 0:
                    self.amount = 0
            self.percent_trade_per_grid = params.get("percent_trade_per_grid", self.percent_trade_per_grid)
            if isinstance(self.percent_trade_per_grid, str):
                if len(self.percent_trade_per_grid) > 0:
                    self.percent_trade_per_grid = float(self.percent_trade_per_grid)
                elif len(self.percent_trade_per_grid) == 0:
                    self.percent_trade_per_grid = 0
            self.offload = params.get("offload", self.offload)
            if isinstance(self.offload, str):
                self.offload = self.offload.lower() == "true"
        else:
            exit(5)

        self.nb_position_max = self.nb_grid

        self.zero_print = True
        self.strategy_id = utils.generate_random_id(4)
        self.grid = GridPosition(self.get_info(),
                                 self.side,
                                 self.symbol,
                                 self.grid_high, self.grid_low, self.nb_grid, self.percent_per_grid,
                                 self.amount,
                                 self.offload,
                                 self.strategy_id,
                                 self.zero_print,
                                 self.loggers)
        if self.percent_per_grid != 0:
            self.nb_grid = self.grid.get_grid_nb_grid()
        self.df_grid_buying_size = pd.DataFrame()
        self.execute_timer = None
        self.df_price = None
        self.mutiple_strategy = False

        self.previous_stats = {}
        self.self_execute_trade_recorder_not_active = True

        self.backup_grid = None          # DEBUG CEDE
        self.backup_df = pd.DataFrame()  # DEBUG CEDE

    def get_data_description(self):
        return {}

    def get_info(self):
        return "StrategyReversedGridTradingGenericV3"

    def get_symbol(self):
        return self.symbol

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
        df_open_triggers = current_state["triggers"].copy()
        df_price = current_state["prices"].copy()

        if not self.mutiple_strategy:
            del current_state["open_orders"]
            del current_state["open_positions"]
            del current_state["prices"]
            del current_state

        del self.df_price
        self.df_price = df_price

        self.grid.set_current_price(df_price.loc[df_price['symbols'] == self.symbol, 'values'].values[0])

        self.grid.update_grid_side()

        self.grid.update_orders_id_status(self.symbol, df_current_states, df_open_triggers)

        lst_order_to_execute = self.grid.get_order_list(self.symbol)

        if not self.zero_print:
            df_sorted = df_current_states.sort_values(by='price')
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

    def set_normalized_grid_price(self, lst_symbol_plc_endstp):
        return

    def get_info_msg_status(self):
        stats = self.grid.get_stats()
        if not(utils.dicts_are_equal(stats, self.previous_stats)):
            msg_stats = utils.dict_to_string(stats)
            self.previous_stats = copy.deepcopy(stats)
            del stats
            return msg_stats
        del stats
        return ""

    def get_grid(self, cpt):
        return self.grid.get_grid(self.symbol, cpt)

    def record_status(self):
        if self.self_execute_trade_recorder_not_active:
            return

        if hasattr(self, 'df_price'):
            df_grid_values = self.grid.get_grid_for_record(self.symbol)
            self.execute_timer.set_grid_infos(df_grid_values, self.side)

    def update_executed_trade_status(self, lst_orders):
        if lst_orders is None:
            return []
        if len(lst_orders) > 0:
            self.grid.update_executed_trade_status(self.symbol, lst_orders)

    def get_strategy_id(self):
        return self.strategy_id

    def set_df_normalize_buying_size(self, df_normalized_buying_size):
        self.df_grid_buying_size = df_normalized_buying_size
        self.grid.set_grid_buying_size(self.df_grid_buying_size)
        self.grid.cross_check_buying_size(self.symbol, self.get_grid_buying_min_size(self.symbol))

    def set_df_buying_size(self, df_symbol_size, cash):
        if not isinstance(df_symbol_size, pd.DataFrame):
            return
        # cash = 10000 # CEDE GRID SCENARIO
        self.df_grid_buying_size = pd.concat([self.df_grid_buying_size, df_symbol_size])
        self.df_grid_buying_size['margin'] = None

        leverage_long = self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, 'leverage_long'].values[0]

        min_buying_sizes = self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, 'minBuyingSize'].values[0]

        if self.percent_trade_per_grid == 0:
            size = float(self.amount) / (self.nb_position_max) / leverage_long
        else:
            size = float(self.amount) * self.percent_trade_per_grid / leverage_long / 100
        if size > min_buying_sizes:
            size = utils.normalize_size(size,
                                         self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol,
                                                                      "sizeMultiplier"].values[0])

            self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, "strategy_id"] = self.strategy_id
            self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, "buyingSize"] = size
            self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, "margin"] = self.grid_margin
            self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, "maxSizeToBuy"] = self.nb_position_max
        else:
            exit(2)

        return self.df_grid_buying_size

    def initialize_backup(self):
        """Initialize backups for the grid-related data."""
        self.backup_grid = copy.deepcopy(self.grid.grid)
        self.backup_grid_intermediat = copy.deepcopy(self.grid.grid_intermediat)
        self.backup_orders_to_cancel_df = copy.deepcopy(self.grid.orders_to_cancel_df)
        self.backup_missing_TP_df = copy.deepcopy(self.grid.missing_TP_df)
        self.backup_missing_open_orders_df = copy.deepcopy(self.grid.missing_open_orders_df)
        self.backup_df_open_triggers_original = copy.deepcopy(self.grid.df_open_triggers_original)

    def print_debug_grid(self):
        backup_dir = 'debug_backup_grid'
        backup_file = f'debug_backup_grid/{self.side}.txt'

        def clean_backup_directory():
            """Clean or create the backup directory."""
            if os.path.exists(backup_dir):
                for file in glob.glob(os.path.join(backup_dir, "*.txt")):
                    os.remove(file)
            else:
                os.makedirs(backup_dir)

        def write_backup_file(dataframes, file_path):
            """Write the provided dataframes to the backup file."""
            # Ensure the directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            # Write data to the file
            with open(file_path, 'a') as f:
                f.write("\n---------------------\n")
                for label, df in dataframes.items():
                    f.write(f"\n{label}\n")  # Add a separator for each section
                    f.write(df.to_string() + "\n")
                f.write("\n---------------------\n")

        # Initialize backup if not already done
        if self.backup_grid is None:
            self.initialize_backup()
            clean_backup_directory()

            # Columns to check for "empty"
            columns_to_check = [
                'planType', 'status', 'status_TP', 'status_open_order',
                'orderId_TP', 'orderId_open_order', 'cancel_status'
            ]

            # Filter rows in backup_grid_intermediat where all specified columns are "empty"
            self.backup_grid_intermediat = self.backup_grid_intermediat[
                ~self.backup_grid_intermediat[columns_to_check].eq('empty').all(axis=1)
            ]
            self.backup_grid = self.backup_grid[
                ~self.backup_grid[columns_to_check].eq('empty').all(axis=1)
            ]

            # Write initial backup
            write_backup_file(
                {
                    "backup_grid_intermediat": self.backup_grid_intermediat,
                    "backup_grid": self.backup_grid,
                },
                backup_file
            )
            return

        # Compare the current and backup open triggers
        current_df = self.grid.df_open_triggers_original.copy()
        backup_df = self.backup_df.copy()

        self.initialize_backup()
        # If there are changes, update the backups and log differences
        if not current_df.equals(backup_df) \
                or len(self.backup_missing_TP_df) > 0 \
                or len(self.backup_missing_open_orders_df) > 0 \
                or len(self.backup_orders_to_cancel_df) > 0:
            # Columns to check for "empty"
            columns_to_check = [
                'planType', 'status', 'status_TP', 'status_open_order',
                'orderId_TP', 'orderId_open_order', 'cancel_status'
            ]

            # Filter rows in backup_grid_intermediat where all specified columns are "empty"
            self.backup_grid_intermediat = self.backup_grid_intermediat[
                ~self.backup_grid_intermediat[columns_to_check].eq('empty').all(axis=1)
            ]

            # Filter rows in backup_grid where all specified columns are "empty"
            self.backup_grid = self.backup_grid[
                ~self.backup_grid[columns_to_check].eq('empty').all(axis=1)
            ]

            write_backup_file(
                {
                    "backup_orders_to_cancel_df": self.backup_orders_to_cancel_df,
                    "backup_missing_TP_df": self.backup_missing_TP_df,
                    "backup_missing_open_orders_df": self.backup_missing_open_orders_df,
                    "backup_df_open_triggers_original": self.backup_df_open_triggers_original,
                    "backup_grid_intermediat": self.backup_grid_intermediat,
                    "backup_grid": self.backup_grid,
                },
                backup_file
            )

        # Update the main backup after processing
        self.backup_df = copy.deepcopy(current_df)

        return

class GridPosition():
    def __init__(self, strategy_name, side, symbol, grid_high, grid_low, nb_grid, percent_per_grid,
                 amount,
                 offload,
                 strategy_id, debug_mode=True, loggers=[]):
        self.grid_side = side
        side_mapping = {
            "long": ("open_long", "close_long"),
            "short": ("open_short", "close_short")
        }
        self.side_mapping_sltp = {
            "long": "buy",
            "short": "sell"
        }
        self.str_open, self.str_close = side_mapping.get(self.grid_side, (None, None))

        self.strategy_id = strategy_id
        self.grid_high = grid_high
        self.grid_low = grid_low
        self.nb_grid = nb_grid
        self.symbol = symbol
        self.percent_per_grid = percent_per_grid

        self.zero_print = debug_mode
        self.loggers = loggers
        self.current_price = None
        self.steps = 0
        self.msg = ""
        self.df_grid_string = ""

        self.current_state = {}

        self.SLTP_activated = True

        self.offload = offload

        # Calculate the step size
        self.step_size = self.grid_low * (self.percent_per_grid / 100)

        # Generate the main grid values
        self.lst_grid_values = list(np.arange(self.grid_low, self.grid_high + self.step_size, self.step_size))
        if self.lst_grid_values[-1] != self.grid_high:
            self.lst_grid_values[-1] = self.grid_high

        if self.grid_side == "long":
            # Generate the shifted grid values
            self.lst_shifted_grid_values = [value - self.step_size for value in self.lst_grid_values]
            # Ensure the first value starts at grid_low - self.step_size
            self.lst_shifted_grid_values[0] = self.grid_low - self.step_size
        elif self.grid_side == "short":
            # Generate the shifted grid values
            self.lst_shifted_grid_values = [value + self.step_size for value in self.lst_grid_values]
            # self.lst_shifted_grid_values[len(self.lst_shifted_grid_values)] = self.grid_high + self.step_size

        self.columns = ["grid_id",
                        "position", "open_position",
                        "side", "size", "planType",
                        "status", "status_TP", "status_open_order",
                        "orderId_TP", "orderId_open_order",
                        "cancel_status"
                        ]

        # self.grid = {key: pd.DataFrame(columns=self.columns) for key in self.lst_symbols}
        self.grid = pd.DataFrame(columns=self.columns)

        self.grid["position"] = self.lst_grid_values
        self.grid["open_position"] = self.lst_shifted_grid_values

        self.grid["grid_id"] = np.arange(len(self.grid))[::-1]

        self.grid["orderId_TP"] = "empty"
        self.grid["side"] = ""
        self.grid["orderId_open_order"] = "empty"

        self.grid["status"] = "empty"
        self.grid["status_TP"] = "empty"
        self.grid["status_open_order"] = "empty"
        self.grid["cancel_status"] = "empty"

        self.stats = {
            "startegy": strategy_name,
            "side": side,
            "nb_open_order": 0,
            "nb_open_tp": 0,
            "total_closed": 0,
            "total_triggered": 0
        }

    def get_stats(self):
        return self.stats

    def log(self, msg, header="", attachments=[]):
        if self.zero_print:
            return
        for iter_logger in self.loggers:
            iter_logger.log(msg, header=header, author=type(self).__name__, attachments=attachments)

    def get_grid_nb_grid(self):
        return len(self.grid)

    def update_grid_side(self):
        df = self.grid
        position = self.current_price

        side_map = {
            "long": {"greater": self.str_close, "less": self.str_open},
            "short": {"greater": self.str_open, "less": self.str_close}
        }
        df.loc[df['position'] > position, 'side'] = side_map[self.grid_side]['greater']
        df.loc[df['position'] < position, 'side'] = side_map[self.grid_side]['less']

        df.loc[df['position'] == self.current_price, 'side'] = "on_edge"

        # differences = df['position'].diff().dropna()
        # diff = differences.mean() / 2

        if True:
            diff = self.step_size / 3
        else:
            diff = 0   # CEDE DEBUG
        df.loc[(df['position'] >= self.current_price - abs(diff))
               & (df['position'] <= self.current_price + abs(diff)), 'side'] = "on_edge"

    def update_orders_id_status(self, symbol, df_current_state, df_open_triggers):
        self.nb_rows_to_cancel = 0
        self.grid['status_TP'] = "empty"
        self.grid['status_open_order'] = "empty"
        self.grid['status'] = "empty"
        self.grid['planType'] = "empty"

        self.df_open_triggers_original = df_open_triggers.copy()

        if False:
            df_debug = df_open_triggers.copy()
            condition_open_executed = df_debug['planStatus'] == 'executed'
            # condition_side_sltp = df_debug['side'] == self.side_mapping_sltp.get(self.grid_side, None)
            df_open_debug = df_debug[condition_open_executed]
            if len(df_open_debug) > 0: # CEDE DEBUG
                print('titi')
                print(df_open_debug.to_string())

        self.current_state = {}
        self.current_state[self.grid_side] = {}

        df_open_triggers_open_order = df_open_triggers[df_open_triggers['planType'] == 'normal_plan']
        if not df_open_triggers_open_order.empty:
            df_open_triggers = df_open_triggers[df_open_triggers["symbol"] == symbol]
            df_open_triggers = df_open_triggers.loc[df_open_triggers['strategyId'] == self.strategy_id]
            df_open_triggers = df_open_triggers[df_open_triggers['planType'] == 'normal_plan']

            condition_open = df_open_triggers['planStatus'] != 'executed'
            condition_not_canceled = df_open_triggers['planStatus'] != 'cancelled'
            condition_side_sltp = df_open_triggers['side'] == self.side_mapping_sltp.get(self.grid_side, None)

            df_open_triggers_side = df_open_triggers[condition_open
                                                     & condition_not_canceled
                                                     & condition_side_sltp]

            self.current_state[self.grid_side]["lst_open_orders_order_id"] = df_open_triggers["orderId"].to_list()
            self.current_state[self.grid_side]["lst_open_orders_order_id_side"] = df_open_triggers_side["orderId"].to_list()

            order_id_list_open_order = self.current_state[self.grid_side]["lst_open_orders_order_id_side"]
            list_orderId_open_order = self.grid["orderId_open_order"].to_list()
            list_orderId_open_order = [item for item in list_orderId_open_order if item != 'empty']

            set_order_id_list_open_order = set(order_id_list_open_order)
            set_list_orderId_open_order = set(list_orderId_open_order)

            # Get the difference: items in order_id_list_TP but not in list_orderId_TP
            difference = set_list_orderId_open_order - set_order_id_list_open_order
            difference_list = list(difference)

            self.grid['status_open_order'] = np.where(self.grid['orderId_open_order'].isin(difference_list),
                                                      'engaged',
                                                      self.grid['status_open_order']
                                                      )
            self.grid['status_TP'] = np.where(self.grid['orderId_open_order'].isin(difference_list),
                                              'empty',
                                              self.grid['status_TP']
                                              )
            for orderId in order_id_list_open_order:
                if orderId in df_open_triggers_side["orderId"].to_list():
                    # Retrieve the corresponding gridId
                    grid_id = float(df_open_triggers_side.loc[df_open_triggers_side["orderId"] == orderId, 'gridId'].values[0])
                    plan_type = df_open_triggers_side.loc[df_open_triggers_side["orderId"] == orderId, 'planType'].values[0]
                    if not self.grid.loc[self.grid['grid_id'] == grid_id].empty:
                        self.grid.loc[self.grid['grid_id'] == grid_id, 'orderId_open_order'] = orderId
                        self.grid.loc[self.grid['grid_id'] == grid_id, 'planType'] = plan_type
                        self.grid.loc[self.grid['grid_id'] == grid_id, 'status_open_order'] = 'engaged'
                        self.grid.loc[self.grid['grid_id'] == grid_id, 'status_TP'] = "empty"

            self.grid["orderId_open_order"] = self.grid["orderId_open_order"].apply(lambda x: "empty" if x not in order_id_list_open_order else x)
        else:
            self.current_state[self.grid_side]["lst_open_orders_order_id"] = []
            self.current_state[self.grid_side]["lst_open_orders_order_id_side"] = []
            self.grid["orderId_open_order"] = "empty"


        df_open_triggers = self.df_open_triggers_original.copy()

        if False: # CEDE DEBUG
            df_open_triggers_TP = df_open_triggers[df_open_triggers['planType'] == 'profit_loss']
            if len(df_open_triggers_TP) > 0:
                print("titi titi")

        df_open_triggers_TP = df_open_triggers[df_open_triggers['planType'].isin(['profit_plan', 'profit_loss'])]
        if not df_open_triggers_TP.empty:
            df_open_triggers = df_open_triggers[df_open_triggers["symbol"] == symbol]
            df_open_triggers = df_open_triggers.loc[df_open_triggers['strategyId'] == self.strategy_id]
            df_open_triggers = df_open_triggers[df_open_triggers['planType'] == 'profit_plan']

            condition_open = df_open_triggers['planStatus'] != 'executed'
            condition_not_canceled = df_open_triggers['planStatus'] != 'cancelled'
            condition_side_sltp = df_open_triggers['side'] == self.side_mapping_sltp.get(self.grid_side, None)

            df_open_triggers_side = df_open_triggers[condition_open
                                                     & condition_not_canceled
                                                     & condition_side_sltp]

            self.current_state[self.grid_side]["lst_TP_orders_order_id"] = df_open_triggers["orderId"].to_list()
            self.current_state[self.grid_side]["lst_TP_orders_order_id_side"] = df_open_triggers_side["orderId"].to_list()

            order_id_list_TP = self.current_state[self.grid_side]["lst_TP_orders_order_id_side"]
            list_orderId_TP = self.grid["orderId_TP"].to_list()
            list_orderId_TP = [item for item in list_orderId_TP if item != 'empty']

            set_order_id_list_TP = set(order_id_list_TP)
            set_list_orderId_TP = set(list_orderId_TP)

            # Get the difference: items in order_id_list_TP but not in list_orderId_TP
            difference = set_list_orderId_TP - set_order_id_list_TP
            difference_list = list(difference)
            if False and len(difference_list) > 0: # CEDE DEBUG
                print("toto")
                print(difference_list)

            self.grid['status_TP'] = np.where(self.grid['orderId_TP'].isin(difference_list),
                                              'triggered',
                                              self.grid['status_TP'])
            self.grid['orderId_TP'] = np.where(self.grid['orderId_TP'].isin(difference_list),
                                              'empty',
                                              self.grid['orderId_TP'])
            self.grid['orderId_TP'] = np.where(self.grid['cancel_status'].isin(['CANCELED']),
                                              'empty',
                                               self.grid['orderId_TP'])

            for orderId in order_id_list_TP:
                if orderId in df_open_triggers_side["orderId"].to_list():
                    # Retrieve the corresponding gridId
                    grid_id = float(df_open_triggers_side.loc[df_open_triggers_side["orderId"] == orderId, 'gridId'].values[0])
                    plan_type = df_open_triggers_side.loc[df_open_triggers_side["orderId"] == orderId, 'planType'].values[0]
                    if not self.grid.loc[self.grid['grid_id'] == grid_id].empty:
                        self.grid.loc[self.grid['grid_id'] == grid_id, 'orderId_TP'] = orderId
                        self.grid.loc[self.grid['grid_id'] == grid_id, 'planType'] = plan_type
                        self.grid.loc[self.grid['grid_id'] == grid_id, 'status_TP'] = "engaged"

            self.grid["orderId_TP"] = self.grid["orderId_TP"].apply(lambda x: "empty" if x not in order_id_list_TP else x)
        else:
            self.current_state[self.grid_side]["lst_TP_orders_order_id"] = []
            self.current_state[self.grid_side]["lst_TP_orders_order_id_side"] = []
            self.grid["orderId_TP"] = "empty"

        self.engaged_count = self.grid[(self.grid['orderId_TP'] != "empty") | (self.grid['orderId_open_order'] != "empty")].shape[0]
        # Apply the function to the dataframe
        # In order to update status
        self.grid = self.grid.apply(self.update_status, axis=1)

        self.filter_status()

        self.engaged_count = self.grid['status'].value_counts().get('engaged', 0)
        self.missing_count = self.grid['status'].value_counts().get('missing_open_order', 0) \
                             + self.grid['status'].value_counts().get('missing_order_TP', 0)
        self.TP_count = self.grid[self.grid['orderId_TP'] != "empty"].shape[0]

        if self.grid_side == "long":
            # Identify the first index where status is not "empty"
            first_non_empty_index = self.grid[(self.grid['status'] != "empty") & (self.grid['side'] == self.str_close)].index.min()
            # Filter rows based on the given conditions
            filtered_rows = self.grid[
                (self.grid['side'] == self.str_close) &
                (self.grid['status'] == 'empty') &
                (self.grid['status_TP'] == 'empty') &
                (self.grid['status_open_order'] == 'empty') &
                (self.grid.index < first_non_empty_index)
                ]
        elif self.grid_side == "short":
            # Identify the first index where status is not "empty"
            first_non_empty_index = self.grid[(self.grid['status'] != "empty") & (self.grid['side'] == self.str_close)].index.max()
            # Filter rows based on the given conditions
            filtered_rows = self.grid[
                (self.grid['side'] == self.str_close) &
                (self.grid['status'] == 'empty') &
                (self.grid['status_TP'] == 'empty') &
                (self.grid['status_open_order'] == 'empty') &
                (self.grid.index > first_non_empty_index)
                ]

        # Number of rows satisfying the condition
        num_rows_free = filtered_rows.shape[0]

        if num_rows_free > 0 \
                and self.missing_count == 0 \
                and self.engaged_count == self.nb_grid \
                and self.TP_count > 0:
            self.nb_rows_to_cancel = min(num_rows_free, self.TP_count)

        self.grid_intermediat = self.grid.copy()

    def update_status(self, row):
        if row['orderId_TP'] != "empty":
            row['status_TP'] = "engaged"
        if row['orderId_open_order'] != "empty":
            row['status_open_order'] = "engaged"

        if row['status_TP'] == "engaged" or row['status_open_order'] == "engaged":
            row['status'] = "engaged"

        elif row['orderId_TP'] != "empty" or row['orderId_open_order'] != "empty":
            row['status'] = "engaged"

        elif row['status_TP'] == "triggered":
            row['status'] = "missing_open_order"

        elif row['status_open_order'] == "triggered":
            row['status'] = "missing_order_TP"

        elif row['orderId_TP'] == "empty" and row['orderId_open_order'] == "empty":
            row['status'] = "missing_order_TP"

        if row['side'] != self.str_close and row['status_TP'] == "empty" and row['orderId_open_order'] == "empty":
            row['status'] = "empty"
        return row

    def filter_status(self):
        df_missing_order_TP = self.grid[self.grid['status'] == "missing_order_TP"]

        df_missing_open_order = self.grid[self.grid['status'] == "missing_open_order"]

        df_engaged = self.grid[self.grid['status'] == "engaged"]

        nb_engaged = len(df_engaged)
        nb_missing_order_TP = len(df_missing_order_TP)
        nb_missing_open_order = len(df_missing_open_order)

        nb_missing_order_to_keep = self.nb_grid - nb_engaged - nb_missing_open_order
        nb_missing_order_to_drop = nb_missing_order_TP - nb_missing_order_to_keep

        if nb_missing_order_to_drop > 0:
            # Get indices of rows with 'missing_order_TP'
            missing_order_TP_indices = df_missing_order_TP.index

            if nb_missing_order_to_keep == 0:
                indices_to_empty = missing_order_TP_indices
            else:
                if self.grid_side == "long":
                    # Keep the nb_missing_order_to_keep with the lower index
                    # indices_to_keep = missing_order_TP_indices[:nb_missing_order_to_keep]
                    indices_to_empty = missing_order_TP_indices[nb_missing_order_to_keep:]
                elif self.grid_side == "short":
                    # Keep the nb_missing_order_to_keep with the greater index
                    # indices_to_keep = missing_order_TP_indices[-nb_missing_order_to_keep:]
                    indices_to_empty = missing_order_TP_indices[:-nb_missing_order_to_keep]

            # Update the 'status' column for rows to be emptied
            self.grid.loc[indices_to_empty, 'status'] = "empty"

    def set_current_price(self, price):
        self.current_price = price

    def clear_grid(self, df):
        # List of columns to be set to "empty"
        columns_to_clear = ['planType', 'status', 'status_TP', 'status_open_order', 'orderId_TP', 'orderId_open_order']

        # Update the original self.grid for the rows in the input dataframe
        self.grid.loc[df.index, columns_to_clear] = "empty"

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
        # Filter rows where 'status' is "missing_order"
        self.missing_TP_df = self.grid[(self.grid['status'] == "missing_order_TP")]

        self.missing_open_orders_df = self.grid[(self.grid['status'] == "missing_open_order")]

        if self.offload and not self.missing_open_orders_df.empty:
            self.clear_grid(self.missing_open_orders_df)
            self.missing_open_orders_df = self.missing_open_orders_df[0:0]

        missing_orders_df = pd.concat([self.missing_TP_df, self.missing_open_orders_df], ignore_index=True)

        # Select relevant columns and convert the result to a list of dictionaries
        lst_missing_open_orders_grid_id = self.missing_open_orders_df['grid_id'].to_list()
        lst_missing_TP_grid_id = self.missing_TP_df['grid_id'].to_list()
        lst_missing_order = missing_orders_df[['grid_id', 'position', 'open_position', 'size', 'side']].to_dict(orient='records')

        lst_orders_to_cancel = []
        lst_grid_ids_to_cancel = []
        if self.nb_rows_to_cancel > 0:
            # Filter rows where status_TP is not "empty"
            filtered_orders = self.grid[
                (self.grid['status_TP'] != 'empty') &
                (self.grid['orderId_TP'] != 'empty') &
                (self.grid['orderId_TP'] != 'profit_plan')
                ]
            # Get the self.nb_rows_to_cancel rows with the highest indices
            self.orders_to_cancel_df = filtered_orders.iloc[-self.nb_rows_to_cancel:]
            # Extract the grid_id values as a list
            lst_grid_ids_to_cancel = self.orders_to_cancel_df['grid_id'].tolist()
            lst_orders_to_cancel = filtered_orders[['grid_id', 'position', "orderId_TP", 'open_position', 'size', 'side', "planType"]].to_dict(orient='records')
        else:
            self.orders_to_cancel_df = pd.DataFrame()

        lst_missing_order += lst_orders_to_cancel

        lst_orders = []
        for order in lst_missing_order:
            if order['grid_id'] in lst_missing_open_orders_grid_id:
                order_to_execute = {
                    "strategy_id": self.strategy_id,
                    "symbol": symbol,
                    "trigger_type": "TRIGGER"
                }
                order_to_execute["type"] = "OPEN_LONG_ORDER" if self.grid_side == "long" else "OPEN_SHORT_ORDER"
                order_to_execute["TP"] = ""
                order_to_execute["SP"] = ""
                if "type" in order_to_execute:
                    order_to_execute["trigger_price"] = self.grid.loc[self.grid["grid_id"] == order['grid_id'], 'open_position'].values[0]
                    order_to_execute["gross_size"] = self.grid.loc[self.grid["grid_id"] == order['grid_id'], 'size'].values[0]
                    order_to_execute["grid_id"] = order['grid_id']
                    order_to_execute["trade_status"] = "pending"
                    # order_to_execute["range_rate"] = self.percent_per_grid
                    lst_orders.append(order_to_execute)
                del order_to_execute
            elif order['grid_id'] in lst_missing_TP_grid_id:
                order_to_execute = {}
                order_to_execute["strategy_id"] = self.strategy_id
                order_to_execute["symbol"] = symbol
                order_to_execute["trigger_type"] = "OPEN_SL_TP"
                order_to_execute["planType"] = "profit_plan"
                order_to_execute["trigger_price"] = order['position']
                order_to_execute["execute_price"] = order['position']
                order_to_execute["grid_id"] = order['grid_id']
                order_to_execute["gross_size"] = order['size']
                order_to_execute["holdSide"] = self.grid_side
                order_to_execute["trade_status"] = "pending"
                order_to_execute["range_rate"] = ""
                self.grid.loc[self.grid["grid_id"] == order['grid_id'], 'status'] = 'pending'
                lst_orders.append(order_to_execute)
                del order_to_execute
            elif order['grid_id'] in lst_grid_ids_to_cancel:
                order_to_execute = {}
                order_to_execute["strategy_id"] = self.strategy_id
                order_to_execute["symbol"] = symbol
                order_to_execute["type"] = "CANCEL_SLTP"
                order_to_execute["trigger_type"] = "CANCEL_SLTP"
                order_to_execute["planType"] = order["planType"]
                order_to_execute["orderId"] = order["orderId_TP"]
                order_to_execute["trade_status"] = "pending"
                order_to_execute["grid_id"] = order['grid_id']
                lst_orders.append(order_to_execute)
                del order_to_execute

        return lst_orders

    def update_executed_trade_status(self, symbol, lst_orders):
        self.grid["cancel_status"] = "empty"
        df_grid = self.grid
        for order in lst_orders:
            if order["strategy_id"] == self.strategy_id:
                grid_id = order["grid_id"]
                if order["trade_status"] == "SUCCESS":
                    if order["trigger_type"] == "OPEN_SL_TP":
                        df_grid.loc[df_grid["grid_id"] == grid_id, "status"] = "engaged"
                        df_grid.loc[df_grid["grid_id"] == grid_id, "status_TP"] = "engaged"
                        df_grid.loc[df_grid["grid_id"] == grid_id, "orderId_TP"] = order["orderId"]
                    elif order["trigger_type"] == "TRIGGER":
                        df_grid.loc[df_grid["grid_id"] == grid_id, "status"] = "engaged"
                        df_grid.loc[df_grid["grid_id"] == grid_id, "status_open_order"] = "engaged"
                        df_grid.loc[df_grid["grid_id"] == grid_id, "orderId_open_order"] = order["orderId"]
                    elif order["trigger_type"] == "CANCEL_SLTP":
                        df_grid.loc[df_grid["grid_id"] == grid_id, "status"] = "empty"
                        df_grid.loc[df_grid["grid_id"] == grid_id, "status_TP"] = "empty"
                        df_grid.loc[df_grid["grid_id"] == grid_id, "orderId_TP"] = "empty"
                        df_grid.loc[df_grid["grid_id"] == grid_id, "cancel_status"] = "CANCELED"
                else:
                    if order["trigger_type"] == "OPEN_SL_TP":
                        df_grid.loc[df_grid["grid_id"] == grid_id, "status"] = "empty"
                        df_grid.loc[df_grid["grid_id"] == grid_id, "status_TP"] = "empty"
                        df_grid.loc[df_grid["grid_id"] == grid_id, "orderId_TP"] = order["empty"]
                    elif order["trigger_type"] == "TRIGGER":
                        df_grid.loc[df_grid["grid_id"] == grid_id, "status"] = "empty"
                        df_grid.loc[df_grid["grid_id"] == grid_id, "status_open_order"] = "empty"
                        df_grid.loc[df_grid["grid_id"] == grid_id, "orderId_open_order"] = order["orderId"]

    def print_grid(self):
        if self.zero_print:
            return
        df_grid = self.grid
        self.log("\n" + df_grid.to_string(index=False))
        del df_grid

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
        df = self.grid
        row_index = df.index[df['grid_id'] == grid_id].tolist()  # Get the index of rows where 'grid_id' equals grid_id
        for index in row_index:
            df.at[index, 'status'] = 'on_hold'  # Update the 'status' column for each matching row
        del df
        del row_index

    def normalize_grid_price(self, symbol, pricePlace, priceEndStep, sizeMultiplier):
        return

    def set_grid_buying_size(self, df_grid_buying_size):
        self.df_grid_buying_size = df_grid_buying_size

        buying_size = self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, "buyingSize"].values[0]
        pricePlace = self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, "pricePlace"].values[0]
        priceEndStep = self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, "priceEndStep"].values[0]
        self.grid['position'] = self.grid['position'].apply(lambda x: utils.normalize_price(x, pricePlace, priceEndStep))
        self.grid['open_position'] = self.grid['open_position'].apply(lambda x: utils.normalize_price(x, pricePlace, priceEndStep))

        self.grid["size"] = buying_size

        sizeMultiplier = self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == self.symbol, "sizeMultiplier"].values[0]
        self.grid["size"] = self.grid["size"].apply(
            lambda size: utils.normalize_size(size, sizeMultiplier))

        # Fill NaN values in case there are any undefined cells
        self.grid["size"].fillna(0, inplace=True)

        return

    def cross_check_buying_size(self, symbol, buying_min_size):
        if (self.grid["size"] < buying_min_size).any():
            print(f"Exiting because buying_min_size for {symbol} has a value less than buying_min_size {buying_min_size}.")
            exit(44)
        return

    def dct_status_info_to_txt(self, symbol):
        del self.msg
        self.msg = ""
        return self.msg.upper()

    def get_grid_as_str(self, symbol):
        del self.df_grid_string
        df_grid = self.grid
        self.df_grid_string = df_grid.to_string(index=False)
        del df_grid
        return self.df_grid_string

    def get_grid(self, symbol, cycle):
        df = self.grid.copy()
        if isinstance(cycle, int):
            df['cycle'] = cycle

            column_to_move = 'cycle'
            first_column = df.pop(column_to_move)
            df.insert(0, column_to_move, first_column)
        return df

    def get_grid_for_record(self, symbol):
        df = self.grid.copy()
        df["values"] = df["side"] + "_" + df["status"]
        df.set_index('position', inplace=True)
        df = df[["values"]]
        return df


