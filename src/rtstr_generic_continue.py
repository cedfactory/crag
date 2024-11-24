from . import rtdp
from . import rtstr, strategies
import math
import pandas as pd
import numpy as np
from concurrent.futures import wait, ALL_COMPLETED, ThreadPoolExecutor

from . import utils
from src import logger

from os import path

class StrategyContinueGeneric(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)
        path_strategy_param = ""
        if params:
            path_strategy_param = params.get("path_strategy_param", path_strategy_param)

        df_strategy_param = None
        if path_strategy_param != "" and path.exists("./symbols/" + path_strategy_param):
            df_strategy_param = pd.read_csv("./symbols/" + path_strategy_param)

        lst_param_strategy = []
        init_params = params
        self.lst_symbols = []
        if isinstance(df_strategy_param, pd.DataFrame):
            for index, row in df_strategy_param.iterrows():
                lst_param_strategy.append({
                    "id": row["id"],
                    "strategy_symbol": row["symbol"],
                    "name": row["name"],
                    "type": row["type"],
                    "grid_high": row["grid_high"],
                    "grid_low": row["grid_low"],
                    "percent_per_grid": row["percent_per_grid"],
                    "nb_grid": row["nb_grid"],
                    "grid_margin": row["grid_margin"],
                    "nb_position_limits": row["nb_position_limits"],
                    "amount": row["amount"]
                })
                self.lst_symbols.append(row["symbol"])

        self.lst_symbols = list(set(self.lst_symbols))

        self.lst_strategy = []
        available_strategies = rtstr.RealTimeStrategy.get_strategies_list()
        for grid_param in lst_param_strategy:
            if grid_param["name"] in available_strategies:
                combined_param_dict = {**init_params, **grid_param}
                my_strategy = rtstr.RealTimeStrategy.get_strategy_from_name(grid_param["name"], combined_param_dict)
                self.lst_strategy.append(my_strategy)

        self.set_multiple_strategy()
        self.execute_timer = None

        self.zero_print = False
        self.execute_timer = None

    def get_data_description(self):
        ds = {}
        return ds

    def get_info(self):
        return "StrategyContinueGeneric"

    def get_lst_symbols(self):
        return self.lst_symbols

    def get_strategy_type(self):
        return "CONTINUE"

    def set_multiple_strategy(self):
        for strategy in self.lst_strategy:
            strategy.set_multiple_strategy()

    def set_df_normalize_buying_size(self, df_normalized_buying_size):
        for strategy in self.lst_strategy:
            condition_strategy_id = df_normalized_buying_size['strategy_id'] == strategy.get_strategy_id()
            df_grid_strategy_id = df_normalized_buying_size[condition_strategy_id]
            strategy.set_df_normalize_buying_size(df_grid_strategy_id)

    def set_execute_time_recorder(self, execute_timer):
        for strategy in self.lst_strategy:
            strategy.set_execute_time_recorder(execute_timer)
        self.execute_timer = execute_timer

    def _set_broker_current_state_for_strategy(self, strategy, df_current_state):
        str_strategy_id_filter = strategy.get_strategy_id_code()
        current_state_filtered = self.filter_position(df_current_state, str_strategy_id_filter)
        lst_positions = strategy.set_broker_current_state(current_state_filtered)

        del current_state_filtered["open_orders"]
        del current_state_filtered["open_positions"]
        del current_state_filtered["prices"]
        del current_state_filtered

        return lst_positions

    def set_broker_current_state(self, df_current_state):
        lst_positions = []

        with ThreadPoolExecutor() as executor:
            futures = []
            for strategy in self.lst_strategy:
                futures.append(executor.submit(self._set_broker_current_state_for_strategy, strategy, df_current_state))

            wait(futures, timeout=1000, return_when=ALL_COMPLETED)

            for future in futures:
                lst_positions.extend(future.result())

        # cleaning
        del df_current_state["open_orders"]
        del df_current_state["open_positions"]
        del df_current_state["prices"]
        del df_current_state

        return lst_positions

    def set_normalized_grid_price(self, lst_symbol_plc_endstp):
        for strategy in self.lst_strategy:
            strategy.set_normalized_grid_price(lst_symbol_plc_endstp)

        del lst_symbol_plc_endstp

    def _get_info_msg_status_for_strategy(self, strategy):
        msg = ""
        msg_strategy = strategy.get_info_msg_status()
        if msg_strategy != "":
            msg += strategy.get_info() + ": \n"
            msg += msg_strategy
        return msg

    def get_info_msg_status(self):
        result_msg = ""
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self._get_info_msg_status_for_strategy, strategy) for strategy in self.lst_strategy]
            wait(futures, timeout=1000, return_when=ALL_COMPLETED)

            for future in futures:
                msg_strategy = future.result()
                if msg_strategy != "":
                    result_msg += msg_strategy + "\n"

        return result_msg

    def get_grid(self, cpt):
        # CEDE: MULTI SYMBOL TO BE IMPLEMENTED IF EVER ONE DAY.....
        for strategy in self.lst_strategy:
            strategy.get_grid(cpt)

    def record_status(self):
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(strategy.record_status) for strategy in self.lst_strategy]
            wait(futures, timeout=1000, return_when=ALL_COMPLETED)

    def filter_position(self, current_state, id):
        current_state_filtred = current_state.copy()
        current_state_filtred["open_orders"] = current_state_filtred["open_orders"][current_state_filtred["open_orders"]['strategyId'] == id]
        return current_state_filtred

    def update_executed_trade_status(self, lst_orders):
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(strategy.update_executed_trade_status,lst_orders) for strategy in self.lst_strategy]
            wait(futures, timeout=1000, return_when=ALL_COMPLETED)

    def print_grid(self):
        for strategy in self.lst_strategy:
            strategy.print_grid()

    def save_grid_scenario(self, path, cpt):
        for strategy in self.lst_strategy:
            strategy.save_grid_scenario(path, cpt)

    def set_df_buying_size(self, df_symbol_size, cash):
        df_concat = pd.DataFrame()
        for strategy in self.lst_strategy:
            df = strategy.set_df_buying_size(df_symbol_size, cash)
            df_concat = pd.concat([df_concat, df], axis=0)
        return df_concat

    def set_df_buying_size_scenario(self, df_symbol_size, cash):
        for strategy in self.lst_strategy:
            strategy.set_df_buying_size_scenario(df_symbol_size, cash)
