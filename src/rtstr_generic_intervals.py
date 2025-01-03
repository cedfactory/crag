from . import rtstr, strategies
import math
import pandas as pd
import numpy as np
from concurrent.futures import wait, ALL_COMPLETED, ThreadPoolExecutor

from . import utils
from src import logger

from os import path

class StrategyIntervalsGeneric(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)
        path_strategy_param = ""
        if params:
            path_strategy_param = params.get("path_strategy_param", path_strategy_param)

        df_strategy_param = None
        if path_strategy_param != "" and path.exists("./symbols/" + path_strategy_param):
            df_strategy_param = pd.read_csv("./symbols/" + path_strategy_param)

        df_strategy_param = df_strategy_param.groupby("symbol").apply(utils.assign_grouped_id).reset_index(drop=True)

        lst_param_strategy = []
        init_params = params
        self.lst_symbols = []
        if isinstance(df_strategy_param, pd.DataFrame):
            for index, row in df_strategy_param.iterrows():
                # Dynamically create a dictionary for each row
                params = {col: row[col] for col in df_strategy_param.columns}
                params["strategy_symbol"] = params.pop("symbol", None)  # Rename 'symbol' to 'strategy_symbol'

                lst_param_strategy.append(params)
                self.lst_symbols.append(row["symbol"])

        self.lst_symbols = list(set(self.lst_symbols))

        self.lst_strategy = []
        available_strategies = rtstr.RealTimeStrategy.get_strategies_list()
        for param_strategy in lst_param_strategy:
            if param_strategy["name"] in available_strategies:
                combined_param_dict = {**init_params, **param_strategy}
                my_strategy = rtstr.RealTimeStrategy.get_strategy_from_name(param_strategy["name"], combined_param_dict)
                self.lst_strategy.append(my_strategy)

        self.set_multiple_strategy()
        self.execute_timer = None

        self.zero_print = False
        self.execute_timer = None

        # Create a mapping of strategy IDs to strategies for faster lookup
        self.strategy_map = {strategy.get_strategy_id(): strategy for strategy in self.lst_strategy}

    def get_data_description(self):
        lst_ds = []
        for strategy in self.lst_strategy:
            ds_strategy = strategy.get_data_description()
            if isinstance(ds_strategy, list):
                lst_ds.extend(ds_strategy)
            else:
                lst_ds.append(ds_strategy)
        return lst_ds

    def set_current_data(self, lst_data):
        for data in lst_data:
            strategy_id = data.strategy_id
            strategy = self.strategy_map.get(strategy_id)

            if strategy:
                strategy.set_current_data(data.current_data)

    def set_multiple_strategy(self):
        for strategy in self.lst_strategy:
            strategy.set_multiple_strategy()

    def set_current_state(self, lst_ds):
        for strategy in self.lst_strategy:
            strategy_id = strategy.get_strategy_id()
            # avoids looping over all lst_ds once a match is found
            matching_ds = next((ds for ds in lst_ds if ds.strategy_id == strategy_id), None)
            if matching_ds:
                strategy.set_current_state(matching_ds)

    def get_lst_trade(self, lst_intervals):
        lst_trade = []
        for strategy in self.lst_strategy:
            if strategy.get_interval() in lst_intervals:
                lst_trade.extend(strategy.get_lst_trade())
        return utils.filtered_grouped_orders(lst_trade)

    def get_info(self):
        return "StrategyIntervalsGeneric"

    def get_strategy_type(self):
        return "INTERVAL"

    def set_execute_time_recorder(self, execute_timer):
        if False: # CEDE ONLY USED FOR DEBUG
            for strategy in self.lst_strategy:
                strategy.set_execute_time_recorder(execute_timer)
            self.execute_timer = execute_timer

    def update_executed_trade_status(self, lst_orders):
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(strategy.update_executed_trade_status,lst_orders) for strategy in self.lst_strategy]
            wait(futures, timeout=1000, return_when=ALL_COMPLETED)

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

    def get_strategy_stats(self, lst_intervals):
        lst_msg_stats = []
        for strategy in self.lst_strategy:
            if strategy.get_interval() in lst_intervals:
                lst_msg_stats.append(strategy.get_strategy_stat())
        return lst_msg_stats






