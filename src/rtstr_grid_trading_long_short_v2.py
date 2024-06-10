from . import rtdp, rtstr, rtctrl
from . import rtstr_grid_trading_long, rtstr_grid_trading_short, rtstr_grid_trading_breakout, rtstr_grid_trading_long_v2, rtstr_grid_trading_generic_v2, rtstr_grid_trading_short_v2
import math
import pandas as pd
import numpy as np

from . import utils
from src import logger

class StrategyGridTradingLongShortV2(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)
        self.lst_strategy = []
        self.strategy_long = rtstr_grid_trading_long_v2.StrategyGridTradingLongV2(params)
        # self.strategy_long = rtstr_grid_trading_generic_v2.StrategyGridTradingGenericV2(params)
        self.lst_strategy.append(self.strategy_long)
        if False:
            self.strategy_short = rtstr_grid_trading_short_v2.StrategyGridTradingShort(params)
            self.lst_strategy.append(self.strategy_short)
            self.strategy_breakout_long = rtstr_grid_trading_breakout.StrategyGridTradingBreakOut(params)
            self.lst_strategy.append(self.strategy_breakout_long)
            self.strategy_breakout_short = rtstr_grid_trading_breakout.StrategyGridTradingBreakOut(params)
            self.lst_strategy.append(self.strategy_breakout_short)
        self.set_multiple_strategy()
        self.execute_timer = None

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        self.zero_print = False
        self.execute_timer = None

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
        return "StrategyGridTradingLongShortv2"

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
        for strategy in self.lst_strategy:
            strategy.set_multiple_strategy()

    def set_df_normalize_buying_size(self, df_normalized_buying_size):
        self.df_grid_buying_size = df_normalized_buying_size
        for strategy in self.lst_strategy:
            strategy.set_df_grid_buying_size(self.df_grid_buying_size)
        del df_normalized_buying_size

    def set_execute_time_recorder(self, execute_timer):
        for strategy in self.lst_strategy:
            strategy.set_execute_time_recorder(execute_timer)
        self.execute_timer = execute_timer

    def set_broker_current_state(self, current_state):
        lst_position = []
        for strategy in self.lst_strategy:
            str_current_state = strategy.get_str_current_state_filter()
            current_state_filtered = self.filter_position(current_state, str_current_state)  #######################
            lst_position.extend(strategy.set_broker_current_state(current_state_filtered))
            del current_state_filtered["open_orders"]
            del current_state_filtered["open_positions"]
            del current_state_filtered["prices"]
            del current_state_filtered

        del current_state["open_orders"]
        del current_state["open_positions"]
        del current_state["prices"]
        del current_state
        return lst_position

    def set_normalized_grid_price(self, lst_symbol_plc_endstp):
        for strategy in self.lst_strategy:
            strategy.set_normalized_grid_price(lst_symbol_plc_endstp)

        del lst_symbol_plc_endstp

    def activate_grid(self, current_state):
        lst_buying_orders = []
        for strategy in self.lst_strategy:
            lst_buying_orders.extend(strategy.activate_grid(current_state))
        return lst_buying_orders

    def get_info_msg_status(self):
        # CEDE: MULTI SYMBOL TO BE IMPLEMENTED IF EVER ONE DAY.....
        self.record_grid_status()

        msg = ''
        for strategy in self.lst_strategy:
            msg_strategy = strategy.get_info_msg_status()
            if msg_strategy != "":
                msg += strategy.get_info() + ": \n"
                msg += msg_strategy
        return msg

    def get_grid(self, cpt):
        # CEDE: MULTI SYMBOL TO BE IMPLEMENTED IF EVER ONE DAY.....
        for strategy in self.lst_strategy:
            strategy.get_grid(cpt)

    def record_grid_status(self):
        for strategy in self.lst_strategy:
            strategy.record_grid_status()

    def filter_position(self, current_state, side):
        current_state_filtred = current_state.copy()
        if side == "long":
            lst_patterns = ["open_short", "close_short"]
            filter_side = 'short'
        elif side == "short":
            lst_patterns = ["open_long", "close_long"]
            filter_side = 'long'
        current_state_filtred["open_orders"] = current_state_filtred["open_orders"][current_state_filtred["open_orders"]['side'].isin(lst_patterns)]
        current_state_filtred["open_positions"] = current_state_filtred["open_positions"][current_state_filtred["open_positions"]['holdSide'] == filter_side]
        return current_state_filtred

    def update_executed_trade_status(self, lst_orders):
        for strategy in self.lst_strategy:
            strategy.update_executed_trade_status(lst_orders)

    def print_grid(self):
        for strategy in self.lst_strategy:
            strategy.print_grid()

    def save_grid_scenario(self, path, cpt):
        for strategy in self.lst_strategy:
            strategy.save_grid_scenario(path, cpt)