from . import rtdp, rtstr, rtctrl
from . import rtstr_grid_trading_long, rtstr_grid_trading_short
import math
import pandas as pd
import numpy as np

from . import utils
from src import logger

class StrategyGridTradingLongShort(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)
        self.strategy_long = rtstr_grid_trading_long.StrategyGridTradingLong(params)
        self.strategy_short = rtstr_grid_trading_short.StrategyGridTradingShort(params)
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
        }

        ds.features = self.get_feature_from_fdp_features(ds.fdp_features)
        ds.interval = self.strategy_interval
        self.log("strategy: " + self.get_info())
        self.log("strategy features: " + str(ds.features))
        return ds

    def get_info(self):
        return "StrategyGridTradingLong"

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
        self.strategy_long.set_multiple_strategy()
        self.strategy_short.set_multiple_strategy()

    def set_df_normalize_buying_size(self, df_normalized_buying_size):
        self.df_grid_buying_size = df_normalized_buying_size
        self.strategy_long.set_df_grid_buying_size(self.df_grid_buying_size)
        self.strategy_short.set_df_grid_buying_size(self.df_grid_buying_size)
        del df_normalized_buying_size

    def set_execute_time_recorder(self, execute_timer):
        self.strategy_long.set_execute_time_recorder(execute_timer)
        self.strategy_short.set_execute_time_recorder(execute_timer)
        self.execute_timer = execute_timer

    def set_broker_current_state(self, current_state):
        current_state_filtered = self.filter_position(current_state, "short")
        lst_long = self.strategy_long.set_broker_current_state(current_state_filtered)
        del current_state_filtered["open_orders"]
        del current_state_filtered["open_positions"]
        del current_state_filtered["prices"]
        del current_state_filtered
        current_state_filtered = self.filter_position(current_state, "long")
        lst_short = self.strategy_short.set_broker_current_state(current_state_filtered)
        del current_state_filtered["open_orders"]
        del current_state_filtered["open_positions"]
        del current_state_filtered["prices"]
        del current_state_filtered
        del current_state["open_orders"]
        del current_state["open_positions"]
        del current_state["prices"]
        del current_state
        return lst_long + lst_short

    def set_normalized_grid_price(self, lst_symbol_plc_endstp):
        self.strategy_long.set_normalized_grid_price(lst_symbol_plc_endstp)
        self.strategy_short.set_normalized_grid_price(lst_symbol_plc_endstp)
        del lst_symbol_plc_endstp

    def activate_grid(self, current_state):
        lst_buying_market_order_long = self.strategy_long.activate_grid(current_state)
        lst_buying_market_order_short = self.strategy_short.activate_grid(current_state)
        return lst_buying_market_order_long + lst_buying_market_order_short

    def get_info_msg_status(self):
        # CEDE: MULTI SYMBOL TO BE IMPLEMENTED IF EVER ONE DAY.....

        self.record_grid_status()

        msg_long = self.strategy_long.get_info_msg_status()
        msg_short = self.strategy_short.get_info_msg_status()

        if msg_short is None and msg_long is None:
            return None
        elif msg_short is None:
            del msg_short
            return msg_long
        elif msg_long is None:
            del msg_long
            return msg_short
        else:
            msg = "LONG: " + "\n"
            msg += msg_long
            msg = "SHORT: " + "\n"
            msg += msg_short
            del msg_long
            del msg_short
            return msg

    def get_grid(self, cpt):
        # CEDE: MULTI SYMBOL TO BE IMPLEMENTED IF EVER ONE DAY.....
        for symbol in self.lst_symbols:
            return self.grid.get_grid(symbol, cpt)

    def record_grid_status(self):
        self.strategy_long.record_grid_status()
        self.strategy_short.record_grid_status()

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

