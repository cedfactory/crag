import pandas as pd
import json

from . import rtdp, rtstr

from . import utils, open_positions

class StrategyTrix(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.strategy_id = utils.generate_random_id(4)
        self.df_current_data = pd.DataFrame()

        self.side = ""
        self.grouped = False
        self.id = ""
        self.symbol = ""
        self.margin = 0
        self.trix_length = 0
        self.trix_window_size = 0
        self.trix_signal_length = 0
        self.trix_signal_type = 0
        self.long_ma_length = 0
        self.window_size = 1
        if params:
            self.id = params.get("id", self.id)
            self.lst_symbols = [params.get("strategy_symbol", self.lst_symbols)]
            self.symbol = params.get("strategy_symbol", self.symbol)
            self.margin = params.get("margin", self.margin)
            self.side = params.get("side", self.side)
            self.grouped_id = params.get("grouped_id")
            self.strategy_str_interval = params.get("interval", self.strategy_str_interval)
            self.trix_length = params.get("trix_length", self.trix_length)
            self.trix_signal_length = params.get("trix_signal_length", self.trix_signal_length)
            self.trix_signal_type = params.get("trix_signal_type", self.trix_signal_type)
            self.long_ma_length = params.get("long_ma_length", self.long_ma_length)
            self.window_size = params.get("window_size", self.window_size)
        else:
            exit(5)

        if self.side in ["long", "short"]:
            self.side = [self.side.upper()]
        elif self.side in ["both", ""]:
            self.side = ["LONG", "SHORT"]

        self.positions = open_positions.OpenPositions(self.symbol, self.strategy_id, self.grouped_id, self.get_info(), self.side)
        self.zero_print = True

        self.mutiple_strategy = False

        self.interval_map = {
            "1m": 60,
            "5m": 5 * 60,
            "15m": 15 * 60,
            "30m": 30 * 60,
            "1h": 60 * 60,
            "2h": 2 * 60 * 60,
            "4h": 4 * 60 * 60
        }

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.strategy_id = self.strategy_id
        ds.strategy_name = self.get_info()
        ds.symbols = self.lst_symbols
        ds.fdp_features = {
            "close": {},
            "trix_id1" : {"indicator": "trix",
                          "trix_length": self.trix_length,
                          "trix_signal_length": self.trix_signal_length,
                          "trix_signal_type": self.trix_signal_type,
                          "long_ma_length": self.long_ma_length,
                          "window_size": self.window_size,
                          "id": "1",
                          "output": ["trix",
                                     "trix_signal",
                                     "trix_hist",
                                     "long_ma"]
                          },
        }

        ds.features = self.get_feature_from_fdp_features(ds.fdp_features)
        ds.interval = self.interval_map.get(self.strategy_str_interval, None)
        ds.str_interval = self.strategy_str_interval
        ds.str_strategy_interval = self.strategy_str_interval
        ds.current_data = pd.DataFrame()

        return ds

    def get_info(self):
        return "StrategyTrix"

    def get_strategy_id(self):
        return self.strategy_id

    def get_interval(self):
        return self.strategy_str_interval

    def set_current_state(self, ds):
        self.df_current_data = ds.current_data.copy()

    def set_current_data(self, current_data):
        self.df_current_data = current_data

    def set_multiple_strategy(self):
        self.mutiple_strategy = True

    def get_lst_trade(self):
        open_long = close_long = open_short = close_short = False

        if "LONG" in self.side:
            open_long = self.condition_for_opening_long_position(self.symbol)
            close_long = self.condition_for_closing_long_position(self.symbol)
        if "SHORT" in self.side:
            open_short = self.condition_for_opening_short_position(self.symbol)
            close_short = self.condition_for_closing_short_position(self.symbol)

        nb_open_total_position = self.positions.get_nb_open_total_position()
        lst_order = []
        if open_long and nb_open_total_position == 0:
            lst_order.append(self.positions.create_open_market_order(self.margin, "open_long", "MARKET_OPEN_POSITION"))
        if open_short and nb_open_total_position == 0:
            lst_order.append(self.positions.create_open_market_order(self.margin, "open_short", "MARKET_OPEN_POSITION"))
        if close_long and self.positions.get_nb_open_long_position() > 0:
            lst_order.append(self.positions.create_close_market_order("close_long"))
        if close_short and self.positions.get_nb_open_short_position() > 0:
            lst_order.append(self.positions.create_close_market_order("close_short"))
        return lst_order

    def condition_for_opening_long_position(self, symbol):
        return (self.df_current_data["trix_hist_1"][symbol] > 0) \
               and (self.df_current_data["close"][symbol] > self.df_current_data["long_ma_1"][symbol])

    def condition_for_opening_short_position(self, symbol):
        return (self.df_current_data["trix_hist_1"][symbol] < 0) \
               and (self.df_current_data["close"][symbol] < self.df_current_data["long_ma_1"][symbol])

    def condition_for_closing_long_position(self, symbol):
        return (self.df_current_data["trix_hist_1"][symbol] < 0)

    def condition_for_closing_short_position(self, symbol):
        return (self.df_current_data["trix_hist_1"][symbol] > 0)

    def update_executed_trade_status(self, lst_order):
        self.positions.validate_market_order(lst_order)

    def get_strategy_stat(self):
        stat_string = json.dumps(self.positions.get_strategy_stat(), indent=4)
        return stat_string
