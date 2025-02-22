import pandas as pd
import json

from . import rtdp, rtstr

from . import utils, open_positions

class StrategyObelix(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.strategy_id = utils.generate_random_id(4)
        self.df_current_data = pd.DataFrame()

        self.side = ""
        self.grouped = False
        self.id = ""
        self.symbol = ""
        self.margin = 0
        self.ma_type = ""
        self.trend_type = ""
        self.high_offset = 0
        self.low_offset = 0
        self.zema_len_buy = 0
        self.zema_len_sell = 0
        self.ssl_atr_period = 0
        self.conversion_line_period = 0
        self.base_line_periods = 0
        self.lagging_span = 0
        self.displacement = 0
        self.current_price = None
        self.trend_str_interval = "1h"
        self.df_current_data_ichimoku = None
        self.df_current_data_trend = None
        self.df_current_data_zerolag_ma = None
        self.presetTakeProfitPrice = ""
        self.presetStopLossPrice = ""
        self.previous_stat_hash = 0
        if params:
            self.id = params.get("id", self.id)
            self.lst_symbols = [params.get("strategy_symbol", self.lst_symbols)]
            self.symbol = params.get("strategy_symbol", self.symbol)
            self.margin = params.get("margin", self.margin)
            self.side = params.get("side", self.side)
            self.grouped_id = params.get("grouped_id")
            # ZEROLAG PARAMS
            self.strategy_str_interval = params.get("interval", self.strategy_str_interval)
            self.ma_type = params.get("ma_type", self.ma_type)
            self.high_offset = params.get("high_offset", self.high_offset)
            self.low_offset = params.get("low_offset", self.low_offset)
            self.zema_len_buy = params.get("zema_len_buy", self.zema_len_buy)
            self.zema_len_sell = params.get("zema_len_sell", self.zema_len_sell)
            self.ssl_atr_period = params.get("ssl_atr_period", self.ssl_atr_period)
            # ICHIMOKU PARAMS
            self.conversion_line_period = params.get("conversion_line_period", self.conversion_line_period)
            self.base_line_periods = params.get("base_line_periods", self.base_line_periods)
            self.lagging_span = params.get("lagging_span", self.lagging_span)
            self.displacement = params.get("displacement", self.displacement)
            # TREND PARAMS
            self.trend_type = params.get("trend_type", self.trend_type)
            self.trend_str_interval = params.get("interval_trend", self.trend_str_interval)

            self.presetTakeProfitPrice = str(params.get("TP", self.presetTakeProfitPrice))
            self.presetStopLossPrice = str(params.get("SL", self.presetStopLossPrice))
        else:
            exit(5)

        if self.side in ["long", "short"]:
            self.side = [self.side.upper()]
        elif self.side in ["both", ""]:
            self.side = ["LONG", "SHORT"]

        self.df_current_data_zerolag_ma = None
        self.df_current_data_ichimoku = None

        self.positions = open_positions.OpenPositions(self.symbol, self.strategy_id, self.grouped_id, self.get_info(), self.side,
                                                      presetTakeProfitPrice=self.presetTakeProfitPrice,
                                                      presetStopLossPrice=self.presetStopLossPrice)
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
        lst_fdp_features = [
            {
                "close": {},
                "zerolag_id1": {
                    "indicator": "zerolag_ma",
                    "ma_type": self.ma_type,
                    "high_offset": self.high_offset,
                    "low_offset": self.low_offset,
                    "zema_len_buy": self.zema_len_buy,
                    "zema_len_sell": self.zema_len_sell,
                    "window_size": max(self.zema_len_buy, self.zema_len_sell),
                    "id": "1",
                    "output": [
                        "zerolag_ma_buy_adj",
                        "zerolag_ma_sell_adj"
                    ]
                },
            },
            {
                "close": {},
                "trend_id1": {
                    "indicator": "trend_indicator",
                    "trend_type": self.trend_type,
                    "id": "1",
                    "window_size": 30,
                    "output": [
                        "trend_signal"
                    ]
                },
            },
            {
                "close": {},
                "ichimoku_id1": {
                    "indicator": "ichimoku",
                    "conversion_line_period": self.conversion_line_period,
                    "base_line_periods": self.base_line_periods,
                    "lagging_span": self.lagging_span,
                    "displacement": self.displacement,
                    "ssl_atr_period": self.ssl_atr_period,
                    "window_size": max(self.base_line_periods, self.lagging_span, self.displacement,
                                       self.ssl_atr_period),
                    "id": "1",
                    "output": [
                        "ichimoku_valid",
                        "trend_pulse",
                        "bear_trend_pulse"
                    ]
                },
            }
        ]

        lst_ds = []
        for fdp_feature in lst_fdp_features:
            ds = rtdp.DataDescription()
            ds.strategy_id = self.strategy_id
            ds.strategy_name = self.get_info()
            ds.symbols = self.lst_symbols
            ds.fdp_features = fdp_feature
            ds.features = self.get_feature_from_fdp_features(ds.fdp_features)
            if fdp_feature.get("ichimoku_id1", {}).get("indicator") == "ichimoku"\
                    or fdp_feature.get("trend_id1", {}).get("indicator") == "trend_indicator":
                ds.interval = self.interval_map.get(self.trend_str_interval, None)
                ds.str_interval = self.trend_str_interval
            else:
                ds.interval = self.interval_map.get(self.strategy_str_interval, None)
                ds.str_interval = self.strategy_str_interval
            ds.str_strategy_interval = self.strategy_str_interval
            ds.current_data = pd.DataFrame()
            ds.hash_id = hash(ds.candle_stick + str(ds.symbols) + repr(ds.fdp_features) + ds.str_interval)
            lst_ds.append(ds)

        return lst_ds

    def get_info(self):
        return "StrategyObelix"

    def get_strategy_id(self):
        return self.strategy_id

    def get_interval(self):
        return self.strategy_str_interval # Used to schedule

    def set_current_state(self, ds):
        self.df_current_data = ds.current_data.copy()

    def set_current_price(self, current_price, available_margin):
        self.current_price = current_price[self.symbol]
        self.margin = available_margin

    def set_current_data(self, current_data):
        if len([col for col in current_data.columns if col.startswith("zerolag_ma")]) > 0:
            self.df_current_data_zerolag_ma = current_data
        if len([col for col in current_data.columns if col.startswith("ichimoku")]) > 0:
            self.df_current_data_ichimoku = current_data
        if len([col for col in current_data.columns if col.startswith("trend_signal")]) > 0:
            self.df_current_data_trend = current_data

    def set_multiple_strategy(self):
        self.mutiple_strategy = True

    def get_engaged_position_status(self):
        return self.positions.get_nb_open_total_position()

    def get_lst_trade(self):
        if (((self.df_current_data_ichimoku is None)
             and (self.df_current_data_trend is None))
                or (self.df_current_data_ichimoku.empty
                    and self.df_current_data_trend.empty)
                or self.df_current_data_zerolag_ma is None
                or self.df_current_data_zerolag_ma.empty
        ):
            return []

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
        # If both trend and ichimoku data are missing, we cannot open a long position.
        if self.df_current_data_trend is None and self.df_current_data_ichimoku is None:
            return False

        # Define the condition to check if the price is below the moving average.
        below_ma = (
                # self.df_current_data_zerolag_ma["close"][symbol]
                self.current_price < self.df_current_data_zerolag_ma["zerolag_ma_buy_adj_1"][symbol]
        )

        # If ichimoku data is available, prioritize using it.
        if self.df_current_data_ichimoku is not None:
            ichimoku_valid = self.df_current_data_ichimoku["ichimoku_valid_1"][symbol] > 0
            trend_pulse = self.df_current_data_ichimoku["trend_pulse_1"][symbol] == 0
            return ichimoku_valid and trend_pulse and below_ma

        # Otherwise, if trend data is available, use that.
        if self.df_current_data_trend is not None:
            trend_up = self.df_current_data_trend["trend_signal_1"] == 1
            return trend_up and below_ma

        # Fallback to False (this line is mostly for clarity).
        return False

    def condition_for_opening_short_position(self, symbol):
        # If both trend and ichimoku data are missing, we cannot open a long position.
        if self.df_current_data_trend is None and self.df_current_data_ichimoku is None:
            return False

        above_ma = (
                # self.df_current_data_zerolag_ma["close"][symbol]
                self.current_price > self.df_current_data_zerolag_ma["zerolag_ma_buy_adj_1"][symbol]
        )

        # If ichimoku data is available, prioritize using it.
        if self.df_current_data_ichimoku is not None:
            ichimoku_valid = self.df_current_data_ichimoku["ichimoku_valid_1"][symbol] > 0
            bear_trend_pulse = self.df_current_data_ichimoku["bear_trend_pulse_1"][symbol] == 0
            return ichimoku_valid and bear_trend_pulse and above_ma

        # Otherwise, if trend data is available, use that.
        if self.df_current_data_trend is not None:
            trend_up = self.df_current_data_trend["trend_signal_1"] == -1
            return trend_up and above_ma

        # Fallback to False (this line is mostly for clarity).
        return False

    def condition_for_closing_long_position(self, symbol):
        # close_price = self.df_current_data_zerolag_ma["close"][symbol]
        close_price = self.current_price
        sell_adj = self.df_current_data_zerolag_ma["zerolag_ma_sell_adj_1"][symbol]
        return close_price > sell_adj

    def condition_for_closing_short_position(self, symbol):
        # close_price = self.df_current_data_zerolag_ma["close"][symbol]
        close_price = self.current_price
        sell_adj = self.df_current_data_zerolag_ma["zerolag_ma_sell_adj_1"][symbol]
        return close_price < sell_adj

    def update_executed_trade_status(self, lst_order):
        self.positions.validate_market_order(lst_order)

    def get_enganged_position_status(self):
        return self.positions.get_nb_open_total_position()

    def get_strategy_stat(self):
        stat_hash = self.positions.get_stat_hash()
        if self.previous_stat_hash == stat_hash:
            return None
        else:
            self.previous_stat_hash = stat_hash
            stat_string = json.dumps(self.positions.get_strategy_stat(), indent=4)
            return stat_string

    def get_lst_sltp(self):
        return self.positions.get_lst_sltp_orderId()

    def update_sltp_order_status(self, sltp_order):
        self.positions.update_sltp_orderId(sltp_order)