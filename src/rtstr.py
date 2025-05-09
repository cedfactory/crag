from abc import ABCMeta, abstractmethod
import pandas as pd
import numpy as np
import inspect
import importlib
import ast
from os import path
from datetime import datetime, timedelta
from src import logger

from . import utils

class RealTimeStrategy(metaclass=ABCMeta):

    def __init__(self, params=None):
        self.df_strategy_param = None
        self.lst_symbols = []
        self.SL = 0              # Stop Loss %
        self.TP = 0              # Take Profit %
        self.trigger_SL = False
        self.trigger_TP = False
        self.global_SL = 0       # Stop Loss % applicable to the overall portfolio
        self.global_TP = 0       # Take Profit % applicable to the overall portfolio
        self.trigger_global_TP = False
        self.trigger_global_SL = False
        self.safety_TP = 100
        self.safety_SL = -30
        self.trailer_TP = 0
        self.trailer_delta_TP = 0
        self.trigger_trailer_TP = False
        self.trailer_global_TP = 0
        self.trailer_global_delta_TP = 0
        self.trigger_global_trailer_TP = False
        self.trailer_SL = 0
        self.trigger_trailer_SL = False
        self.trailer_global_SL = 0
        self.trigger_global_trailer_SL = False
        self.drawdown_SL = 0
        self.MAX_POSITION = 1    # Asset Overall Percent Size
        self.buying_size = 0
        self.zero_print = True
        self.loggers = [ logger.LoggerConsole() ]
        self.id = ""
        self.min_bol_spread = 0   # Bollinger Trend startegy
        self.trade_over_range_limits = False
        self.tradingview_condition = False
        self.short_and_long = False
        self.strategy_interval = 0
        self.strategy_str_interval = ""
        self.candle_stick = "released"  # last released from broker vs alive
        self.high_volatility_protection = False
        self.BTC_volatility_protection = False
        self.total_postion_requested = 0
        self.upon_stop_exit_close = False
        self.upon_stop_exit_reverse = False
        self.scenario_mode = False

        if params:
            self.candle_stick = params.get("candle_stick", self.candle_stick)
            symbols = params.get("path_strategy_param", "")
            if symbols != "" and path.exists("./symbols/"+symbols):
                self.df_strategy_param = pd.read_csv("./symbols/"+symbols)
                self.lst_symbols = self.df_strategy_param['symbol'].tolist()
                self.lst_symbols = list(dict.fromkeys(self.lst_symbols))

            self.trix_period = 0
            self.stoch_rsi_period = 0

            self.SL = float(params.get("sl", self.SL))
            self.TP = float(params.get("tp", self.TP))
            self.global_SL = float(params.get("global_sl", self.global_SL))
            self.global_TP = float(params.get("global_tp", self.global_TP))

            self.zero_print = params.get("zero_print", self.zero_print)
            if isinstance(self.zero_print, str):
                self.zero_print = self.zero_print == "True"  # convert string to boolean

            loggers = params.get("loggers", self.loggers)
            if isinstance(loggers, str):
                self.loggers = logger.get_loggers(loggers)
            elif isinstance(loggers, list):
                self.loggers = loggers
            else:
                self.loggers = []

            self.id = params.get("id", self.id)
            self.min_bol_spread = params.get("min_bol_spread", self.min_bol_spread)
            if isinstance(self.min_bol_spread, str):
                self.min_bol_spread = int(self.min_bol_spread)
            self.trade_over_range_limits = params.get("trade_over_range_limits", self.trade_over_range_limits)
            if isinstance(self.trade_over_range_limits, str):
                self.trade_over_range_limits = ast.literal_eval(self.trade_over_range_limits)
            self.tradingview_condition = params.get("tradingview_condition", self.tradingview_condition)
            if isinstance(self.tradingview_condition, str):
                self.tradingview_condition = ast.literal_eval(self.tradingview_condition)
            self.short_and_long = params.get("short_and_long", self.short_and_long)
            if isinstance(self.short_and_long, str):
                self.short_and_long = ast.literal_eval(self.short_and_long)
            self.trailer_TP = float(params.get("trailer_tp", self.trailer_TP))
            self.trailer_delta_TP = float(params.get("trailer_delta_tp", self.trailer_delta_TP))
            self.trailer_global_TP = float(params.get("trailer_global_tp", self.trailer_global_TP))
            self.trailer_global_delta_TP = float(params.get("trailer_global_delta_tp", self.trailer_global_delta_TP))
            self.trailer_SL = float(params.get("trailer_sl", self.trailer_SL))
            self.trailer_global_SL = float(params.get("trailer_global_sl", self.trailer_global_SL))
            self.drawdown_SL = float(params.get("drawdown_SL", self.drawdown_SL))
            self.high_volatility_protection = params.get("high_volatility", self.high_volatility_protection)
            if isinstance(self.high_volatility_protection, str):
                self.high_volatility_protection = ast.literal_eval(self.high_volatility_protection)
            self.upon_stop_exit_close = params.get("upon_stop_exit_close", self.upon_stop_exit_close)
            self.upon_stop_exit_reverse = params.get("upon_stop_exit_reverse", self.upon_stop_exit_reverse)
            if isinstance(self.upon_stop_exit_close, str):
                self.upon_stop_exit_close = ast.literal_eval(self.upon_stop_exit_close)
            if isinstance(self.upon_stop_exit_reverse, str):
                self.upon_stop_exit_reverse = ast.literal_eval(self.upon_stop_exit_reverse)
            if self.upon_stop_exit_reverse and self.upon_stop_exit_close:
                self.upon_stop_exit_close = False
                self.upon_stop_exit_reverse = False

        self.high_volatility = HighVolatilityPause(self.high_volatility_protection)

        self.trigger_trailer_TP = self.trailer_TP > 0 and self.trailer_delta_TP > 0
        self.trigger_global_trailer_TP = self.trailer_global_TP > 0 and self.trailer_global_delta_TP > 0
        self.trigger_trailer_SL = self.trailer_SL < 0
        self.trigger_global_trailer_SL = self.trailer_global_SL < 0
        self.trigger_SL = self.SL < 0
        self.trigger_TP = self.TP > 0
        self.trigger_global_SL = self.global_SL < 0
        self.trigger_global_TP = self.global_TP > 0

        if self.trigger_trailer_TP and self.trigger_trailer_SL:
            print("#############################")
            print("## WARNING TRAILER TP & SL ##")
            print("#############################")

        self.rtctrl = None
        self.match_full_position = False # disabled

        self.str_short_long_position = StrOpenClosePosition()
        self.open_long = self.str_short_long_position.get_open_long()
        self.close_long = self.str_short_long_position.get_close_long()
        self.open_short = self.str_short_long_position.get_open_short()
        self.close_short = self.str_short_long_position.get_close_short()
        self.no_position = self.str_short_long_position.get_no_position()

        self.df_long_short_record = ShortLongPosition(self.lst_symbols, self.str_short_long_position)
        if self.trigger_trailer_TP:
            self.df_trailer_TP = TrailerTP(self.lst_symbols, self.trailer_TP, self.trailer_delta_TP)
        if self.trigger_global_trailer_TP:
            self.df_trailer_global_TP = TrailerGlobalTP(self.trailer_global_TP, self.trailer_global_delta_TP)
        if self.trigger_trailer_SL:
            self.df_trailer_SL = TrailerSL(self.lst_symbols, self.trailer_SL)
        if self.trigger_global_trailer_SL:
            self.df_trailer_global_SL = TrailerGlobalSL(self.trailer_global_SL)

    def log_info(self):
        pass

    def log_current_info(self):
        pass

    def log(self, msg, header="", attachments=[]):
        if self.zero_print:
            return
        for iter_logger in self.loggers:
            iter_logger.log(msg, header=header, author=self.get_name(), attachments=attachments)

    def get_name(self):
        return type(self).__name__

    def get_strategy_type(self):
        return ""

    def get_rtctrl(self):
        return self.rtctrl

    @abstractmethod
    def get_data_description(self):
        pass

    def set_current_data(self, current_data, current_prices):
        self.df_current_data = current_data

    def get_feature_from_fdp_features(self, fdp_features):
        lst_features = []
        for feature in fdp_features:
            if len(fdp_features[feature]) == 0:
                lst_features.append(feature)
            elif fdp_features[feature] != None:
                lst_param = list(fdp_features[feature])
                if "id" in lst_param:
                    id = "_" + fdp_features[feature]["id"]
                else:
                    id = ""
                if "n" in lst_param:
                    n = "n" + fdp_features[feature]["n"] + "_"
                else:
                    n = ""
                if not feature.startswith("postprocess"):
                    lst_features.append(fdp_features[feature]["indicator"] + id)
                if "output" in lst_param:
                    for output in fdp_features[feature]["output"]:
                        lst_features.append(output + id)
                if "indicator" in fdp_features[feature] \
                        and fdp_features[feature]["indicator"] == "shift" \
                        and "input" in lst_param:
                    for input in fdp_features[feature]["input"]:
                        lst_features.append(n + input + id)
        return list(dict.fromkeys(lst_features))

    def condition_for_opening_long_position(self, symbol):
        return False

    def condition_for_opening_short_position(self, symbol):
        return False

    def condition_for_buying(self, symbol):
        result = False
        open_long = self.condition_for_opening_long_position(symbol)
        open_short = self.condition_for_opening_short_position(symbol)

        if (open_long and open_short)\
                or (not open_long and not open_short):
            self.open_position_failed(symbol)
            result = False
        elif open_long:
            print('============= OPEN LONG =============')
            self.open_long_position(symbol)
            result = True
        elif open_short:
            print('============= OPEN SHORT =============')
            self.open_short_position(symbol)
            result = True

        return result

    def condition_for_closing_long_position(self, symbol):
        return False

    def condition_for_closing_short_position(self, symbol):
        return False

    def condition_for_global_sl_tp_signal(self):
        return (self.trigger_global_TP and self.rtctrl.wallet_value >= self.rtctrl.init_cash_value + self.rtctrl.init_cash_value * self.global_TP / 100) \
               or (self.trigger_global_SL and self.rtctrl.wallet_value <= self.rtctrl.init_cash_value + self.rtctrl.init_cash_value * self.global_SL / 100)

    def condition_for_sl_tp_signal(self, symbol, df_sl_tp):
        return (isinstance(df_sl_tp, pd.DataFrame) and self.trigger_TP and df_sl_tp['roi_sl_tp'][symbol] > self.TP) \
               or (isinstance(df_sl_tp, pd.DataFrame) and self.trigger_SL and df_sl_tp['roi_sl_tp'][symbol] < self.SL)

    def condition_for_safety_sl_tp_signal(self, symbol, df_sl_tp):
        return isinstance(df_sl_tp, pd.DataFrame)\
               and (df_sl_tp['roi_sl_tp'][symbol] < self.safety_SL
                    or df_sl_tp['roi_sl_tp'][symbol] > self.safety_TP)

    def authorize_multi_transaction_for_symbols(self):
        # Multi buy have to be specified in the strategy
        return False

    def condition_for_selling(self, symbol, df_sl_tp):
        result = False
        if self.is_open_type_short(symbol) and self.condition_for_closing_short_position(symbol):
            result = True
            print('============= CLOSE_SHORT =============')
        elif self.is_open_type_long(symbol) and self.condition_for_closing_long_position(symbol):
            result = True
            print('============= CLOSE_LONG =============')
        if result:
            self.set_symbol_trailers_turned_off(symbol)
        return result

    def get_df_buying_symbols(self):
        data = {'symbol':[], 'stimulus':[], 'size':[], 'percent':[], 'gridzone':[], 'pos_type':[]}
        lst_symbols = self.df_current_data.index.to_list()
        self.total_postion_requested = 0
        for symbol in lst_symbols:
            if ((not self.is_open_type_short_or_long(symbol)) or self.authorize_multi_transaction_for_symbols()) \
                    and self.condition_for_buying(symbol):
                size, percent, zone = self.get_symbol_buying_size(symbol)
                data['symbol'].append(symbol)
                data["stimulus"].append(self.get_open_type(symbol))
                data['size'].append(size)
                data['percent'].append(percent)
                data['gridzone'].append(zone)
                data['pos_type'].append(self.get_open_type(symbol))

        df_result = pd.DataFrame(data)
        df_result.reset_index(inplace=True, drop=True)
        
        df_result = self.get_df_selling_symbols_common(df_result)
        
        if not df_result.empty:
            df_traces = df_result.copy()
            df_traces.drop(columns=['stimulus', 'gridzone'], axis=1, inplace=True)
            self.log(df_traces, header="get_df_buying_symbols")
            
        return df_result

    # Comment: Avoid to buy more than MAX_POSITION of one asset
    #          Re ajust the % to buy in order to match MAX_POSITION
    def get_df_selling_symbols_common(self, df_result):
        if not self.rtctrl or len(self.rtctrl.df_rtctrl) == 0:
            return df_result

        df_rtctrl = self.rtctrl.df_rtctrl.copy()

        df_rtctrl.set_index('symbol', inplace=True)
        lst_symbols_to_buy = df_result.symbol.to_list()
        for symbol in lst_symbols_to_buy:
            try:
                df_result_percent = df_result.copy()
                df_result_percent.set_index('symbol', inplace=True, drop=True)
                if abs(df_rtctrl["wallet_%"][symbol]) >= self.MAX_POSITION:
                    # Symbol to be removed - Over the % limits
                    df_result.drop(df_result[df_result['symbol'] == symbol].index, inplace=True)
                elif (abs(df_rtctrl["wallet_%"][symbol]) + abs(df_result_percent['percent'][symbol])) >= self.MAX_POSITION:
                    # feature match_full_position not in use
                    # due to limitation regarding buying sizes too small
                    if self.match_full_position == True:
                        diff_percent = self.MAX_POSITION - abs(df_rtctrl["wallet_%"][symbol])
                        cash_needed = diff_percent * df_rtctrl["wallet_value"][symbol] / 100
                        size_to_match = cash_needed / self.rtctrl.prices_symbols[symbol]
                        if df_result.loc[df_result['symbol'] == symbol, 'pos_type'][0] == 'OPEN_SHORT':
                            size_to_match = -1 * size_to_match
                        df_result['size'] = np.where(df_result['symbol'] == symbol, size_to_match, df_result['size'])
                        df_result['percent'] = np.where(df_result['symbol'] == symbol, diff_percent, df_result['percent'])
                    else:
                        df_result.drop(df_result[df_result['symbol'] == symbol].index, inplace=True)
            except:
                # Stay in list
                pass
        df_result.reset_index(inplace=True, drop=True)
        return df_result

    def get_df_selling_symbols(self, lst_symbols, df_sl_tp):
        data = {'symbol':[], 'stimulus':[], 'size':[], 'percent':[], 'gridzone':[], 'pos_type':[]}
        for symbol in self.df_current_data.index.to_list():
            if symbol in lst_symbols and self.condition_for_selling(symbol, df_sl_tp):
                size, percent, zone = self.get_symbol_selling_size(symbol)
                data['symbol'].append(symbol)
                data["stimulus"].append(self.get_close_type(symbol))
                data['size'].append(size)
                data['percent'].append(percent)
                data['gridzone'].append(zone) # CEDE: Previous zone engaged
                data['pos_type'].append(self.get_close_type(symbol))

        df_result = pd.DataFrame(data)
        
        if self.zero_print == False and len(df_result) > 0:
            df_traces = df_result.copy()
            df_traces.drop(columns=['stimulus', 'gridzone'], axis=1, inplace=True)
            df_traces['percent'] = df_traces['percent'].round(decimals=2)
            df_traces['size'] = df_traces['size'].round(decimals=3)
            self.log(df_traces, header="get_df_selling_symbols")
            df_traces = None
         
        return df_result

    # @staticmethod
    def get_df_forced_selling_symbols(self):
        lst_symbols = self.rtctrl.df_rtctrl['symbol'].tolist()
        lst_size = self.rtctrl.df_rtctrl['size'].tolist()
        lst_stimulus = []
        for symbol in lst_symbols:
            lst_stimulus.append(self.get_close_type(symbol))
        lst_percent = [0] * len(lst_symbols)
        data = {'symbol': lst_symbols, 'stimulus': lst_stimulus, 'size': lst_size, 'percent': lst_percent}

        df_result = pd.DataFrame(data)

        return df_result

    def get_lst_symbols(self):
        return self.lst_symbols

    def get_nb_envelope_to_purchase(self, symbol):
        return 1

    def get_nb_symbol_position_engaged(self, symbol):
        return self.get_nb_symbol_position_engaged(symbol)

    def get_grid_buying_min_size(self, symbol):
        row_index = self.df_grid_buying_size.index[self.df_grid_buying_size['symbol'] == symbol].tolist()
        min_buying_size = self.df_grid_buying_size.at[row_index[0], "minBuyingSize"] if row_index else None
        del row_index
        return min_buying_size

    def set_scenario_mode(self):
        self.scenario_mode = True

    def get_grid_buying_size(self, symbol, strategy_id):
        if not self.scenario_mode:
            row_index = self.df_grid_buying_size.index[(self.df_grid_buying_size['symbol'] == symbol)
                                                       & (self.df_grid_buying_size['strategy_id'] == strategy_id)].tolist()
            buying_size = self.df_grid_buying_size.at[row_index[0], "buyingSize"] if row_index else None
            del row_index
            return buying_size
        else:
            return 10

    def set_df_buying_size_scenario(self, df_symbol_size, cash):
        self.df_grid_buying_size = df_symbol_size
        for symbol in df_symbol_size['symbol'].tolist():
            self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol, "buyingSize"] = 10

        return self.df_grid_buying_size

    def set_df_buying_size(self, df_symbol_size, cash):
        if not isinstance(df_symbol_size, pd.DataFrame):
            return
        # cash = 10000 # CEDE GRID SCENARIO
        self.df_grid_buying_size = pd.concat([self.df_grid_buying_size, df_symbol_size])
        self.df_grid_buying_size['margin'] = None

        for symbol in df_symbol_size['symbol'].tolist():
            dol_per_grid = self.grid_margin / (self.nb_grid + 1)
            size = dol_per_grid / ((self.grid_high + self.grid_low )/2)
            size_high = dol_per_grid / self.grid_high
            size_low = dol_per_grid / self.grid_low
            size_high = utils.normalize_size(size_high, self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol,
                                                                                    "sizeMultiplier"].values[0])
            size_low = utils.normalize_size(size_low, self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol,
                                                                                  "sizeMultiplier"].values[0])
            if (self.get_grid_buying_min_size(symbol) <= size_high) \
                    and (self.get_grid_buying_min_size(symbol) <= size_low) \
                    and (dol_per_grid > 5) \
                    and (cash >= self.grid_margin):
                size = (size_high + size_low) / 2
                size = utils.normalize_size(size,
                                           self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol,
                                                                        "sizeMultiplier"].values[0])
                self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol, "strategy_id"] = self.strategy_id
                self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol, "buyingSize"] = size    # CEDE: Average size
                self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol, "margin"] = self.grid_margin
                self.df_grid_buying_size.loc[self.df_grid_buying_size['symbol'] == symbol, "maxSizeToBuy"] = self.nb_grid
                msg = "**" + symbol + "**\n"
                msg += "**cash: " + str(round(cash, 2)) + "**\n"
                msg += "**grid_margin: " + str(round(self.grid_margin, 2)) + "**\n"
                msg += "**nb grid: " + str(self.nb_grid) + "**\n"
                msg += "**steps: " + str((self.grid_high - self.grid_low) / self.nb_grid) + "**\n"
                msg += "**amount buying > 5 usd: " + str(round(size * self.grid_low, 2)) + "**\n"
                msg += "**buying size: " + str(size) + " - $" + str(size * (self.grid_high + self.grid_low )/2) + "**\n"
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
                print(msg)
                print("GRID SETUP FAILED")
                print("set_df_buying_size")
                exit(2)
        return self.df_grid_buying_size

    def set_df_normalize_buying_size(self, df_normalized_buying_size):
        self.df_grid_buying_size = df_normalized_buying_size

    def get_symbol_buying_size(self, symbol):
        if not symbol in self.rtctrl.prices_symbols or self.rtctrl.prices_symbols[symbol] < 0:  # first init at -1
            return 0, 0, 0

        if self.rtctrl.init_cash_value == 0:  # CEDE DEBUG for resume
            print("init_cash_value: ", self.rtctrl.init_cash_value, " wallet_cash: ", self.rtctrl.wallet_cash)
            self.rtctrl.init_cash_value = self.rtctrl.wallet_cash

        if round(self.rtctrl.wallet_cash, 2) != round(self.rtctrl.cash_available, 2):
            print("traces get_symbol_buying_size - wallet cash: ", round(self.rtctrl.wallet_cash, 2),
                  " cash available: ", round(self.rtctrl.cash_available, 2))

        total_postion_engaged = self.rtctrl.get_rtctrl_nb_symbols()
        print("DEBUG - nb  position engaged: ", total_postion_engaged, " max position: ", self.MAX_POSITION)
        if total_postion_engaged + self.total_postion_requested >= self.MAX_POSITION:
            print("max position reached: ", self.MAX_POSITION)
            print("symbol: ", symbol, "size: 0")
            return 0, 0, 0
        else:
            if (self.MAX_POSITION - 1) == total_postion_engaged + self.total_postion_requested:
                self.buying_size = self.rtctrl.cash_available
            else:
                self.buying_size = self.rtctrl.cash_available / (self.MAX_POSITION - total_postion_engaged)

            self.total_postion_requested +=1
            print("DEBUG - nb  position engaged increased to: ", total_postion_engaged + self.total_postion_requested, " max position: ", self.MAX_POSITION)

        available_cash = self.rtctrl.cash_available
        if available_cash == 0:
            print("symbol: ", symbol, "size: 0 cash: 0")
            return 0, 0, 0

        cash_to_buy = self.buying_size

        if cash_to_buy > available_cash:
            cash_to_buy = available_cash

        size = cash_to_buy / self.rtctrl.prices_symbols[symbol]

        percent = cash_to_buy * 100 / self.rtctrl.init_cash_value

        gridzone = -1

        # DEBUG CEDE:
        print("symbol: ", symbol,
              " size: ", size,
              " cash_to_buy: ", cash_to_buy,
              " available cash: ", available_cash,
              " price symbol: ", self.rtctrl.prices_symbols[symbol],
              " init_cash_value: ", self.rtctrl.init_cash_value,
              " wallet_cash: ", self.rtctrl.wallet_cash,
              " available cash: ", self.rtctrl.cash_available
              )

        return size, percent, gridzone

    def get_symbol_selling_size(self, symbol):
        if not symbol in self.rtctrl.prices_symbols or self.rtctrl.prices_symbols[symbol] < 0: # first init at -1
            return 0, 0, 0

        size = self.rtctrl.df_rtctrl.loc[self.rtctrl.df_rtctrl['symbol'] == symbol, "size"].values[0]

        if self.rtctrl.wallet_cash > 0:
            percent = size * self.rtctrl.prices_symbols[symbol] * 100 / self.rtctrl.wallet_cash
        else:
            percent = 100
        gridzone = -1

        return size, percent, gridzone


    def get_portfolio_value(self):
        return self.rtctrl.df_rtctrl['portfolio_value'].sum()

    # get_df_selling_symbols and get_df_forced_exit_selling_symbols
    # could be merged in one...

    def authorize_clear_current_trades(self):
        return True

    def authorize_merge_current_trades(self):
        return True

    def authorize_merge_buy_long_position(self):
        return True

    def is_open_type(self, type):
        if type == self.open_long or type == self.open_short:
            return True
        else:
            return False

    def get_lst_opening_type(self):
        return [self.open_long, self.open_short]

    def get_lst_closing_type(self):
        return [self.close_long, self.close_short]

    def get_open_type(self, symbol):
        return self.df_long_short_record.get_position(symbol)

    def get_close_type(self, symbol):
        if self.df_long_short_record.get_position(symbol) == self.open_long:
            return self.close_long
        elif self.df_long_short_record.get_position(symbol) == self.open_short:
            return self.close_short
        return self.no_position # ERROR this case should not happen

    def get_close_type_and_close(self, symbol):
        if self.df_long_short_record.get_position(symbol) == self.open_long:
            self.df_long_short_record.set_close_short_long(symbol)
            return self.close_long
        elif self.df_long_short_record.get_position(symbol) == self.open_short:
            self.df_long_short_record.set_close_short_long(symbol)
            return self.close_short
        return self.no_position # ERROR this case should not happen

    def get_bitget_position(self, symbol, bitget_position):
        return self.df_long_short_record.get_bitget_position(symbol, bitget_position)

    def is_open_type_short(self, symbol):
        return self.get_open_type(symbol) == self.open_short

    def is_open_type_long(self, symbol):
        return self.get_open_type(symbol) == self.open_long

    def is_open_type_short_or_long(self, symbol):
        return self.is_open_type_short(symbol) or self.is_open_type_long(symbol)

    def open_position_failed(self, symbol):
        # open position broker failure -> retrun to state NO_POSITION
        self.df_long_short_record.set_close_short_long(symbol)

    def open_long_position(self,symbol):
        self.df_long_short_record.set_open_long(symbol)

    def open_short_position(self,symbol):
        self.df_long_short_record.set_open_short(symbol)

    @staticmethod
    def __get_strategies_list_from_class(strategy):
        if strategy:
            available_strategies = []
            if not inspect.isabstract(strategy):
                available_strategies = [strategy.__name__]
            substrategies = strategy.__subclasses__()
            for substrategy in substrategies:
                available_strategies.extend(RealTimeStrategy.__get_strategies_list_from_class(substrategy))
            return available_strategies
        else:
            return []

    @staticmethod
    def get_strategies_list():
        return RealTimeStrategy.__get_strategies_list_from_class(RealTimeStrategy)

    @staticmethod
    def __get_strategy_from_name(strategy, name, params=None):
        if strategy:
            if strategy.__qualname__ == name and not inspect.isabstract(strategy):
                module = importlib.import_module(strategy.__module__)
                klass = getattr(module, strategy.__qualname__)
                instance = klass(params)
                return instance
            else:
                substrategies = strategy.__subclasses__()
                for substrategy in substrategies:
                    instance = RealTimeStrategy.__get_strategy_from_name(substrategy, name, params)
                    if instance:
                        return instance
                return None
        else:
            return None

    @staticmethod
    def get_strategy_from_name(name, params=None):
        return RealTimeStrategy.__get_strategy_from_name(RealTimeStrategy, name, params)

    def get_global_SL(self):
        return self.global_SL

    def get_global_TP(self):
        return self.global_TP

    def get_SL(self):
        return self.SL

    def get_TP(self):
        return self.TP

    def condition_for_global_SLTP(self, global_unrealizedPL):
        return ((self.global_TP != 0) and (global_unrealizedPL >= self.global_TP)) \
               or ((self.global_SL != 0) and (global_unrealizedPL <= self.global_SL))

    def condition_for_max_drawdown_SL(self, drawdown_SL_percent):
        return ((not self.trigger_high_volatility_protection()) and (self.drawdown_SL != 0) and (drawdown_SL_percent <= self.drawdown_SL))

    def condition_for_global_trailer_TP(self, global_unrealizedPL):
        if not self.trigger_global_trailer_TP:
            return False
        if self.df_trailer_global_TP.is_off_trailer_TP(global_unrealizedPL)\
                or self.df_trailer_global_TP.is_triggered_with_no_signal(global_unrealizedPL):
            # self.df_trailer_global_TP.print_trailer_status_for_debug(global_unrealizedPL)
            return False
        if self.df_trailer_global_TP.is_triggered_with_signal(global_unrealizedPL):
            # self.df_trailer_global_TP.print_trailer_status_for_debug(global_unrealizedPL)
            return True
        return False

    def condition_for_global_trailer_SL(self, global_unrealizedPL):
        if not self.trigger_global_trailer_SL:
            return False
        self.df_trailer_global_SL.update_trailer_SL(global_unrealizedPL)

        if self.df_trailer_global_SL.is_triggered_with_signal(global_unrealizedPL):
            # self.df_trailer_global_SL.print_trailer_status_for_debug(global_unrealizedPL)
            return True
        return False

    def condition_for_SLTP(self, unrealizedPL):
            return ((self.TP != 0) and (unrealizedPL >= self.TP)) \
                   or ((self.SL != 0) and (unrealizedPL <= self.SL)) \
                   or ((self.safety_TP != 0) and (unrealizedPL >= self.safety_TP)) \
                   or ((self.safety_SL != 0) and (unrealizedPL <= self.safety_SL))

    def condition_trailer_TP(self, symbol, unrealizedPL):
        if not self.trigger_trailer_TP:
            return False
        if self.df_trailer_TP.is_off_trailer_TP(symbol, unrealizedPL)\
                or self.df_trailer_TP.is_triggered_with_no_signal(symbol, unrealizedPL):
            # self.df_trailer_TP.print_trailer_status_for_debug(symbol, unrealizedPL) #CEDE DEBUG
            return False
        if self.df_trailer_TP.is_triggered_with_signal(symbol, unrealizedPL):
            # self.df_trailer_TP.print_trailer_status_for_debug(symbol, unrealizedPL) #CEDE DEBUG
            return True
        return False

    def condition_trailer_SL(self, symbol, unrealizedPL):
        if not self.trigger_trailer_SL:
            return False
        self.df_trailer_SL.update_trailer_SL(symbol, unrealizedPL)

        if self.df_trailer_SL.is_triggered_with_signal(symbol, unrealizedPL):
            # self.df_trailer_SL.print_trailer_status_for_debug(symbol, unrealizedPL) #CEDE DEBUG
            return True
        return False

    def set_symbol_trailers_turned_off(self, symbol):
        self.set_symbol_trailer_tp_turned_off(symbol)
        self.set_symbol_trailer_sl_turned_off(symbol)

    def set_symbol_trailer_tp_turned_off(self, symbol):
        if self.trigger_trailer_TP:
            self.df_trailer_TP.set_trailer_turned_off(symbol)

    def set_symbol_trailer_sl_turned_off(self, symbol):
        if self.trigger_trailer_SL:
            self.df_trailer_SL.set_trailer_turned_off(symbol)

    def trigger_high_volatility_protection(self):
        return self.high_volatility_protection

    def set_high_volatility_protection_data(self, lst_row):
        self.high_volatility.set_high_volatility_protection_data(lst_row)

    def high_volatility_protection_activation(self, drawdown_SL_percent):
        if self.high_volatility.get_len_df_high_volatility() >= 2:
            max_equity = self.high_volatility.get_max_equity()
            min_equity = self.high_volatility.get_min_equity()
            equity = self.high_volatility.get_equity()
            max_BTC = self.high_volatility.get_max_BTC()
            min_BTC = self.high_volatility.get_min_BTC()
            price_BTC = self.high_volatility.get_price_BTC()
            max_equity_pct = self.high_volatility.get_max_equity_pct()
            mean_equity_pct = self.high_volatility.get_mean_equity_pct()
            max_BTC_pct = self.high_volatility.get_max_BTC_pct()

            equipty_high_vs_low = max_equity - min_equity
            equipty_high_vs_low_percent = 100 * equipty_high_vs_low / max_equity
            print("delta equity (high vs low): ", equipty_high_vs_low, " % ", equipty_high_vs_low_percent)
            equipty_high_vs_now = max_equity - equity
            equipty_high_vs_now_percent = 100 * equipty_high_vs_now / max_equity
            print("delta equity (vs now): ", equipty_high_vs_now, " % ", equipty_high_vs_now_percent)
            BTC_high_vs_low = max_BTC - min_BTC
            BTC_high_vs_low_percent = 100 * BTC_high_vs_low / max_BTC
            print("delta BTC (high vs low): ", BTC_high_vs_low, " % ", BTC_high_vs_low_percent)
            BTC_high_vs_now = max(abs(max_BTC - price_BTC), abs(min_BTC - price_BTC))
            BTC_high_vs_now_percent = 100 * BTC_high_vs_now / max_BTC
            print("delta BTC (vs now): ", BTC_high_vs_now, " % ", BTC_high_vs_now_percent)
            print("delta equity pct: ", max_equity_pct)
            print("mean equity pct: ", mean_equity_pct)
            print("delta BTC pct max: ", max_BTC_pct)

            """
            if (equipty_high_vs_low_percent >= 3) \
                    | (equipty_high_vs_now_percent >= 3) \
                    | (BTC_high_vs_low_percent >= 2) \
                    | (BTC_high_vs_now_percent >= 0.8) \
                    | (max_equity_pct >= 0.5) \
                    | (max_BTC_pct >= 0.5):
                print("PAUSE STRATEGY DUE TO HIGH VOLATILITY")
                return True
            """
            if (equipty_high_vs_low_percent >= 3) \
                    | (equipty_high_vs_now_percent >= 2) \
                    | ((self.BTC_volatility_protection)
                       & (BTC_high_vs_now_percent >= 1.2)) \
                    | self.condition_for_max_drawdown_SL(drawdown_SL_percent):
                print("PAUSE STRATEGY DUE TO HIGH VOLATILITY")
                self.high_volatility.trigger()
                return True

        self.high_volatility.shut_down()
        return False

class HighVolatilityPause():
    def __init__(self, activate):
        self.status = False
        self.duration = 60 * 60 * 6
        self.high_volatility_period = 2 * 60  # min
        self.df_high_volatility = pd.DataFrame(columns=['datetime', 'timestamp', 'symbol', 'price_BTC', 'pct_BTC', 'equity', 'pct_equity'])

        if activate:
            self.inactive = False
        else:
            self.inactive = True

    def trigger(self):
        self.status = True
        self.reset_df_high_volatility()

    def shut_down(self):
        self.status = False

    def high_volatility_pause_status(self):
        if self.inactive:
            return False
        else:
            return self.status

    def high_volatility_get_duration(self):
        return self.duration

    def reset_df_high_volatility(self):
        self.df_high_volatility = pd.DataFrame(columns=['datetime', 'timestamp', 'symbol', 'price_BTC', 'pct_BTC', 'equity', 'pct_equity'])

    def set_high_volatility_protection_data(self, lst_row):
        # ['datetime', 'timestamp', 'symbol', 'price_BTC', 'pct_BTC', 'equity', 'pct_equity']
        self.df_high_volatility.loc[len(self.df_high_volatility)] = lst_row
        self.df_high_volatility.sort_values('timestamp', ascending=True,inplace=True)
        self.df_high_volatility.reset_index(inplace=True, drop=True)

        self.df_high_volatility['pct_BTC'] = self.df_high_volatility['price_BTC'].pct_change().abs()
        self.df_high_volatility['pct_equity'] = self.df_high_volatility['equity'].pct_change().abs()

        timestamp_start_window = datetime.timestamp(self.df_high_volatility.at[len(self.df_high_volatility)-1, 'datetime'] - timedelta(minutes=self.high_volatility_period))

        self.df_high_volatility.drop(self.df_high_volatility[self.df_high_volatility['timestamp'] < timestamp_start_window].index, inplace=True)
        self.df_high_volatility.reset_index(inplace=True, drop=True)

    def get_len_df_high_volatility(self):
        return len(self.df_high_volatility)

    def get_max_equity(self):
        return self.df_high_volatility["equity"].max()

    def get_min_equity(self):
        return self.df_high_volatility["equity"].min()

    def get_equity(self):
        return self.df_high_volatility.at[len(self.df_high_volatility) - 1, "equity"]

    def get_max_BTC(self):
        return self.df_high_volatility["price_BTC"].max()

    def get_min_BTC(self):
        return self.df_high_volatility["price_BTC"].min()

    def get_price_BTC(self):
        return self.df_high_volatility.at[len(self.df_high_volatility) - 1, "price_BTC"]

    def get_max_equity_pct(self):
        return self.df_high_volatility["pct_equity"].max()

    def get_mean_equity_pct(self):
        return self.df_high_volatility["pct_equity"].mean()

    def get_max_BTC_pct(self):
        return self.df_high_volatility["pct_BTC"].max()

class ShortLongPosition():
    def __init__(self, lst_symbol, str_short_long_position):
        self.str_short_long_position = str_short_long_position
        self.open_long = self.str_short_long_position.get_open_long()
        self.close_long = self.str_short_long_position.get_close_long()
        self.open_short = self.str_short_long_position.get_open_short()
        self.close_short = self.str_short_long_position.get_close_short()
        self.no_position = self.str_short_long_position.get_no_position()

        self.df_short_long_position = pd.DataFrame(lst_symbol, columns=['symbol'])
        self.df_short_long_position['position'] = self.no_position

    def get_position(self, symbol):
        filtered = self.df_short_long_position.loc[self.df_short_long_position['symbol'] == symbol, 'position']
        if filtered.size >= 1:
            return filtered.values[0]
        return None

    def set_open_long(self, symbol):
        self.set_position(symbol, self.open_long)

    def set_open_short(self, symbol):
        self.set_position(symbol, self.open_short)

    def set_close_short_long(self, symbol):
        self.set_position(symbol,  self.no_position)

    def set_position(self, symbol, position):
        self.df_short_long_position.loc[self.df_short_long_position['symbol'] == symbol, 'position'] = position

    def get_bitget_position(self, symbol, bitget_position):
        position = self.str_short_long_position.get_bitget_str_position(bitget_position)
        self.set_position(symbol, position)
        return position

class StrOpenClosePosition():
    string = {
        "openlong" : 'OPEN_LONG',
        "openshort" : 'OPEN_SHORT',
        "closelong" : 'CLOSE_LONG',
        "closeshort" : 'CLOSE_SHORT',
        "noposition" : 'NO_POSITION'
    }

    def get_open_long(self):
        return self.string["openlong"]

    def get_open_short(self):
        return self.string["openshort"]

    def get_close_long(self):
        return self.string["closelong"]

    def get_close_short(self):
        return self.string["closeshort"]

    def get_no_position(self):
        return self.string["noposition"]

    # Specific for Grid
    def get_strategy_type(self):
        return ""

    # Specific for Grid
    def set_broker_current_state(self, current_state, df_price):
        return []

    def get_bitget_str_position(self, bitget_position):
        # ref: https://bitgetlimited.github.io/apidoc/en/mix/#holdmode
        # holdSide # Position Direction
        if bitget_position == "long":
            return self.get_open_long()
        elif bitget_position == "short":
            return self.get_open_short()

class TrailerTP():
    def __init__(self, lst_symbol, TP, delta):
        self.TP = TP
        self.delta = delta

        self.df_trailer = pd.DataFrame(lst_symbol, columns=['symbol'])
        self.df_trailer['threshold_TP'] = self.TP + self.delta
        self.df_trailer['delta'] = self.delta
        self.df_trailer['TP'] = self.TP
        self.df_trailer['status'] = False

    def is_off_trailer_TP(self, symbol, roi):
        return not self.get_trailer_status(symbol)\
               and self.is_below_threshold(symbol, roi)

    def is_triggered_with_no_signal(self, symbol, roi):
        if self.is_above_threshold(symbol, roi)\
                and not self.get_trailer_status(symbol):
            self.set_trailer_turned_on(symbol, roi)
            return True
        elif self.is_above_TP(symbol, roi)\
                and self.get_trailer_status(symbol):
            self.update_trailer_TP(symbol, roi)
            return True
        else:
            return False

    def is_triggered_with_signal(self, symbol, roi):
        if self.is_below_TP(symbol, roi) and self.get_trailer_status(symbol):
            self.set_trailer_turned_off(symbol)
            return True

    def set_trailer_turned_off(self, symbol):
        # print("********************* TRAILER TURNED OFF FOR ", symbol, "*********************")
        self.df_trailer.loc[self.df_trailer['symbol'] == symbol, 'status'] = False
        self.df_trailer.loc[self.df_trailer['symbol'] == symbol, 'TP'] = self.TP
        self.df_trailer.loc[self.df_trailer['symbol'] == symbol, 'threshold_TP'] = self.TP + self.delta

    def set_trailer_turned_on(self, symbol, roi):
        self.df_trailer.loc[self.df_trailer['symbol'] == symbol, 'status'] = True
        self.update_trailer_TP(symbol, roi)

    def update_trailer_TP(self, symbol, roi):
        previous_TP = self.get_trailer_TP(symbol)
        self.df_trailer.loc[self.df_trailer['symbol'] == symbol, 'TP'] = max(roi - self.get_trailer_delta(symbol),
                                                                             previous_TP)

    def is_below_threshold(self, symbol, roi):
        return roi < self.get_trailer_threshold(symbol)

    def is_above_threshold(self, symbol, roi):
        return roi > self.get_trailer_threshold(symbol)

    def is_below_TP(self, symbol, roi):
        return roi < self.get_trailer_TP(symbol)

    def is_above_TP(self, symbol, roi):
        return roi > self.get_trailer_TP(symbol)

    def get_trailer_threshold(self, symbol):
        return self.df_trailer.loc[self.df_trailer['symbol'] == symbol, 'threshold_TP'].values[0]

    def get_trailer_status(self, symbol):
        return self.df_trailer.loc[self.df_trailer['symbol'] == symbol, 'status'].values[0]

    def get_trailer_TP(self, symbol):
        return self.df_trailer.loc[self.df_trailer['symbol'] == symbol, 'TP'].values[0]

    def get_trailer_delta(self, symbol):
        return self.df_trailer.loc[self.df_trailer['symbol'] == symbol, 'delta'].values[0]

    def print_trailer_status_for_debug(self, symbol, roi):
        print("-------------------- TP DEBUG - ROI: ", roi,
              " - STATUS: ", self.get_trailer_status(symbol),
              " - TP: ", self.get_trailer_TP(symbol),
              " - THRESHOLD: ", self.get_trailer_threshold(symbol))

class TrailerGlobalTP():
    def __init__(self, TP, delta):
        self.init_TP = TP
        self.delta = delta

        self.threshold_TP = TP + self.delta
        self.TP = TP
        self.status = False

    def is_off_trailer_TP(self, roi):
        return not self.get_trailer_status()\
               and self.is_below_threshold(roi)

    def is_triggered_with_no_signal(self, roi):
        if self.is_above_threshold(roi)\
                and not self.get_trailer_status():
            self.set_trailer_turned_on(roi)
            return True
        elif self.is_above_TP(roi)\
                and self.get_trailer_status():
            self.update_trailer_TP(roi)
            return True
        else:
            return False

    def is_triggered_with_signal(self, roi):
        if self.is_below_TP(roi) and self.get_trailer_status():
            self.set_trailer_turned_off()
            return True

    def set_trailer_turned_off(self):
        # print("********************* GLOBAL TRAILER TURNED OFF *********************")
        self.status = False
        self.TP = self.init_TP
        self.threshold_TP = self.TP + self.delta

    def set_trailer_turned_on(self, roi):
        self.status = True
        self.update_trailer_TP(roi)

    def update_trailer_TP(self, roi):
        previous_TP = self.get_trailer_TP()
        self.TP = max(roi - self.get_trailer_delta(),
                      previous_TP)

    def is_below_threshold(self, roi):
        return roi < self.get_trailer_threshold()

    def is_above_threshold(self, roi):
        return roi > self.get_trailer_threshold()

    def is_below_TP(self, roi):
        return roi < self.get_trailer_TP()

    def is_above_TP(self, roi):
        return roi > self.get_trailer_TP()

    def get_trailer_threshold(self):
        return self.threshold_TP

    def get_trailer_status(self):
        return self.status

    def get_trailer_TP(self):
        return self.TP

    def get_trailer_delta(self):
        return self.delta

    def print_trailer_status_for_debug(self, roi):
        print("-------------------- GLOBAL DEBUG - ROI: ", roi,
              " - STATUS: ", self.get_trailer_status(),
              " - TP: ", self.get_trailer_TP(),
              " - THRESHOLD: ", self.get_trailer_threshold())

class TrailerSL():
    def __init__(self, lst_symbol, SL):
        self.SL = SL

        self.df_trailer = pd.DataFrame(lst_symbol, columns=['symbol'])
        self.df_trailer['SL'] = self.SL    # negative
        self.df_trailer['status'] = True

    def update_trailer_SL(self, symbol, roi):
        if self.is_off_trailer_SL(symbol):
            self.set_trailer_turned_on(symbol)
        previous_SL = self.get_trailer_SL(symbol)
        self.set_trailer_SL(symbol, max(roi + self.SL, previous_SL))

    def is_triggered_with_signal(self, symbol, roi):
        if self.is_below_SL(symbol, roi) and self.get_trailer_status(symbol):
            self.set_trailer_turned_off(symbol)
            return True

    def is_off_trailer_SL(self, symbol):
        return not self.get_trailer_status(symbol)

    def is_below_SL(self, symbol, roi):
        return roi < self.get_trailer_SL(symbol)

    def get_trailer_SL(self, symbol):
        return self.df_trailer.loc[self.df_trailer['symbol'] == symbol, 'SL'].values[0]

    def set_trailer_SL(self, symbol, SL):
        self.df_trailer.loc[self.df_trailer['symbol'] == symbol, 'SL'] = SL

    def get_trailer_status(self, symbol):
        return self.df_trailer.loc[self.df_trailer['symbol'] == symbol, 'status'].values[0]

    def set_trailer_turned_off(self, symbol):
        # print("********************* TRAILER TURNED OFF FOR ", symbol, "*********************")
        self.df_trailer.loc[self.df_trailer['symbol'] == symbol, 'status'] = False
        self.df_trailer.loc[self.df_trailer['symbol'] == symbol, 'SL'] = self.SL

    def set_trailer_turned_on(self, symbol):
        self.df_trailer.loc[self.df_trailer['symbol'] == symbol, 'status'] = True
        self.set_trailer_SL(symbol, self.SL)

    def print_trailer_status_for_debug(self, symbol, roi):
        print("-------------------- TP DEBUG - ROI: ", roi,
              " - STATUS: ", self.get_trailer_status(symbol),
              " - SL: ", self.get_trailer_SL(symbol))

class TrailerGlobalSL():
    def __init__(self, lst_symbol, SL):
        self.SL = SL

        self.trailer_SL = self.SL    # negative
        self.status = True

    def update_trailer_SL(self, roi):
        if self.is_off_trailer_SL():
            self.set_trailer_turned_on()
        previous_SL = self.get_trailer_SL()
        self.set_trailer_SL(max(roi + self.SL, previous_SL))

    def is_triggered_with_signal(self, roi):
        if self.is_below_SL(roi) and self.get_trailer_status():
            self.set_trailer_turned_off()
            return True

    def is_off_trailer_SL(self):
        return not self.get_trailer_status()

    def is_below_SL(self, roi):
        return roi < self.get_trailer_SL()

    def get_trailer_SL(self):
        return self.trailer_SL

    def set_trailer_SL(self, SL):
        self.trailer_SL = SL

    def get_trailer_status(self):
        return self.status

    def set_trailer_turned_off(self, symbol):
        # print("********************* TRAILER TURNED OFF FOR ", symbol, "*********************")
        self.status = False
        self.set_trailer_SL(self.SL)

    def set_trailer_turned_on(self):
        self.status = True
        self.set_trailer_SL(self.SL)

    def print_trailer_status_for_debug(self, roi):
        print("-------------------- GLOBAL DEBUG - ROI: ", roi,
              " - STATUS: ", self.get_trailer_status(),
              " - TP: ", self.get_trailer_SL())