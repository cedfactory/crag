from abc import ABCMeta, abstractmethod
import pandas as pd
import numpy as np
import inspect
import importlib
import os
import ast
from os import path

class RealTimeStrategy(metaclass=ABCMeta):

    def __init__(self, params=None):
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
        self.safety_SL = -60
        self.global_safety_TP = 100
        self.global_safety_SL = -30
        self.trailer_TP = 0
        self.trailer_delta = 0
        self.trigger_trailer = False
        self.trailer_global_TP = 0
        self.trailer_global_delta = 0
        self.trigger_global_trailer = False
        self.MAX_POSITION = 5    # Asset Overall Percent Size
        self.set_buying_size = False
        self.buying_size = 0
        self.logger = None
        self.id = ""
        self.min_bol_spread = 0   # Bollinger Trend startegy
        self.trade_over_range_limits = False
        self.tradingview_condition = False
        self.short_and_long = False
        if params:
            self.MAX_POSITION = params.get("max_position", self.MAX_POSITION)
            if isinstance(self.MAX_POSITION, str):
                self.MAX_POSITION = int(self.MAX_POSITION)
            symbols = params.get("symbols", "")
            if symbols != "" and path.exists("./symbols/"+symbols):
                self.lst_symbols = pd.read_csv("./symbols/"+symbols)['symbol'].tolist()
            else:
                self.lst_symbols = symbols.split(",")

            self.SL = float(params.get("sl", self.SL))
            self.TP = float(params.get("tp", self.TP))
            self.global_SL = float(params.get("global_sl", self.global_SL))
            self.global_TP = float(params.get("global_tp", self.global_TP))
            self.logger = params.get("logger", self.logger)
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
            self.trailer_delta = float(params.get("trailer_delta", self.trailer_delta))
            self.trailer_global_TP = float(params.get("trailer_global_tp", self.trailer_global_TP))
            self.trailer_global_delta = float(params.get("trailer_global_delta", self.trailer_global_delta))

        self.trigger_trailer = self.trailer_TP > 0 and self.trailer_delta > 0
        self.trigger_global_trailer = self.trailer_global_TP > 0 and self.trailer_global_delta > 0
        self.trigger_SL = self.SL < 0
        self.trigger_TP = self.TP > 0
        self.trigger_global_SL = self.global_SL < 0
        self.trigger_global_TP = self.global_TP > 0

        self.rtctrl = None
        self.match_full_position = False # disabled

        self.str_short_long_position = StrOpenClosePosition()
        self.open_long = self.str_short_long_position.get_open_long()
        self.close_long = self.str_short_long_position.get_close_long()
        self.open_short = self.str_short_long_position.get_open_short()
        self.close_short = self.str_short_long_position.get_close_short()
        self.no_position = self.str_short_long_position.get_no_position()

        self.df_long_short_record = ShortLongPosition(self.lst_symbols, self.str_short_long_position)
        if self.trigger_trailer:
            self.df_trailer_TP = TrailerTP(self.lst_symbols, self.trailer_TP, self.trailer_delta)
        if self.trigger_global_trailer:
            self.df_trailer_global_TP = TrailerGlobalTP(self.trailer_global_TP, self.trailer_global_delta)

    def log_info(self):
        pass

    def log_current_info(self):
        pass

    def log(self, msg, header="", attachments=[]):
        if self.logger:
            self.logger.log(msg, header="[#"+self.id+"] "+header, author=type(self).__name__, attachments=attachments)

    def get_name(self):
        return type(self).__name__

    def get_rtctrl(self):
        return self.rtctrl

    @abstractmethod
    def get_data_description(self):
        pass

    def set_current_data(self, current_data):
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
        return lst_features

    def condition_for_opening_long_position(self, symbol):
        return False

    def condition_for_opening_short_position(self, symbol):
        return False

    def condition_for_buying(self, symbol):
        result = False
        if self.condition_for_opening_long_position(symbol):
            print('============= OPEN LONG =============')
            self.open_long_position(symbol)
            result = True

        if self.condition_for_opening_short_position(symbol):
            if result:
                # SHORT and LONG on the same step can't be processed
                self.open_position_failed(symbol)
                result = False
            else:
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

    def condition_for_grid_out_of_range_sl_tp_signal(self, symbol, df_sl_tp):
        # SIGNAL SPECIFIC TO GRID STRATEGY
        return False

    def condition_for_selling(self, symbol, df_sl_tp):
        result = False
        if self.is_open_type_short(symbol) and self.condition_for_closing_short_position(symbol):
            result = True
            print('============= CLOSE_SHORT =============')
        elif self.is_open_type_long(symbol) and self.condition_for_closing_long_position(symbol):
            result = True
            print('============= CLOSE_LONG =============')
        elif self.condition_for_grid_out_of_range_sl_tp_signal(symbol, df_sl_tp):
            result = True
            print('============= CLOSE_GRID_SL_TP =============')
        if result:
            self.set_symbol_trailer_tp_turned_off(symbol)
        return result

    def get_df_buying_symbols(self):
        data = {'symbol':[], 'stimulus':[], 'size':[], 'percent':[], 'gridzone':[], 'pos_type':[]}
        lst_symbols = self.df_current_data.index.to_list()
        lst_symbols = self.sort_list_symbols(lst_symbols)
        for symbol in lst_symbols:
            if not self.is_open_type_short_or_long(symbol) and self.condition_for_buying(symbol):
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
        
        if self.logger != None and len(df_result) > 0:
            df_traces = df_result.copy()
            df_traces.drop(columns=['stimulus', 'gridzone'], axis=1, inplace=True)
            df_traces['percent'] = df_traces['percent'].round(decimals=2)
            df_traces['size'] = df_traces['size'].round(decimals=3)
            self.logger.log(df_traces, header="get_df_selling_symbols", author=self.get_name())
         
        return df_result

    def set_lower_zone_unengaged_position(self, symbol, zone_position):
        return True

    def set_zone_engaged(self, symbol, price):
        return True

    def get_lower_zone_buy_engaged(self, zone):
        return -10

    def get_grid_sell_condition(self, symbol, zone):
        return True

    def grid_exit_range_trend_down(self, symbol):
        return False

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

    def update(self, current_datetime, current_trades, broker_cash, broker_cash_borrowed, prices_symbols, record_info, final_date, df_balance):
        if self.rtctrl:
            self.rtctrl.update_rtctrl(current_datetime, current_trades, broker_cash, broker_cash_borrowed, prices_symbols, final_date, df_balance)
            self.rtctrl.display_summary_info(record_info)
        
    def get_symbol_buying_size(self, symbol):
        if not symbol in self.rtctrl.prices_symbols or self.rtctrl.prices_symbols[symbol] < 0: # first init at -1
            return 0, 0, 0

        if self.rtctrl.init_cash_value == 0: # CEDE DEBUG for resume
            print("init_cash_value: ", self.rtctrl.init_cash_value, " wallet_cash: ", self.rtctrl.wallet_cash)
            self.rtctrl.init_cash_value = self.rtctrl.wallet_cash

        if self.rtctrl.init_cash_value == self.rtctrl.wallet_cash \
                and not self.set_buying_size:
            self.buying_size = self.rtctrl.init_cash_value * self.MAX_POSITION / 100
            self.set_buying_size = True

        if self.rtctrl.wallet_cash > self.rtctrl.init_cash_value \
                and self.set_buying_size:
            self.buying_size = self.rtctrl.wallet_cash * self.MAX_POSITION / 100

        available_cash = self.rtctrl.wallet_cash
        if available_cash == 0:
            return 0, 0, 0

        wallet_value = available_cash

        if True:
            cash_to_buy = self.buying_size  # fixed size
        else:
            cash_to_buy = wallet_value * self.MAX_POSITION / 100 # proportional size

        if cash_to_buy > available_cash:
            cash_to_buy = available_cash

        size = cash_to_buy / self.rtctrl.prices_symbols[symbol]

        percent = cash_to_buy * 100 / wallet_value

        gridzone = -1

        # if self.is_open_type_short(symbol):
        #     size = -size

        # DEBUG CEDE:
        print("symbol: ", symbol,
              " size: ", size,
              " cash_to_buy: ", cash_to_buy,
              " available cash: ", available_cash,
              " price symbol: ", self.rtctrl.prices_symbols[symbol],
              " init_cash_value: ", self.rtctrl.init_cash_value,
              " wallet_cash: ", self.rtctrl.wallet_cash)

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

    def reset_selling_limits(self):
        return True

    def force_selling_limits(self):
        return True

    def get_selling_limit(self, symbol):
        return True

    def count_selling_limits(self, symbol):
        return True

    def set_selling_limits(self, symbol):
        return True

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
               or ((self.global_SL != 0) and (global_unrealizedPL <= self.global_SL)) \
               or ((self.global_safety_TP != 0) and (global_unrealizedPL >= self.global_safety_TP)) \
               or ((self.global_safety_SL != 0) and (global_unrealizedPL <= self.global_safety_SL))

    def condition_for_global_trailer_TP(self, global_unrealizedPL):
        if not self.trigger_global_trailer:
            return False
        if self.df_trailer_global_TP.is_off_trailer_TP(global_unrealizedPL)\
                or self.df_trailer_global_TP.is_triggered_with_no_signal(global_unrealizedPL):
            # self.df_trailer_global_TP.print_trailer_status_for_debug(global_unrealizedPL)
            return False
        if self.df_trailer_global_TP.is_triggered_with_signal(global_unrealizedPL):
            # self.df_trailer_global_TP.print_trailer_status_for_debug(global_unrealizedPL)
            return True
        return False


    def condition_for_SLTP(self, unrealizedPL):
            return ((self.TP != 0) and (unrealizedPL >= self.TP)) \
                   or ((self.SL != 0) and (unrealizedPL <= self.SL)) \
                   or ((self.safety_TP != 0) and (unrealizedPL >= self.safety_TP)) \
                   or ((self.safety_SL != 0) and (unrealizedPL <= self.safety_SL))

    def condition_trailer_TP(self, symbol, unrealizedPL):
        if not self.trigger_trailer:
            return False
        if self.df_trailer_TP.is_off_trailer_TP(symbol, unrealizedPL)\
                or self.df_trailer_TP.is_triggered_with_no_signal(symbol, unrealizedPL):
            # self.df_trailer_TP.print_trailer_status_for_debug(symbol, unrealizedPL) #CEDE DEBUG
            return False
        if self.df_trailer_TP.is_triggered_with_signal(symbol, unrealizedPL):
            # self.df_trailer_TP.print_trailer_status_for_debug(symbol, unrealizedPL) #CEDE DEBUG
            return True
        return False

    def set_symbol_trailer_tp_turned_off(self, symbol):
        if self.trigger_trailer:
            self.df_trailer_TP.set_trailer_turned_off(symbol)

    def trigger_global_trailer_status(self):
        return self.trigger_global_trailer

    def set_global_trailer_tp_turned_off(self):
        if self.trigger_global_trailer:
            self.df_trailer_global_TP.set_trailer_turned_off()

    def trigger_global_trailer_status(self):
        return self.trigger_trailer

    def sort_list_symbols(self, lst_symbols):
        return lst_symbols

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
        return self.df_short_long_position.loc[self.df_short_long_position['symbol'] == symbol, 'position'].values[0]

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
