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
        self.global_SL = 0       # Stop Loss % applicable to the overall portfolio
        self.global_TP = 0       # Take Profit % applicable to the overall portfolio
        self.build_in_TP = 100
        self.build_in_SL = -50
        self.liquidation_SL = self.build_in_SL # Stop Loss % specific to short liquidation
        self.MAX_POSITION = 5    # Asset Overall Percent Size
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
            self.SL = int(params.get("sl", self.SL))
            self.TP = int(params.get("tp", self.TP))
            self.global_SL = int(params.get("global_sl", self.global_SL))
            self.global_TP = int(params.get("global_tp", self.global_TP))
            self.liquidation_SL = int(params.get("liquidation_SL", self.liquidation_SL))
            if self.liquidation_SL < self.build_in_SL:
                self.build_in_SL = self.liquidation_SL
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

        if self.SL == 0:     # SL == 0 => mean no SL
            self.SL = -1000
        if self.TP == 0:     # TP == 0 => mean no TP
            self.TP = 1000
        if self.global_SL == 0:     # SL == 0 => mean no SL
            self.global_SL = -1000
        if self.global_TP == 0:     # TP == 0 => mean no TP
            self.global_TP = 1000

        self.rtctrl = None
        self.match_full_position = False # disabled

        self.str_short_long_position = StrOpenClosePosition()
        self.open_long = self.str_short_long_position.get_open_long()
        self.close_long = self.str_short_long_position.get_close_long()
        self.open_short = self.str_short_long_position.get_open_short()
        self.close_short = self.str_short_long_position.get_close_short()
        self.no_position = self.str_short_long_position.get_no_position()

        self.df_long_short_record = ShortLongPosition(self.lst_symbols, self.str_short_long_position)

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
        return (self.rtctrl.wallet_value >= self.rtctrl.init_cash_value + self.rtctrl.init_cash_value * self.global_TP / 100) \
               or (self.rtctrl.wallet_value <= self.rtctrl.init_cash_value + self.rtctrl.init_cash_value * self.global_SL / 100)

    def condition_for_sl_tp_signal(self, symbol, df_sl_tp):
        return (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] > self.TP) \
               or (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] < self.SL)

    def condition_for_sl_liquidation_signal(self, symbol, df_sl_tp):
        return isinstance(df_sl_tp, pd.DataFrame) \
               and self.is_open_type_short(symbol) \
               and df_sl_tp['roi_sl_tp'][symbol] < self.liquidation_SL

    def condition_for_build_in_sl_tp_signal(self, symbol, df_sl_tp):
        return isinstance(df_sl_tp, pd.DataFrame)\
               and (df_sl_tp['roi_sl_tp'][symbol] < self.build_in_SL
                    or df_sl_tp['roi_sl_tp'][symbol] > self.build_in_TP)

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
        elif self.condition_for_sl_tp_signal(symbol, df_sl_tp)\
                or self.condition_for_global_sl_tp_signal()\
                or self.condition_for_sl_liquidation_signal(symbol, df_sl_tp)\
                or self.condition_for_build_in_sl_tp_signal(symbol, df_sl_tp)\
                or self.condition_for_grid_out_of_range_sl_tp_signal(symbol, df_sl_tp):
            result = True
            print('============= CLOSE_SL_TP =============')

        return result

    def get_df_buying_symbols(self):
        data = {'symbol':[], 'stimulus':[], 'size':[], 'percent':[], 'gridzone':[], 'pos_type':[]}
        for symbol in self.df_current_data.index.to_list():
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
            self.log(df_result, header="get_df_buying_symbols")
            
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
            self.logger.log(df_result, header="get_df_selling_symbols", author=self.get_name())
         
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

    def update(self, current_datetime, current_trades, broker_cash, broker_cash_borrowed, prices_symbols, record_info, final_date):
        if self.rtctrl:
            self.rtctrl.update_rtctrl(current_datetime, current_trades, broker_cash, broker_cash_borrowed, prices_symbols, final_date)
            self.rtctrl.display_summary_info(record_info)
        
    def get_symbol_buying_size(self, symbol):
        if not symbol in self.rtctrl.prices_symbols or self.rtctrl.prices_symbols[symbol] < 0: # first init at -1
            return 0, 0, 0

        available_cash = self.rtctrl.wallet_cash
        if available_cash == 0:
            return 0, 0, 0

        wallet_value = available_cash

        cash_to_buy = wallet_value * self.MAX_POSITION / 100

        if cash_to_buy > available_cash:
            cash_to_buy = available_cash

        size = cash_to_buy / self.rtctrl.prices_symbols[symbol]

        percent = cash_to_buy * 100 / wallet_value

        gridzone = -1

        if self.is_open_type_short(symbol):
            size = -size

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

class ShortLongPosition():
    def __init__(self, lst_symbol, str_short_long_position):
        self.str_short_long_position = str_short_long_position
        self.open_long = self.str_short_long_position.get_open_long()
        self.close_long = self.str_short_long_position.get_close_long()
        self.open_short = self.str_short_long_position.get_open_short()
        self.close_short = self.str_short_long_position.get_close_short()
        self.no_position = self.str_short_long_position.get_no_position()

        self.df_short_long_position = pd.DataFrame(lst_symbol, columns=['symbol'])
        self.df_short_long_position['position'] =  self.no_position

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