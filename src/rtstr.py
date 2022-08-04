from abc import ABCMeta, abstractmethod
import pandas as pd
import inspect
import importlib

class RealTimeStrategy(metaclass=ABCMeta):

    def __init__(self, params=None):
        self.SL = 0             # Stop Loss %
        self.TP = 0             # Take Profit %
        self.logger = None
        if params:
            self.SL = params.get("sl", self.SL)
            self.TP = params.get("tp", self.TP)
            self.logger = params.get("logger", self.logger)
        self.str_sl = "sl" + str(self.SL)
        self.str_tp = "tp" + str(self.TP)

        if self.SL == 0:     # SL == 0 => mean no SL
            self.SL = -1000
        if self.TP == 0:     # TP == 0 => mean no TP
            self.TP = 1000

        self.SPLIT = 5           # Asset Split %
        self.MAX_POSITION = 5    # Asset Overall Percent Size
        self.match_full_position = True

    @abstractmethod
    def get_data_description(self):
        pass

    def set_current_data(self, current_data):
        self.df_current_data = current_data

    @abstractmethod
    def condition_for_buying(self, symbol):
        pass

    @abstractmethod
    def condition_for_selling(self, symbol, df_sl_tp):
        pass

    def get_df_buying_symbols(self):
        data = {'symbol':[], 'size':[], 'percent':[]}
        for symbol in self.df_current_data.index.to_list():
            if self.condition_for_buying(symbol) == True:
                size, percent = self.get_symbol_buying_size(symbol)
                data['symbol'].append(symbol)
                data['size'].append(size)
                data['percent'].append(percent)

        df_result = pd.DataFrame(data)
        df_result.reset_index(inplace=True, drop=True)
        
        df_result = self.get_df_selling_symbols_common(df_result)
        
        if self.logger != None and len(df_result) > 0:
            df_result["symbol"].values.tolist()
            str_symbols = ", ".join(df_result["symbol"].values.tolist())
            self.logger.log("[get_df_buying_symbols] : "+str_symbols)
            
        return df_result

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
                if df_rtctrl["wallet_%"][symbol] >= self.MAX_POSITION:
                    # Symbol to be removed - Over the % limits
                    df_result.drop(df_result[df_result['symbol'] == symbol].index, inplace=True)
                elif (df_rtctrl["wallet_%"][symbol] + df_result_percent['percent'][symbol]) >= self.MAX_POSITION:
                    if self.match_full_position == True:
                        diff_percent = self.MAX_POSITION - df_rtctrl["wallet_%"][symbol]
                        cash_needed = diff_percent * df_rtctrl["wallet_value"][symbol] / 100
                        size_to_match = cash_needed / self.rtctrl.prices_symbols[symbol]
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
        data = {'symbol':[], 'stimulus':[]}
        for symbol in self.df_current_data.index.to_list():
            if self.condition_for_selling(symbol, df_sl_tp):
                data["symbol"].append(symbol)
                data["stimulus"].append("SELL")

                if(isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] > self.TP):
                    print('TAKE PROFIT: ', symbol, ": ", df_sl_tp['roi_sl_tp'][symbol])
                if(isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] < self.SL):
                    print('STOP LOST: ', symbol, ": ", df_sl_tp['roi_sl_tp'][symbol])

        df_result = pd.DataFrame(data)
        
        if self.logger != None and len(df_result) > 0:
            self.logger.log(df_result)
         
        return df_result

    def update(self, current_datetime, current_trades, broker_cash, prices_symbols, record_info):
        if self.rtctrl:
            self.rtctrl.update_rtctrl(current_datetime, current_trades, broker_cash, prices_symbols)
            self.rtctrl.display_summary_info(record_info)
        
    def get_symbol_buying_size(self, symbol):
        if not symbol in self.rtctrl.prices_symbols or self.rtctrl.prices_symbols[symbol] < 0: # first init at -1
            return 0, 0

        available_cash = self.rtctrl.wallet_cash
        if available_cash == 0:
            return 0, 0

        wallet_value = available_cash

        cash_to_buy = wallet_value * self.SPLIT / 100

        if cash_to_buy > available_cash:
            cash_to_buy = available_cash

        size = cash_to_buy / self.rtctrl.prices_symbols[symbol]

        percent =  cash_to_buy * 100 / wallet_value

        return size, percent

    def get_portfolio_value(self):
        return self.rtctrl.df_rtctrl['portfolio_value'].sum()

    # get_df_selling_symbols and get_df_forced_exit_selling_symbols
    # could be merged in one...
    def get_df_forced_exit_selling_symbols(self, lst_symbols):
        data = {'symbol':[], 'stimulus':[]}
        if hasattr(self, 'df_current_data'):
            for symbol in self.df_current_data.index.to_list():
                data["symbol"].append(symbol)
                data["stimulus"].append("SELL")

        df_result = pd.DataFrame(data)
        return df_result


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
