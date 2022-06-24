from abc import ABCMeta, abstractmethod
import inspect
import importlib

class RealTimeStrategy(metaclass=ABCMeta):

    def __init__(self, params=None):
        pass

    @abstractmethod
    def get_data_description(self):
        pass

    @abstractmethod
    def get_df_buying_symbols(self):
        pass

    @abstractmethod
    def get_df_selling_symbols(self, lst_symbols, df_sl_tp):
        pass

    @abstractmethod
    def get_df_forced_exit_selling_symbols(self, lst_symbols):
        pass

    @abstractmethod
    def update(self, current_datetime, current_trades, broker_cash, prices_symbols):
        pass


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