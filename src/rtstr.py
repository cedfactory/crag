from abc import ABCMeta, abstractmethod


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
    def get_df_selling_symbols(self, lst_symbols):
        pass

    @abstractmethod
    def update(self, current_trades, broker_cash):
        pass

    @abstractmethod
    def get_price_symbol(self, symbol):
        pass