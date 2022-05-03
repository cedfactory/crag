from abc import ABCMeta, abstractmethod


class RealTimeStrategy(metaclass=ABCMeta):

    def __init__(self, params=None):
        pass

    @abstractmethod
    def get_crypto_buying_list(self, current_data):
        pass

    @abstractmethod
    def get_crypto_selling_list(self, current_data, lst_symbols_to_buy):
        pass

    @abstractmethod
    def get_crypto_holding_list(self,  current_trade, sell_trade, lst_symbols_to_buy, df_rtctrl):
        pass
