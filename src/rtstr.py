from abc import ABCMeta, abstractmethod


class RealTimeStrategy(metaclass=ABCMeta):

    def __init__(self, params=None):
        pass

    @abstractmethod
    def get_crypto_buying_list(self, current_data, df_rtctrl):
        pass

    @abstractmethod
    def get_crypto_selling_list(self, current_trade, sell_trade, df_rtctrl):
        pass
