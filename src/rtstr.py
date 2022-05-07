from abc import ABCMeta, abstractmethod


class RealTimeStrategy(metaclass=ABCMeta):

    def __init__(self, params=None):
        pass

    @abstractmethod
    def get_crypto_buying_list(self, current_data):
        pass

    @abstractmethod
    def get_crypto_selling_list(self, current_trade, sell_trade):
        pass

    @abstractmethod
    def end_of_trading(self, current_trades, broker_cash):
        pass