from abc import ABCMeta, abstractmethod

class Broker(metaclass = ABCMeta):
    
    def __init__(self, params = None):
        self.cash = 0
        if params:
            self.cash = params.get("cash", self.cash)
        self.rtdp = None

    @abstractmethod
    def initialize(self, params):
        pass

    def get_current_data(self):
        return None

    def next(self, data_description):
        return None

    def get_cash(self):
        return self.cash

    def get_current_datetime(self):
        if self.rtdp:
            return self.rtdp.get_current_datetime()
        return None

    @abstractmethod
    def get_value(self, symbol):
        pass

    @abstractmethod
    def get_commission(self, symbol):
        pass

    @abstractmethod
    def execute_trade(self, trade):
        pass

    @abstractmethod
    def export_history(self, target):
        pass
