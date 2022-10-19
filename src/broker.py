from abc import ABCMeta, abstractmethod

class Broker(metaclass = ABCMeta):
    
    def __init__(self, params = None):
        self.cash = 0
        if params:
            self.cash = params.get("cash", self.cash)
        self.rtdp = None

    def log_info(self):
        return ""

    def tick(self):
        if self.rtdp:
            return self.rtdp.tick()

    def check_data_description(self, data_description):
        if self.rtdp:
            self.rtdp.check_data_description(data_description)

    def get_current_data(self, data_description):
        if self.rtdp:
            return self.rtdp.get_current_data(data_description)
        return None

    def get_cash(self):
        return self.cash

    def get_portfolio_value(self):
        # todo : to implement
        return self.cash

    def get_current_datetime(self, format=None):
        if self.rtdp:
            return self.rtdp.get_current_datetime(format)
        return None

    def get_final_datetime(self):
        if self.rtdp:
            return self.rtdp.get_final_datetime()
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
