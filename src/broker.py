from abc import ABCMeta, abstractmethod
import csv

class Broker(metaclass = ABCMeta):
    
    def __init__(self, params = None):
        self.cash = 0
    
    @abstractmethod
    def initialize(self, params):
        pass

    def get_cash(self):
        return self.cash

    @abstractmethod
    def get_commission(self, symbol):
        pass

    @abstractmethod
    def execute_trade(self, trade):
        pass

    @abstractmethod
    def export_history(self, target):
        pass


class BrokerSimulation(Broker):
    def __init__(self, params = None):
        super().__init__(params)

        self.trades = []

    def initialize(self, params):
        if params:
            self.cash = params.get("cash", self.cash)

    def get_commission(self, symbol):
        return 0.07

    def execute_trade(self, trade):
        print("execute trade {}".format(trade.id))
        if self.cash < trade.gross_price:
            return False
        self.cash = self.cash - trade.gross_price
        self.trades.append(trade)
        return True

    def export_history(self, target):
        if target.endswith(".csv"):
            with open(target, 'w') as f:
                writer = csv.writer(f)
                writer.writerow(self.trades[0].get_csv_header())
                for trade in self.trades:
                    writer.writerow(trade.get_csv_row())
                f.close()
        else:
            print(self.trades)