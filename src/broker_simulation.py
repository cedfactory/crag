from . import broker,rtdp
import csv

class SimBroker(broker.Broker):
    def __init__(self, params = None):
        super().__init__(params)

        self.rtdp = rtdp.SimRealTimeDataProvider(params)
        self.trades = []

    def initialize(self, params):
        if params:
            self.cash = params.get("cash", self.cash)

    def get_current_data(self):
        return self.rtdp.get_current_data()

    def next(self, data_description):
        return self.rtdp.next(data_description)

    def get_value(self, symbol):
        return self.rtdp.get_value(symbol)

    def get_commission(self, symbol):
        return 0.0007

    def execute_trade(self, trade):
        if trade.type == "BUY":
            if self.cash < trade.gross_price:
                return False
            self.cash = self.cash - trade.gross_price
            if self.cash < 0.00001:
                self.cash = 0
        elif trade.type == "SELL":
            # self.cash = self.cash + trade.net_price - trade.selling_fee
            self.cash = self.cash + trade.net_price
        self.trades.append(trade)
        
        return True

    def export_history(self, target):
        if len(self.trades) > 0:
            if target.endswith(".csv"):
                with open(target, 'w', newline='') as f:
                    writer = csv.writer(f, delimiter=';')
                    writer.writerow(self.trades[0].get_csv_header())
                    for trade in self.trades:
                        writer.writerow(trade.get_csv_row())
                    f.close()
            else:
                print(self.trades)

    def export_status(self):
        print("Status :")
        print("cash : {:.2f}".format(self.cash))
        total = self.cash
        for current_trade in self.trades:
            if current_trade.type == "BUY":
                symbol_value = current_trade.net_price - current_trade.net_price * self.get_commission(current_trade.symbol)
                print("{} : {:.2f}".format(current_trade.symbol, symbol_value))
                total += symbol_value
        print("Total : {:.2f}".format(total))