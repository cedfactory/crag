import pandas as pd

from . import broker,rtdp_simulation
import csv

class SimBroker(broker.Broker):
    def __init__(self, params = None):
        super().__init__(params)

        self.rtdp = rtdp_simulation.SimRealTimeDataProvider(params)
        self.trades = []
        self.initialize(params)

    def initialize(self, params):
        self.start_date = 0
        self.end_date = 0
        self.intervals = 0
        if params:
            self.cash = params.get("cash", self.cash)
            self.start_date = params.get("start", self.start_date)
            self.end_date = params.get("end", self.end_date)
            self.intervals = params.get("intervals", self.intervals)

    def drop_unused_data(self, data_description):
        self.rtdp.drop_unused_data(data_description)

    def get_current_data(self, data_description):
        return self.rtdp.get_current_data(data_description)

    def get_value(self, symbol):
        return self.rtdp.get_value(symbol)

    def get_commission(self, symbol):
        return 0.0007

    def get_info(self):
        return self.start_date, self.end_date,  self.intervals

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
            df = pd.DataFrame(columns=self.trades[0].get_csv_header())
            if target.endswith(".csv"):
                for trade in self.trades:
                    list_trade_row = trade.get_csv_row()
                    df.loc[len(df)] = list_trade_row
            df.to_csv(target, sep=';')

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
