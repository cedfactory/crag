import pandas as pd

from . import broker,rtdp
import csv

class SimBroker(broker.Broker):
    def __init__(self, params = None):
        super().__init__(params)

        self.name = ""
        self.rtdp = rtdp.RealTimeDataProvider(params)
        self.trades = []

        self.start_date = ""
        self.end_date = ""
        self.intervals = ""
        if params:
            self.name = params.get("name", self.name)
            self.cash = params.get("cash", self.cash)
            if isinstance(self.cash, str):
                self.cash = int(self.cash)
            self.start_date = params.get("start_date", self.start_date)
            self.end_date = params.get("end_date", self.end_date)
            self.intervals = params.get("intervals", self.intervals)

    def ready(self):
        return True

    def _get_symbol(self, coin):
        return coin

    def _get_coin(self, symbol):
        return symbol

    def configure_for_data_description(self, data_description):
        if self.start_date != "":
            self.rtdp.record_for_data_scenario(data_description, self.start_date, self.end_date, self.intervals)

    def check_data_description(self, data_description):
        self.rtdp.check_data_description(data_description)

    def get_lst_current_data(self, lst_data_description):
        return self.rtdp.get_current_data(lst_data_description, self.fdp_url_id)

    def get_value(self, symbol):
        return self.rtdp.get_value(symbol)

    def get_commission(self, symbol):
        return 0.0007

    def get_info(self):
        return self.start_date, self.end_date,  self.intervals

    def get_balance(self):
        return None

    def export_history(self, target):
        if len(self.trades) > 0:
            df = pd.DataFrame(columns=self.trades[0].get_csv_header())
            if target.endswith(".csv"):
                for trade in self.trades:
                    list_trade_row = trade.get_csv_row()
                    df.loc[len(df)] = list_trade_row
            df.to_csv(target, sep=',')

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
