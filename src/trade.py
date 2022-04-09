from datetime import datetime


class Trade:
    id = 0
    def __init__(self):
        self.id = Trade.id
        Trade.id = Trade.id + 1
        self.time = datetime.now()

    def dump(self):
        print("{} : {} for {}".format(self.id, self.symbol, self.net_price))

    def get_csv_header(self):
        return ["id", "time", "symbol", "symbol_price", "size", "net_price", "commission", "gross_price"]

    def get_csv_row(self):
        return [self.id, self.time, self.symbol, self.symbol_price, self.size, self.net_price, self.commission, self.gross_price]

