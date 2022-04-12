from datetime import datetime


class Trade:
    id = 0
    def __init__(self):
        self.id = Trade.id
        Trade.id = Trade.id + 1
        self.time = datetime.now()

    def dump(self):
        print("-> trade {}:".format(self.id))
        print("   {} {} ({})".format(self.type, self.symbol, self.stimulus))
        print("   symbol price : {:.3f}".format(self.symbol_price))
        print("   size : {:.3f}".format(self.size))
        print("   net price : {:.3f}".format(self.net_price))
        print("   commission : {:.3f}".format(self.commission))
        print("   gross price : {:.3f}".format(self.gross_price))

    def get_csv_header(self):
        return ["id", "time", "type", "stimulus", "symbol", "symbol_price", "size", "net_price", "commission", "gross_price"]

    def get_csv_row(self):
        return [self.id, self.time, self.type, self.stimulus, self.symbol, self.symbol_price, self.size, self.net_price, self.commission, self.gross_price]

