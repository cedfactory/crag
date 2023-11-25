from rich import print,inspect

class Trade:
    id = 0
    def __init__(self, current_datetime=None):
        self.id = Trade.id
        Trade.id = Trade.id + 1
        self.time = current_datetime

        self.sell_id = ""
        self.stimulus = ""
        self.roi = ""
        self.buying_time = ""
        self.selling_fee = ""

    @staticmethod
    def reset_id():
        Trade.id = 0
        
    def dump(self):
        inspect(self)
        return
        print("-> trade {}:".format(self.id))
        print("   {} {} ({})".format(self.type, self.symbol, self.stimulus))
        print("   symbol price : {:.3f}".format(self.symbol_price))
        print("   size : {:.3f}".format(self.size))
        print("   net price : {:.3f}".format(self.net_price))
        print("   commission : {:.3f}".format(self.commission))
        print("   gross price : {:.3f}".format(self.gross_price))

    def get_csv_header(self):
        return ["transaction_id", "time", "buying_time","type", 'sell_id', "stimulus", "symbol", "buying_price", "symbol_price", "net_size", "net_price", "buying_fees", "selling_fees", "gross_price", "transaction_roi%", "remaining_cash", "portfolio_value", "wallet_value", "wallet_roi%"]

    def get_csv_row(self):
        return [self.id, self.time, self.buying_time,self.type, self.sell_id,self.stimulus, self.symbol, self.buying_price, self.symbol_price, self.net_size, self.net_price, self.buying_fee, self.selling_fee, self.gross_price, self.roi, self.cash, self.portfolio_value, self.wallet_value, self.wallet_roi]

