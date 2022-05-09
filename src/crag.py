import time
import pandas as pd
from . import trade

class Crag:
    def __init__(self, params = None):

        self.rtdp = None
        self.broker = None
        self.rtstr = None
        if params:
            self.rtdp = params.get("rtdp", self.rtdp)
            self.broker = params.get("broker", self.broker)
            self.rtstr = params.get("rtstr", self.rtstr)

        self.log = []
        self.current_step = -1

        self.current_trades = []

        self.cash = 0
        self.portfolio_value = 0
        self.wallet_value = 0

        self.static_size = True
        self.size = 1

    def run(self, interval=1):
        done = False
        while not done:
            # log
            self.current_step = self.current_step + 1
            self.log.append({})

            done = not self.step()
            time.sleep(interval)
            self.export_history("broker_history.csv")

    def step(self):
        print("[Crag] new step...")

        # update all the data
        self.rtstr.update(self.current_trades, self.broker.get_cash())

        ds = self.rtstr.get_data_description()
        df_current_data = self.rtdp.next(ds)
        if df_current_data is None:
            return False

        self.rtstr.set_current_data(df_current_data)

        # execute trading
        self.trade()

        return True

    def add_to_log(self, key, value):
        self.log[self.current_step][key] = value

    def export_history(self, target=None):
        self.broker.export_history(target)

    def manage_current_data(self):
        print("[Crag.manage_current_data]")

        current_data = self.rtdp.get_current_data()
        lst_symbols_to_buy = self.rtstr.get_crypto_buying_list(current_data)

        # log
        self.add_to_log("lst_symbols_to_buy", lst_symbols_to_buy)


    def trade(self):
        print("[Crag.trade]")
        self.cash = self.broker.get_cash()
        trades = []

        # sell symbols
        lst_symbols = [current_trade.symbol for current_trade in self.current_trades if current_trade.type == "BUY"]
        lst_symbols = list(set(lst_symbols))
        df_selling_symbols = self.rtstr.get_df_selling_symbols(lst_symbols)
        for current_trade in self.current_trades:
            if current_trade.type == "BUY" and df_selling_symbols[symbol]["stimulus"] != "HOLD":
                sell_trade = trade.Trade()
                sell_trade.type = "SELL"
                sell_trade.sell_id = current_trade.id
                sell_trade.buying_price = current_trade.buying_price
                sell_trade.buying_time = current_trade.time
                sell_trade.symbol = current_trade.symbol
                sell_trade.symbol_price = float(self.rtstr.get_price_symbol(sell_trade.symbol))
                sell_trade.size = current_trade.size
                sell_trade.net_price = sell_trade.size * sell_trade.symbol_price
                sell_trade.buying_fee = current_trade.buying_fee
                sell_trade.selling_fee = sell_trade.net_price * self.broker.get_commission(sell_trade.symbol)
                sell_trade.gross_price = sell_trade.net_price + sell_trade.buying_fee + sell_trade.selling_fee
                sell_trade.roi = (sell_trade.gross_price - sell_trade.net_price) / sell_trade.net_price

                done = self.broker.execute_trade(sell_trade)
                if done:
                    current_trade.type = "SOLD"
                    self.cash = self.broker.get_cash()
                    sell_trade.cash = self.cash

                    self.portfolio_value = self.portfolio_value - sell_trade.net_price
                    self.wallet_value = self.portfolio_value + self.cash

                    sell_trade.portfolio_value = self.portfolio_value
                    sell_trade.wallet_value = self.wallet_value

                    trades.append(sell_trade)
                    self.current_trades.append(sell_trade)
                    print("{} ({}) {} {:.2f} roi={:.2f}".format(sell_trade.type, sell_trade.stimulus, sell_trade.symbol, sell_trade.gross_price, sell_trade.roi))

        # buy symbols
        df_buying_symbols = self.rtstr.get_df_buying_symbols()
        for symbol in df_buying_symbols.index.to_list():
            current_trade = trade.Trade()
            current_trade.type = "BUY"
            current_trade.sell_id = ""
            current_trade.stimulus = ""
            current_trade.roi = ""
            current_trade.buying_time = ""
            current_trade.selling_fee = ""
            current_trade.symbol = symbol
            current_trade.symbol_price = self.rtstr.rtctrl.df_rtctrl_symbol_price['price'][symbol]
            current_trade.symbol_price = float(current_trade.symbol_price)
            current_trade.buying_price = current_trade.symbol_price
            current_trade.size = df_buying_symbols[symbol]["size"]
            current_trade.net_price = current_trade.size * current_trade.symbol_price
            current_trade.buying_fee = current_trade.net_price * self.broker.get_commission(current_trade.symbol)
            current_trade.gross_price = current_trade.net_price + current_trade.buying_fee
            current_trade.profit_loss = -current_trade.buying_fee
            #current_trade.dump()
            if current_trade.gross_price <= self.cash:
                done = self.broker.execute_trade(current_trade)
                if done:
                    self.cash = self.broker.get_cash()
                    current_trade.cash = self.cash
                    self.portfolio_value = self.portfolio_value + current_trade.net_price
                    self.wallet_value = self.portfolio_value + self.cash
                    current_trade.portfolio_value = self.portfolio_value
                    current_trade.wallet_value = self.wallet_value

                    trades.append(current_trade)
                    self.current_trades.append(current_trade)

                    print("{} {} {:.2f}".format(current_trade.type, current_trade.symbol, current_trade.gross_price))

        self.add_to_log("trades", trades)

    def export_status(self):
        return self.broker.export_status()

    def get_trade_asset_size(self, symbol_price):
        if self.static_size:
            return self.size / symbol_price
        else:
            return (self.cash / 100) / symbol_price



