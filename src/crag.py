import time
import pandas as pd
from . import trade
from . import rtctrl

class Crag:
    def __init__(self, params = None):

        self.rtdp = None
        self.broker = None
        self.rtstr = None
        if params:
            self.rtdp = params.get("rtdp", self.rtdp)
            self.broker = params.get("broker", self.broker)
            self.rtstr = params.get("rtstr", self.rtstr)

        self.rtctrl = rtctrl.rtctrl()

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

        # get current data
        df_current_data = self.rtdp.next()
        if df_current_data is None:
            return False

        # manage current data
        self.manage_current_data()

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
        lst_symbols_to_buy = self.rtstr.get_crypto_buying_list(current_data, self.rtctrl.df_rtctrl.copy())

        # log
        self.add_to_log("lst_symbols_to_buy", lst_symbols_to_buy)


    def trade(self):
        print("[Crag.trade]")
        self.cash = self.broker.get_cash()
        trades = []

        # sell stuffs
        for current_trade in self.current_trades:
            if current_trade.type != "BUY":
                continue

            sell_trade = trade.Trade()
            sell_trade.type = "SELL"
            sell_trade.sell_id = current_trade.id
            sell_trade.buying_price = current_trade.buying_price
            sell_trade.buying_time = current_trade.time
            sell_trade.stimulus = ""
            sell_trade.symbol = current_trade.symbol
            key = sell_trade.symbol.replace('/', '_')
            if key not in self.rtdp.current_data["symbols"]:
                print("[Crag::trade] symbol {} not found".format(key))
                continue
            sell_trade.symbol_price = float(self.rtdp.current_data["symbols"][key]["info"]["info"]["price"])
            sell_trade.size = current_trade.size
            sell_trade.net_price = sell_trade.size * sell_trade.symbol_price
            sell_trade.buying_fee = current_trade.buying_fee
            sell_trade.selling_fee = sell_trade.net_price * self.broker.get_commission(sell_trade.symbol)

            sell_trade.gross_price = sell_trade.net_price + sell_trade.buying_fee + sell_trade.selling_fee
            # sell_trade.gross_price = sell_trade.net_price + sell_trade.buying_fee

            sell_trade = self.rtstr.get_crypto_selling_list(current_trade, sell_trade, self.rtctrl.df_rtctrl.copy())

            if sell_trade.stimulus != "":
                done = self.broker.execute_trade(sell_trade)
                if done:
                    current_trade.type = "SOLD"
                    sell_trade.stimulus = "SOLD_FOR_"+sell_trade.stimulus
                    self.cash = self.broker.get_cash()
                    # self.cash = cash + sell_trade.net_price    # CEDE to be verified
                    sell_trade.cash = self.cash

                    self.portfolio_value = self.portfolio_value - sell_trade.net_price
                    self.wallet_value = self.portfolio_value + self.cash

                    sell_trade.portfolio_value = self.portfolio_value
                    sell_trade.wallet_value = self.wallet_value

                    trades.append(sell_trade)
                    self.current_trades.append(sell_trade)
                    print("{} ({}) {} {:.2f} roi={:.2f}".format(sell_trade.type, sell_trade.stimulus, sell_trade.symbol, sell_trade.gross_price, sell_trade.roi))

        # buy stuffs
        for symbol in self.log[self.current_step]["lst_symbols_to_buy"]:
            current_trade = trade.Trade()
            current_trade.type = "BUY"
            current_trade.sell_id = ""
            current_trade.stimulus = ""
            current_trade.roi = ""
            current_trade.buying_time = ""
            current_trade.selling_fee = ""
            current_trade.symbol = symbol
            current_trade.symbol_price = self.rtdp.current_data["symbols"][symbol.replace('/', '_')]["info"]["info"]["price"]
            current_trade.symbol_price = float(current_trade.symbol_price)
            current_trade.buying_price = current_trade.symbol_price
            current_trade.size = self.get_trade_asset_size(current_trade.symbol_price)
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

        self.rtctrl.update_rtctrl(self.current_trades, self.broker.get_cash())
        self.rtctrl.display_summary_info()
        self.add_to_log("trades", trades)

    def export_status(self):
        return self.broker.export_status()

    def get_trade_asset_size(self, symbol_price):
        if self.static_size:
            return self.size / symbol_price
        else:
            return (self.cash / 100) / symbol_price



