import time
import pandas as pd
from . import trade

class Crag:
    def __init__(self, params = None):

        self.rtdp = None
        self.broker = None
        if params:
            self.rtdp = params.get("rtdp", self.rtdp)
            self.broker = params.get("broker", self.broker)

        self.log = {}
        self.current_step = -1

        self.current_trades = []

        self.stop_loss = -1  # %
        self.get_profit = .05   # %

    def run(self, interval=1):
        done = False
        while not done:
            # log
            self.current_step = self.current_step + 1

            done = not self.step()
            time.sleep(interval)
        
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
        if self.current_step not in self.log:
            self.log[self.current_step] = {}
        self.log[self.current_step][key] = value

    def export_history(self, target=None):
        self.broker.export_history(target)

    def manage_current_data(self):
        print("[Crag.manage_current_data]")

        current_data = self.rtdp.get_current_data()

        df_portfolio = pd.read_json(current_data["portfolio"])

        # remove unused columns
        unused_columns = [column for column in df_portfolio.columns if column.startswith('RECOMMENDATION_')]
        unused_columns.extend(["rank_change1h", "rank_change24h"])
        df_portfolio.drop(unused_columns, axis=1, inplace=True)

        # sum all the buy, neutral & sell values
        for action in ["buy", "neutral", "sell"]:
            columns = [column for column in df_portfolio.columns if column.startswith(action+"_")]
            df_portfolio["sum_"+action]=df_portfolio.loc[:,columns].sum(axis=1)
        
        # compute a score based on TDView and 24h & 1h trends
        df_portfolio['score'] = df_portfolio['sum_buy'] + 1*df_portfolio['change24h'] + 1*df_portfolio['change1h'] - 2 * df_portfolio['sum_sell'] - df_portfolio['sum_neutral']
        df_portfolio.sort_values(by=['score'], ascending=False, inplace=True)

        # get rid of lower scores
        df_portfolio.drop(df_portfolio[df_portfolio.score <= 0].index, inplace=True)

        lst_symbols_to_buy = df_portfolio['symbol'].tolist()
        #print(lst_symbols_to_buy)

        # log
        self.add_to_log("lst_symbols_to_buy", lst_symbols_to_buy)


    def trade(self):
        print("[Crag.trade]")

        cash = self.broker.get_cash()
        trades = []

        # sell stuffs
        for current_trade in self.current_trades:
            if current_trade.type != "BUY":
                continue

            sell_trade = trade.Trade()
            sell_trade.type = "SELL"
            sell_trade.stimulus = ""
            sell_trade.symbol = current_trade.symbol
            key = sell_trade.symbol.replace('/', '_')
            if key not in self.rtdp.current_data["symbols"]:
                print("[Crag::trade] symbol {} not found".format(key))
                continue
            sell_trade.symbol_price = float(self.rtdp.current_data["symbols"][key]["info"]["info"]["price"])
            sell_trade.size = current_trade.size
            sell_trade.net_price = sell_trade.size * sell_trade.symbol_price
            sell_trade.commission = sell_trade.net_price * self.broker.get_commission(sell_trade.symbol)
            sell_trade.gross_price = sell_trade.net_price + sell_trade.commission

            roi = sell_trade.net_price - current_trade.gross_price
            if (roi >= 0 and 100.0 * roi / sell_trade.gross_price >= self.get_profit):
                sell_trade.stimulus = "GET_PROFIT"
            if (roi < 0 and 100.0 * roi / sell_trade.gross_price <= self.stop_loss):
                sell_trade.stimulus = "STOP_LOSS"

            if sell_trade.stimulus != "":
                done = self.broker.execute_trade(sell_trade)
                if done:
                    current_trade.type = "SOLD_FOR_"+sell_trade.stimulus
                    cash = cash + sell_trade.gross_price
                    trades.append(sell_trade)
                    self.current_trades.append(sell_trade)
                    print("{} ({}) {} {:.2f} roi={:.2f}".format(sell_trade.type, sell_trade.stimulus, sell_trade.symbol, sell_trade.gross_price, roi))

        # buy stuffs
        for symbol in self.log[self.current_step]["lst_symbols_to_buy"]:
            current_trade = trade.Trade()
            current_trade.type = "BUY"
            current_trade.stimulus = ""
            current_trade.symbol = symbol
            current_trade.symbol_price = self.rtdp.current_data["symbols"][symbol.replace('/', '_')]["info"]["info"]["price"]
            current_trade.symbol_price = float(current_trade.symbol_price)
            current_trade.size = (cash / 100) / current_trade.symbol_price
            current_trade.net_price = current_trade.size * current_trade.symbol_price
            current_trade.commission = current_trade.net_price * self.broker.get_commission(current_trade.symbol)
            current_trade.gross_price = current_trade.net_price + current_trade.commission
            current_trade.profit_loss = -current_trade.commission

            if current_trade.gross_price <= cash:
                done = self.broker.execute_trade(current_trade)
                if done:
                    cash = cash - current_trade.gross_price
                    trades.append(current_trade)
                    self.current_trades.append(current_trade)
                    print("{} {} {:.2f}".format(current_trade.type, current_trade.symbol, current_trade.gross_price))

        self.add_to_log("trades", trades)

    def export_status(self):
        return self.broker.export_status()


