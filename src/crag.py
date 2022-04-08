import time
import pandas as pd
from datetime import datetime

class Trade:
    id = 0
    def __init__(self):
        self.id = Trade.id
        Trade.id = Trade.id + 1
        self.time = datetime.now()
    
    def dump(self):
        print("{} : {} for {}".format(self.id, self.symbol, self.net_price))


class Crag:
    def __init__(self, rtdp, params = None):
        self.rtdp = rtdp
        self.cash = 100

        self.log = {}
        self.current_step = -1

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

        #
        self.manage_current_data()

        #
        self.trade()

        return True

    def add_to_log(self, key, value):
        if self.current_step not in self.log:
            self.log[self.current_step] = {}
        self.log[self.current_step][key] = value

    def dump_log(self):
        for key, value in self.log.items():
            print(key)
            if "lst_symbols_to_buy" in value:
                print(value["lst_symbols_to_buy"])
            if "trades" in value:
                for trade in value["trades"]:
                    trade.dump()


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

        # create trades
        trades = []
        for symbol in self.log[self.current_step]["lst_symbols_to_buy"]:
            trade = Trade()
            trade.symbol = symbol
            trade.symbol_price = self.rtdp.current_data["symbols"][symbol.replace('/', '_')]["info"]["info"]["price"]
            trade.symbol_price = float(trade.symbol_price)
            trade.size = self.cash/100
            trade.net_price = trade.size * trade.symbol_price
            trade.commission = trade.net_price * 0.04
            trade.gross_price = trade.net_price + trade.commission
            trade.profit_loss = -trade.commission
            trades.append(trade)
        self.add_to_log("trades", trades)

