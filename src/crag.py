import time
import pandas as pd
from . import trade

class Crag:
    def __init__(self, params = None):

        self.broker = None
        self.rtstr = None
        self.scheduler = None
        if params:
            self.broker = params.get("broker", self.broker)
            self.rtstr = params.get("rtstr", self.rtstr)
            self.scheduler = params.get("chronos", self.scheduler)

        self.log = []
        self.current_step = self.scheduler.get_current_position()

        self.current_trades = []

        self.cash = 0
        self.init_cash_value = 0
        self.df_portfolio_status = pd.DataFrame(columns=['symbol', 'portfolio_size', 'value'])
        self.portfolio_value = 0
        self.wallet_value = 0

        self.static_size = True
        self.size = 1

    def run(self, interval=1):
        done = False
        while not done:
            done = not self.step()
            # time.sleep(interval)
            # self.export_history("broker_history.csv") # DEBUG
            self.export_history("sim_broker_history.csv") # DEBUG

            # log
            self.increment_position()
            self.log.append({})

    def step(self):
        print("⌛ [Crag]")

        # update all the data
        ds = self.rtstr.get_data_description()
        prices_symbols = {symbol:self.broker.get_value(symbol) for symbol in ds.symbols}
        current_datetime = self.broker.get_current_datetime()
        self.rtstr.update(current_datetime, self.current_trades, self.broker.get_cash(), prices_symbols)

        if(len(self.df_portfolio_status) == 0):
            self.df_portfolio_status['symbol'] = ds.symbols
            self.df_portfolio_status['portfolio_size'] = 0
            self.df_portfolio_status['value'] = 0
            self.df_portfolio_status.set_index('symbol', drop=True, inplace=True)

        current_data = self.broker.next(ds)
        # print(current_data) # DEBUG
        if current_data is None:
            self.force_sell_open_trade()
            self.rtstr.update(current_datetime, self.current_trades, self.broker.get_cash(), prices_symbols)
            return False

        self.rtstr.set_current_data(current_data)

        # execute trading
        self.trade()

        return True

    def add_to_log(self, key, value):
        # To be fixed - current_step = 400 not compatible with chronos implementation
        # self.log[self.current_step][key] = value
        pass

    def export_history(self, target=None):
        self.broker.export_history(target)

    def trade(self):
        print("[Crag.trade]")
        if self.cash == 0 and self.init_cash_value == 0:
            self.init_cash_value = self.broker.get_cash()
        self.cash = self.broker.get_cash()
        self.portfolio_value = self.rtstr.get_portfolio_value()
        current_datetime = self.broker.get_current_datetime()
        # current_datetime = self.scheduler.get_current_time()
        trades = []

        # sell symbols
        lst_symbols = [current_trade.symbol for current_trade in self.current_trades if current_trade.type == "BUY"]
        lst_symbols = list(set(lst_symbols))
        df_selling_symbols = self.rtstr.get_df_selling_symbols(lst_symbols)
        # print("👎") # DEBUG
        # print(df_selling_symbols) # DEBUG
        list_symbols_to_sell = df_selling_symbols.symbol.to_list()
        df_selling_symbols.set_index("symbol", inplace=True)
        for current_trade in self.current_trades:
            if current_trade.type == "BUY" and current_trade.symbol in list_symbols_to_sell and df_selling_symbols["stimulus"][current_trade.symbol] != "HOLD":
                sell_trade = trade.Trade(current_datetime)
                sell_trade.type = "SELL"
                sell_trade.sell_id = current_trade.id
                sell_trade.buying_price = current_trade.buying_price
                sell_trade.buying_time = current_trade.time
                sell_trade.stimulus = df_selling_symbols["stimulus"][current_trade.symbol]
                sell_trade.symbol = current_trade.symbol
                sell_trade.symbol_price = self.broker.get_value(current_trade.symbol)

                sell_trade.size = current_trade.size # Gross size
                sell_trade.gross_price = sell_trade.size * sell_trade.symbol_price

                sell_trade.size = current_trade.size - self.broker.get_commission(sell_trade.symbol)  # Net size
                sell_trade.net_price = sell_trade.size * sell_trade.symbol_price

                sell_trade.buying_fee = current_trade.buying_fee

                sell_trade.selling_fee = sell_trade.gross_price - sell_trade.net_price
                sell_trade.gross_price = sell_trade.gross_price + sell_trade.buying_fee

                sell_trade.roi = (sell_trade.net_price - sell_trade.gross_price) / current_trade.net_price

                done = self.broker.execute_trade(sell_trade)
                if done:
                    current_trade.type = "SOLD"
                    self.cash = self.broker.get_cash()
                    sell_trade.cash = self.cash

                    # Portfolio Size/Value Update
                    self.df_portfolio_status['portfolio_size'][sell_trade.symbol] = self.df_portfolio_status['portfolio_size'][sell_trade.symbol] - current_trade.size
                    self.df_portfolio_status['value'][sell_trade.symbol] = self.df_portfolio_status['portfolio_size'][sell_trade.symbol] * sell_trade.symbol_price
                    self.portfolio_value = self.df_portfolio_status['value'].sum()

                    self.wallet_value = self.portfolio_value + self.cash

                    sell_trade.portfolio_value = self.portfolio_value
                    sell_trade.wallet_value = self.wallet_value
                    sell_trade.wallet_roi = (self.wallet_value - self.init_cash_value) * 100 / self.init_cash_value

                    trades.append(sell_trade)
                    self.current_trades.append(sell_trade)
                    print("{} ({}) {} {:.2f} roi={:.2f}".format(sell_trade.type, sell_trade.stimulus, sell_trade.symbol, sell_trade.gross_price, sell_trade.roi))

        # buy symbols
        df_buying_symbols = self.rtstr.get_df_buying_symbols()
        # print("👍") # DEBUG
        # print(df_buying_symbols) # DEBUG
        df_buying_symbols.set_index('symbol', inplace=True)
        df_buying_symbols.drop(df_buying_symbols[df_buying_symbols['size'] == 0].index, inplace=True)
        for symbol in df_buying_symbols.index.to_list():
            current_trade = trade.Trade(current_datetime)
            current_trade.type = "BUY"
            current_trade.sell_id = ""
            current_trade.stimulus = ""
            current_trade.roi = ""
            current_trade.buying_time = ""
            current_trade.selling_fee = ""
            current_trade.symbol = symbol

            current_trade.symbol_price = self.broker.get_value(symbol)
            current_trade.buying_price = current_trade.symbol_price

            current_trade.commission = self.broker.get_commission(current_trade.symbol)
            current_trade.size = df_buying_symbols["size"][symbol]  # Gross size
            current_trade.gross_price = current_trade.size * current_trade.buying_price
            current_trade.size = df_buying_symbols["size"][symbol] - current_trade.commission  # Net size
            current_trade.net_price = current_trade.size * current_trade.symbol_price
            current_trade.buying_fee = current_trade.gross_price - current_trade.net_price
            current_trade.profit_loss = -current_trade.buying_fee
            #current_trade.dump()
            if current_trade.gross_price <= self.cash:
                done = self.broker.execute_trade(current_trade)
                if done:
                    self.cash = self.broker.get_cash()
                    current_trade.cash = self.cash

                    # Portfolio Size/Value Update
                    self.df_portfolio_status['portfolio_size'][symbol] = self.df_portfolio_status['portfolio_size'][symbol] + current_trade.size
                    self.df_portfolio_status['value'][symbol] = self.df_portfolio_status['portfolio_size'][symbol] * current_trade.symbol_price
                    self.portfolio_value = self.df_portfolio_status['value'].sum()

                    self.wallet_value = self.portfolio_value + self.cash
                    current_trade.portfolio_value = self.portfolio_value
                    current_trade.wallet_value = self.wallet_value
                    current_trade.wallet_roi = (self.wallet_value - self.init_cash_value) * 100 / self.init_cash_value

                    trades.append(current_trade)
                    self.current_trades.append(current_trade)

                    print("{} {} {:.2f}".format(current_trade.type, current_trade.symbol, current_trade.gross_price))

        self.add_to_log("trades", trades)

    def export_status(self):
        return self.broker.export_status()

    def force_sell_open_trade(self):
        print("[Crag.forced.exit.trade]")
        if self.cash == 0 and self.init_cash_value == 0:
            self.init_cash_value = self.broker.get_cash()
        self.cash = self.broker.get_cash()
        self.portfolio_value = self.rtstr.get_portfolio_value()
        current_datetime = self.broker.get_current_datetime()
        trades = []

        # sell symbols
        lst_symbols = [current_trade.symbol for current_trade in self.current_trades if current_trade.type == "BUY"]
        lst_symbols = list(set(lst_symbols))
        df_selling_symbols = self.rtstr.get_df_forced_exit_selling_symbols(lst_symbols)
        print("👎")
        print('Selling remaining open positions before exit')
        print(df_selling_symbols)
        list_symbols_to_sell = df_selling_symbols.symbol.to_list()
        df_selling_symbols.set_index("symbol", inplace=True)
        for current_trade in self.current_trades:
            if current_trade.type == "BUY" and current_trade.symbol in list_symbols_to_sell and df_selling_symbols["stimulus"][current_trade.symbol] != "HOLD":
                sell_trade = trade.Trade(current_datetime)
                sell_trade.type = "SELL"
                sell_trade.sell_id = current_trade.id
                sell_trade.buying_price = current_trade.buying_price
                sell_trade.buying_time = current_trade.time
                sell_trade.stimulus = df_selling_symbols["stimulus"][current_trade.symbol]
                sell_trade.symbol = current_trade.symbol
                sell_trade.symbol_price = self.broker.get_value(current_trade.symbol)
                sell_trade.size = current_trade.size
                sell_trade.net_price = sell_trade.size * sell_trade.symbol_price
                sell_trade.buying_fee = current_trade.buying_fee
                sell_trade.selling_fee = sell_trade.net_price * self.broker.get_commission(sell_trade.symbol)
                sell_trade.gross_price = current_trade.net_price + sell_trade.buying_fee + sell_trade.selling_fee
                sell_trade.roi = (sell_trade.net_price - sell_trade.gross_price) / current_trade.net_price

                done = self.broker.execute_trade(sell_trade)
                if done:
                    current_trade.type = "SOLD"
                    self.cash = self.broker.get_cash()
                    sell_trade.cash = self.cash

                    # Portfolio Size/Value Update
                    self.df_portfolio_status['portfolio_size'][sell_trade.symbol] = self.df_portfolio_status['portfolio_size'][sell_trade.symbol] - sell_trade.size
                    self.df_portfolio_status['value'][sell_trade.symbol] = self.df_portfolio_status['portfolio_size'][sell_trade.symbol] * sell_trade.symbol_price
                    self.portfolio_value = self.df_portfolio_status['value'].sum()

                    self.wallet_value = self.portfolio_value + self.cash

                    sell_trade.portfolio_value = self.portfolio_value
                    sell_trade.wallet_value = self.wallet_value
                    sell_trade.wallet_roi = (self.wallet_value - self.init_cash_value) * 100 / self.init_cash_value

                    trades.append(sell_trade)
                    self.current_trades.append(sell_trade)
                    print("{} ({}) {} {:.2f} roi={:.2f}".format(sell_trade.type, sell_trade.stimulus, sell_trade.symbol, sell_trade.gross_price, sell_trade.roi))

        self.add_to_log("trades", trades)

    def increment_position(self):
        self.scheduler.increment_time()
        self.current_step = self.scheduler.get_current_position()