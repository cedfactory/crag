import os
import time
import pandas as pd
from . import trade,rtstr

class Crag:
    def __init__(self, params = None):

        self.broker = None
        self.rtstr = None
        self.working_directory = None
        self.interval = 1
        self.logger = None
        self.clear_unused_data = True
        self.original_portfolio_value = 0
        if params:
            self.broker = params.get("broker", self.broker)
            self.original_portfolio_value = self.broker.get_portfolio_value()
            self.rtstr = params.get("rtstr", self.rtstr)
            self.interval = params.get("interval", self.interval)
            self.logger = params.get("logger", self.logger)
            self.working_directory = params.get("working_directory", self.working_directory)

        self.current_trades = []

        self.cash = 0
        self.init_cash_value = 0
        self.df_portfolio_status = pd.DataFrame(columns=['symbol', 'portfolio_size', 'value', 'buying_value', 'roi_sl_tp'])
        self.portfolio_value = 0
        self.wallet_value = 0

        self.zero_print = True

        if self.rtstr != None:
            self.strategy_name, self.str_sl, self.str_tp = self.rtstr.get_info()
        if self.broker != None:
            self.final_datetime = self.broker.get_final_datetime()
            self.start_date, self.end_date,  self.inteval = self.broker.get_info()
        self.export_filename = "sim_broker_history"\
                               + "_" + self.strategy_name\
                               + "_" + str(self.start_date)\
                               + "_" + str(self.end_date)\
                               + "_" + str(self.inteval)\
                               + "_" + str(self.str_sl)\
                               + "_" + str(self.str_tp)\
                               + ".csv"
        if self.working_directory != None:
            self.export_filename = os.path.join(self.working_directory, self.export_filename)

    def log(self, msg, header="", attachments=[]):
        if self.logger:
            self.logger.log(msg, header=header, author=type(self).__name__, attachments=attachments)


    def run(self, queue=None):
        msg_broker_info = "{}\nCash : {}".format(type(self.broker).__name__, self.broker.get_cash())
        msh_strategy_info = "Running with {}".format(type(self.rtstr).__name__)
        msg = msg_broker_info + "\n" + msh_strategy_info
        self.log(msg, "run")
        done = False
        while not done:
            done = not self.step()
            if done:
                break
            start = time.time()
            while True:
                if queue:
                    data = queue.get()
                    print(data)
                    if data == "stop":
                        self.export_history(self.export_filename)
                        self.log(msg="> {}".format(self.export_filename), header="stopping", attachments=[self.export_filename])
                        return
                end = time.time()
                if end - start > self.interval:
                    break
            self.broker.tick() # increment

        self.export_history(self.export_filename)

    def step(self):
        msg = "original portfolio value = {:.2f}\n".format(self.original_portfolio_value)
        msg += "current portfolio value = {:.2f}\n".format(self.broker.get_portfolio_value())
        msg += "cash = {:.2f}".format(self.broker.get_cash())
        self.log(msg, "step")
        if not self.zero_print:
            print("[Crag] ⌛")

        # update all the data
        # TODO : this call should be done once, during the initialization of the system
        ds = self.rtstr.get_data_description()
        if self.clear_unused_data:
            self.broker.check_data_description(ds)
            self.clear_unused_data = False

        prices_symbols = {symbol:self.broker.get_value(symbol) for symbol in ds.symbols}
        current_datetime = self.broker.get_current_datetime()
        self.rtstr.update(current_datetime, self.current_trades, self.broker.get_cash(), prices_symbols, False, self.final_datetime)

        if(len(self.df_portfolio_status) == 0):
            self.df_portfolio_status = pd.DataFrame({"symbol":ds.symbols, "portfolio_size":0, "value":0, "buying_value":0, "roi_sl_tp":0})
            self.df_portfolio_status.set_index('symbol', drop=True, inplace=True)

        current_data = self.broker.get_current_data(ds)
        if current_data is None:
            if not self.zero_print:
                print("[Crag] 💥 no current data")
            # self.force_sell_open_trade()
            self.rtstr.update(current_datetime, self.current_trades, self.broker.get_cash(), prices_symbols, True, self.final_datetime)
            return False

        self.rtstr.set_current_data(current_data)

        # execute trading
        self.trade()

        return True

    def export_history(self, target=None):
        self.broker.export_history(target)

    def _prepare_sell_trade_from_bought_trade(self, bought_trade, current_datetime, df_selling_symbols):
        sell_trade = trade.Trade(current_datetime)
        sell_trade.type = "SELL"
        sell_trade.sell_id = bought_trade.id
        sell_trade.buying_price = bought_trade.buying_price
        sell_trade.buying_time = bought_trade.time
        sell_trade.stimulus = df_selling_symbols["stimulus"][bought_trade.symbol]
        sell_trade.symbol = bought_trade.symbol
        sell_trade.symbol_price = self.broker.get_value(bought_trade.symbol)
        sell_trade.gross_size = bought_trade.net_size         # Sell Net_size = bought_trade.Gross size
        sell_trade.gross_price = sell_trade.gross_size * sell_trade.symbol_price
        sell_trade.net_price = sell_trade.gross_price - sell_trade.gross_price * self.broker.get_commission(bought_trade.symbol)
        sell_trade.net_size = round(sell_trade.net_price / sell_trade.symbol_price, 8)
        sell_trade.buying_fee = bought_trade.buying_fee
        sell_trade.selling_fee = sell_trade.gross_price - sell_trade.net_price
        sell_trade.roi = 100 * (sell_trade.net_price - bought_trade.gross_price) / bought_trade.gross_price
        return sell_trade

    def trade(self):
        if not self.zero_print:
            print("[Crag.trade]")
        if self.cash == 0 and self.init_cash_value == 0:
            self.init_cash_value = self.broker.get_cash()
        self.cash = self.broker.get_cash()
        self.portfolio_value = self.rtstr.get_portfolio_value()
        current_datetime = self.broker.get_current_datetime()

        trades = []

        # sell symbols
        lst_symbols = [current_trade.symbol for current_trade in self.current_trades if current_trade.type == "BUY"]
        lst_symbols = list(set(lst_symbols))
        self.update_df_roi_sl_tp(lst_symbols)
        if current_datetime == self.final_datetime:
            # final step - force all the symbols to be sold
            df_selling_symbols = rtstr.RealTimeStrategy.get_df_forced_selling_symbols(lst_symbols)
        else:
            # identify symbols to sell
            df_selling_symbols = self.rtstr.get_df_selling_symbols(lst_symbols, self.df_portfolio_status)
        list_symbols_to_sell = df_selling_symbols.symbol.to_list()

        df_selling_symbols.set_index("symbol", inplace=True)

        for current_trade in self.current_trades:
            if current_trade.type == "BUY" and current_trade.symbol in list_symbols_to_sell and df_selling_symbols["stimulus"][current_trade.symbol] != "HOLD":
                sell_trade = self._prepare_sell_trade_from_bought_trade(current_trade, current_datetime, df_selling_symbols)
                done = self.broker.execute_trade(sell_trade)
                if done:
                    current_trade.type = "SOLD"
                    self.cash = self.broker.get_cash()
                    sell_trade.cash = self.cash

                    # Portfolio Size/Value Update

                    self.df_portfolio_status.at[sell_trade.symbol, 'portfolio_size'] = self.df_portfolio_status.at[sell_trade.symbol, 'portfolio_size'] - sell_trade.gross_size
                    if self.df_portfolio_status.at[sell_trade.symbol, 'portfolio_size'] < 0.0000001:
                        self.df_portfolio_status.at[sell_trade.symbol, 'portfolio_size'] = 0
                    self.df_portfolio_status.at[sell_trade.symbol, 'value'] = self.df_portfolio_status.at[sell_trade.symbol, 'portfolio_size'] * sell_trade.symbol_price
                    self.df_portfolio_status.at[sell_trade.symbol, 'buying_value'] = self.df_portfolio_status.at[sell_trade.symbol, 'buying_value'] - current_trade.gross_price
                    if self.df_portfolio_status.at[sell_trade.symbol, 'portfolio_size'] == 0:
                        self.df_portfolio_status.at[sell_trade.symbol, 'roi_sl_tp'] = 0
                    else:
                        self.df_portfolio_status.at[sell_trade.symbol, 'roi_sl_tp'] = 100 * (self.df_portfolio_status.at[sell_trade.symbol, 'value'] / self.df_portfolio_status.at[sell_trade.symbol, 'buying_value'] - 1)

                    self.portfolio_value = self.df_portfolio_status['value'].sum()

                    self.wallet_value = self.portfolio_value + self.cash

                    sell_trade.portfolio_value = self.portfolio_value
                    sell_trade.wallet_value = self.wallet_value
                    sell_trade.wallet_roi = (self.wallet_value - self.init_cash_value) * 100 / self.init_cash_value

                    trades.append(sell_trade)
                    self.current_trades.append(sell_trade)
                    
                    msg = "{} ({}) {} {:.2f} roi={:.2f}".format(sell_trade.type, sell_trade.stimulus, sell_trade.symbol, sell_trade.gross_price, sell_trade.roi)
                    self.log(msg, "symbol sold")

        # buy symbols
        df_buying_symbols = self.rtstr.get_df_buying_symbols()
        df_buying_symbols.set_index('symbol', inplace=True)
        df_buying_symbols.drop(df_buying_symbols[df_buying_symbols['size'] == 0].index, inplace=True)
        if current_datetime == self.final_datetime:
            df_buying_symbols.drop(df_buying_symbols.index, inplace=True)
        symbols_bought = {"symbol":[], "size":[], "percent":[], "gross_price":[]}
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
            current_trade.gross_size = df_buying_symbols["size"][symbol]  # Gross size
            current_trade.gross_price = current_trade.gross_size * current_trade.buying_price

            current_trade.net_price = current_trade.gross_price * (1 - current_trade.commission)
            current_trade.net_size = round(current_trade.net_price / current_trade.buying_price, 8)

            current_trade.buying_fee = current_trade.gross_price - current_trade.net_price
            current_trade.profit_loss = -current_trade.buying_fee

            if current_trade.gross_price <= self.cash:
                done = self.broker.execute_trade(current_trade)
                if done:
                    self.cash = self.broker.get_cash()
                    current_trade.cash = self.cash

                    # Portfolio Size/Value Update
                    self.df_portfolio_status.at[symbol, 'portfolio_size'] = self.df_portfolio_status.at[symbol, 'portfolio_size'] + current_trade.net_size
                    self.df_portfolio_status.at[symbol, 'value'] = self.df_portfolio_status.at[symbol, 'value'] + current_trade.net_price
                    self.df_portfolio_status.at[symbol, 'buying_value'] = self.df_portfolio_status.at[symbol, 'buying_value'] + current_trade.gross_price
                    self.df_portfolio_status.at[symbol, 'roi_sl_tp'] = 100 * (self.df_portfolio_status.at[symbol, 'value'] / self.df_portfolio_status.at[symbol, 'buying_value'] - 1)

                    self.portfolio_value = self.df_portfolio_status['value'].sum()

                    self.wallet_value = self.portfolio_value + self.cash
                    current_trade.portfolio_value = self.portfolio_value
                    current_trade.wallet_value = self.wallet_value
                    current_trade.wallet_roi = (self.wallet_value - self.init_cash_value) * 100 / self.init_cash_value

                    trades.append(current_trade)
                    self.current_trades.append(current_trade)

                    symbols_bought["symbol"].append(current_trade.symbol)
                    symbols_bought["size"].append(current_trade.gross_size)
                    symbols_bought["percent"].append(df_buying_symbols["percent"][current_trade.symbol])
                    symbols_bought["gross_price"].append(current_trade.gross_price)
        df_symbols_bought = pd.DataFrame(symbols_bought)
        self.log(df_symbols_bought, "symbols bought")

        # Clear the current_trades for optimization
        lst_buy_trades = []
        for current_trade in self.current_trades:
            if current_trade.type == "BUY":
                lst_buy_trades.append(current_trade)
        self.log("current cash {}".format(self.broker.get_cash()), "trade")

    def export_status(self):
        return self.broker.export_status()

    def update_df_roi_sl_tp(self, lst_symbols):
        for symbol in lst_symbols:
            symbol_price = self.broker.get_value(symbol)
            self.df_portfolio_status.at[symbol, 'value'] = self.df_portfolio_status.at[symbol, 'portfolio_size'] * symbol_price
            if self.df_portfolio_status.at[symbol, 'portfolio_size'] == 0 \
                    or self.df_portfolio_status.at[symbol, 'buying_value'] == 0 \
                    or self.df_portfolio_status.at[symbol, 'value'] == 0:
                self.df_portfolio_status.at[symbol, 'roi_sl_tp'] = 0
            else:
                self.df_portfolio_status.at[symbol, 'roi_sl_tp'] = 100 * (self.df_portfolio_status.at[symbol, 'value'] / self.df_portfolio_status.at[symbol, 'buying_value'] - 1)
