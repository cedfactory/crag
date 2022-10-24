import os
import time
import pandas as pd
from . import trade,rtstr,utils
import pika
import threading
import pickle

# to launch crag as a rabbitmq receiver :
# > apt-get install rabbitmq-server
# > systemctl enable rabbitmq-server
# > systemctl start rabbitmq-server
# systemctl is not provided by WSL :
# > service rabbitmq-server start
# reference : https://medium.com/analytics-vidhya/how-to-use-rabbitmq-with-python-e0ccfe7fa959
class Crag:
    def __init__(self, params = None):

        self.broker = None
        self.rtstr = None
        self.working_directory = None
        self.interval = 1
        self.logger = None
        self.clear_unused_data = True
        self.start_date = ""
        self.original_portfolio_value = 0
        self.minimal_portfolio_value = 0
        self.minimal_portfolio_date = ""
        self.maximal_portfolio_value = 0
        self.maximal_portfolio_date = ""
        self.id = ""
        if params:
            self.broker = params.get("broker", self.broker)
            self.original_portfolio_value = self.broker.get_portfolio_value()
            self.minimal_portfolio_value = self.original_portfolio_value
            self.maximal_portfolio_value = self.original_portfolio_value
            self.rtstr = params.get("rtstr", self.rtstr)
            self.interval = params.get("interval", self.interval)
            self.logger = params.get("logger", self.logger)
            self.working_directory = params.get("working_directory", self.working_directory)
            self.id = params.get("id", self.id)

        self.current_trades = []

        self.cash = 0
        self.init_cash_value = 0
        self.df_portfolio_status = pd.DataFrame(columns=['symbol', 'portfolio_size', 'value', 'buying_value', 'roi_sl_tp'])
        self.portfolio_value = 0
        self.wallet_value = 0

        self.zero_print = True
        self.flush_current_trade = False

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
        self.backup_filename = 'crag_backup.pickle'

        if self.working_directory != None:
            self.export_filename = os.path.join(self.working_directory, self.export_filename)
            self.backup_filename = os.path.join(self.working_directory, self.backup_filename)

        self.temp_debug = True
        if self.temp_debug:
            self.df_debug_traces = pd.DataFrame(columns=['time', 'cash_dol', ' coin_size', 'coin_dol', 'cash_pct', 'coin_pct', 'total_cash', 'total_pct'])


        # rabbitmq connection
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            channel = connection.channel()
            channel.queue_declare(queue='crag')

            def callback(ch, method, properties, bbody):
                print(" [x] Received {}".format(bbody))
                body = bbody.decode()
                if body == "history" or body == "stop":
                    self.export_history(self.export_filename)
                    self.log(msg="> {}".format(self.export_filename), header="{}".format(body), attachments=[self.export_filename])
                    os.remove(self.export_filename)
                    if body == "stop":
                        os._exit(0)
                elif body == "rtctrl":
                    rtctrl = self.rtstr.get_rtctrl()
                    if rtctrl:
                        summary = rtctrl.display_summary_info()
                        df_rtctrl = rtctrl.df_rtctrl
                        if isinstance(df_rtctrl, pd.DataFrame):
                            filename = str(utils.get_random_id())+"_rtctrl.csv"
                            df_rtctrl.to_csv(filename)
                            self.log(msg="> {}".format(filename), header="{}".format(body), attachments=[filename])
                            os.remove(filename)
                        else:
                            self.log("rtctrl is not a dataframe", header="{}".format(body))
                elif body == "rtctrl_summary":
                    rtctrl = self.rtstr.get_rtctrl()
                    if rtctrl:
                        summary = rtctrl.display_summary_info()
                        self.log(msg=summary, header="{}".format(body))
                else:
                    self.log(msg="> {} : unknown message".format(body))

            channel.basic_consume(queue='crag', on_message_callback=callback, auto_ack=True)
            
            #channel.start_consuming()
            thread = threading.Thread(name='t', target=channel.start_consuming, args=())
            thread.setDaemon(True)
            thread.start()
        except:
            print("Problem encountered while configuring the rabbitmq receiver")


    def log(self, msg, header="", attachments=[]):
        if self.logger:
            self.logger.log(msg, header="["+self.id+"] "+header, author=type(self).__name__, attachments=attachments)


    def run(self):
        self.start_date = self.broker.get_current_datetime("%Y/%m/%d %H:%M:%S")
        self.minimal_portfolio_date = self.start_date
        self.maximal_portfolio_date = self.start_date
        msg_broker_info = self.broker.log_info()
        msg_strategy_info = "Running with {}".format(type(self.rtstr).__name__)
        msg = msg_broker_info + "\n" + msg_strategy_info
        self.log(msg, "run")
        self.rtstr.log_info()
        done = False
        while not done:
            start = time.time()
            done = not self.step()
            if done:
                break
            end = time.time()
            sleeping_time = self.interval - (end - start)
            if sleeping_time >= 0:
                time.sleep(self.interval - (end - start))
            else:
                self.log("warning : time elapsed for the step ({}) is greater than the interval ({})".format(end - start, self.interval))

            self.broker.tick() # increment
            self.backup() # backup for reboot

        self.export_history(self.export_filename)

    def step(self):
        portfolio_value = self.broker.get_portfolio_value()
        current_date = self.broker.get_current_datetime("%Y/%m/%d %H:%M:%S")
        if portfolio_value < self.minimal_portfolio_value:
            self.minimal_portfolio_value = portfolio_value
            self.minimal_portfolio_date = current_date
        if portfolio_value > self.maximal_portfolio_value:
            self.maximal_portfolio_value = portfolio_value
            self.maximal_portfolio_date = current_date

        msg = "original portfolio value : $ {} ({})\n".format(utils.KeepNDecimals(self.original_portfolio_value, 2), self.start_date)
        variation_percent = utils.get_variation(self.original_portfolio_value, portfolio_value)
        msg += "current portfolio value : $ {} ({}%)\n".format(utils.KeepNDecimals(portfolio_value, 2), utils.KeepNDecimals(variation_percent, 2))
        msg += "    minimal portfolio value : $ {} ({})\n".format(utils.KeepNDecimals(self.minimal_portfolio_value, 2), self.minimal_portfolio_date)
        msg += "    maximal portfolio value : $ {} ({})\n".format(utils.KeepNDecimals(self.maximal_portfolio_value, 2), self.maximal_portfolio_date)
        msg += "current cash = {}".format(utils.KeepNDecimals(self.broker.get_cash(), 2))
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

        self.rtstr.log_current_info()

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

        # Clear one trade position partialy
        sell_trade.gross_size = df_selling_symbols['size'][sell_trade.symbol]
        # Clear one trade position totaly
        sell_trade.gross_size = bought_trade.net_size

        sell_trade.gross_price = sell_trade.gross_size * sell_trade.symbol_price
        sell_trade.net_price = sell_trade.gross_price - sell_trade.gross_price * self.broker.get_commission(bought_trade.symbol)
        sell_trade.net_size = round(sell_trade.net_price / sell_trade.symbol_price, 8)
        sell_trade.buying_fee = bought_trade.buying_fee
        sell_trade.selling_fee = sell_trade.gross_price - sell_trade.net_price
        sell_trade.roi = 100 * (sell_trade.net_price - bought_trade.gross_price) / bought_trade.gross_price

        # CEDE DEBUG TRACES
        if sell_trade.roi < 0:
            if (sell_trade.symbol_price - sell_trade.buying_price) < 0:
                print('NEGATIVE TRADE: $', sell_trade.net_price - bought_trade.gross_price)
            else:
                print('NEGATIVE TRADE DUE TO FEES: $', sell_trade.net_price - bought_trade.gross_price)
            print('BUYING AT: $', sell_trade.buying_price, ' SELLING AT: $', sell_trade.symbol_price)
        # CEDE TRACES FOR DEBUG
        # else:
        #     print('POSITIVE TRADE: $', sell_trade.net_price - bought_trade.gross_price)
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
        self.rtstr.reset_selling_limits()
        if self.final_datetime and current_datetime >= self.final_datetime:
            # final step - force all the symbols to be sold
            df_selling_symbols = rtstr.RealTimeStrategy.get_df_forced_selling_symbols(lst_symbols, self.rtstr.get_rtctrl().df_rtctrl)
            self.flush_current_trade = True
            self.rtstr.force_selling_limits()
        else:
            # identify symbols to sell
            df_selling_symbols = self.rtstr.get_df_selling_symbols(lst_symbols, self.df_portfolio_status)
            self.rtstr.set_selling_limits(df_selling_symbols)

        list_symbols_to_sell = df_selling_symbols.symbol.to_list()
        df_selling_symbols.set_index("symbol", inplace=True)

        for current_trade in self.current_trades:
            if current_trade.type == "BUY" and current_trade.symbol in list_symbols_to_sell \
                    and df_selling_symbols["stimulus"][current_trade.symbol] != "HOLD"\
                    and (self.flush_current_trade
                         or ((self.rtstr.get_selling_limit(current_trade.symbol))
                             and (current_trade.gridzone > self.rtstr.get_lower_zone_buy_engaged(current_trade.symbol)))):
                sell_trade = self._prepare_sell_trade_from_bought_trade(current_trade, current_datetime, df_selling_symbols)
                done = self.broker.execute_trade(sell_trade)
                if done:
                    self.rtstr.count_selling_limits(current_trade.symbol)
                    current_trade.type = "SOLD"
                    self.cash = self.broker.get_cash()
                    sell_trade.cash = self.cash

                    # Update grid strategy
                    self.rtstr.set_lower_zone_unengaged_position(current_trade.symbol, current_trade.gridzone)
                    sell_trade.gridzone = current_trade.gridzone

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
        symbols_bought = {"symbol":[], "size":[], "percent":[], "gross_price":[], "gridzone":[]}
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
            current_trade.gridzone = df_buying_symbols["gridzone"][symbol]

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

                    # Update grid strategy
                    self.rtstr.set_zone_engaged(current_trade.symbol, current_trade.symbol_price)

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
                    symbols_bought["size"].append(utils.KeepNDecimals(current_trade.gross_size))
                    symbols_bought["percent"].append(utils.KeepNDecimals(df_buying_symbols["percent"][current_trade.symbol]))
                    symbols_bought["gross_price"].append(utils.KeepNDecimals(current_trade.gross_price))
                    symbols_bought["gridzone"].append(df_buying_symbols["gridzone"][current_trade.symbol])
        df_symbols_bought = pd.DataFrame(symbols_bought)

        if not df_symbols_bought.empty:
            self.log(df_symbols_bought, "symbols bought")

        # Clear the current_trades for optimization
        lst_buy_trades = []
        for current_trade in self.current_trades:
            if current_trade.type == "BUY":
                lst_buy_trades.append(current_trade)

        # CEDE: Only for debug purpose
        if self.temp_debug:
            total = self.cash + self.df_portfolio_status['value'].sum()
            coin_size = self.df_portfolio_status['portfolio_size'].sum()
            print(current_datetime, ' cash : $', round(self.cash, 2), ' coin size : ', round(coin_size, 4), 'portfolio : $', round(self.df_portfolio_status['value'].sum(), 2), ' total : $', round(total))
            cash_percent = 100 * self.cash / total
            coin_percent = 100 * self.df_portfolio_status['value'].sum() / total
            print(current_datetime, ' cash% : %', round(cash_percent,2), ' portfolio : %', round(coin_percent,2), ' total : %', round(cash_percent + coin_percent, 2))

            # add row to end of DataFrame
            self.df_debug_traces.loc[len(self.df_debug_traces.index)] = [current_datetime,
                                                                         round(self.cash,2),
                                                                         round(coin_size, 4),
                                                                         round(self.df_portfolio_status['value'].sum(),2),
                                                                         round(cash_percent,2),
                                                                         round(coin_percent,2),
                                                                         round(total, 2),
                                                                         round(cash_percent + coin_percent, 2)]
            if current_datetime == self.final_datetime:
                self.export_debug_traces()


    def export_status(self):
        return self.broker.export_status()

    def update_df_roi_sl_tp(self, lst_symbols):
        for symbol in lst_symbols:
            symbol_price = self.broker.get_value(symbol)
            if symbol_price == None:
                continue
            self.df_portfolio_status.at[symbol, 'value'] = self.df_portfolio_status.at[symbol, 'portfolio_size'] * symbol_price
            if self.df_portfolio_status.at[symbol, 'portfolio_size'] == 0 \
                    or self.df_portfolio_status.at[symbol, 'buying_value'] == 0 \
                    or self.df_portfolio_status.at[symbol, 'value'] == 0:
                self.df_portfolio_status.at[symbol, 'roi_sl_tp'] = 0
            else:
                self.df_portfolio_status.at[symbol, 'roi_sl_tp'] = 100 * (self.df_portfolio_status.at[symbol, 'value'] / self.df_portfolio_status.at[symbol, 'buying_value'] - 1)

    def backup(self):
        with open(self.backup_filename, 'wb') as file:
            # print("[crah::backup]", self.backup_filename)
            pickle.dump(self, file)

    def export_debug_traces(self):
        if self.temp_debug:
            self.df_debug_traces.to_csv('debug_traces.csv')