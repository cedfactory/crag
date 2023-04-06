import os
import shutil
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
        self.exit = False
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
        self.id = str(utils.get_random_id())
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
            self.working_directory = params.get("working_directory", self.working_directory)

        self.current_trades = []

        self.cash = 0
        self.init_cash_value = 0
        self.portfolio_value = 0
        self.wallet_value = 0
        self.sell_performed = False

        self.zero_print = True
        self.flush_current_trade = False

        if self.rtstr != None:
            self.strategy_name = self.rtstr.get_info()
        if self.broker != None:
            self.final_datetime = self.broker.get_final_datetime()
            self.start_date, self.end_date,  self.inteval = self.broker.get_info()
        self.export_filename = "sim_broker_history"\
                               + "_" + self.strategy_name\
                               + "_" + str(self.start_date)\
                               + "_" + str(self.end_date)\
                               + "_" + str(self.inteval)\
                               + ".csv"
        self.backup_filename = self.id + "_crag_backup.pickle"

        if self.working_directory == None:
            self.working_directory = './output/' # CEDE NOTE: output directory name to be added to .xml / output as default value

        if not os.path.exists(self.working_directory):
            os.makedirs(self.working_directory)
        else:
            shutil.rmtree(self.working_directory)
            os.makedirs(self.working_directory)
        self.export_filename = os.path.join(self.working_directory, self.export_filename)
        self.backup_filename = os.path.join(self.working_directory, self.backup_filename)

        self.temp_debug = True
        if self.temp_debug:
            self.df_debug_traces = pd.DataFrame(columns=['time', 'cash_dol', ' coin_size', 'coin_dol', 'cash_pct', 'coin_pct', 'total_cash', 'total_pct'])

        self.traces_trade_total_opened = 0
        self.traces_trade_positive = 0
        self.traces_trade_negative = 0
        self.traces_trade_performed = 1 # find a better way to fix the incrementation of it

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
            if self.start_date and self.end_date:
                sleeping_time = 0
                # self.export_history(self.export_filename) # CEDE DEBUG
            else:
                sleeping_time = self.interval - (end - start)
                if sleeping_time >= 0:
                    time.sleep(self.interval - (end - start))
                else:
                    self.log("warning : time elapsed for the step ({}) is greater than the interval ({})".format(end - start, self.interval))

            self.broker.tick() # increment
            self.backup() # backup for reboot

        self.export_history(self.export_filename)

    def step(self):
        # portfolio_value = self.broker.get_portfolio_value()
        portfolio_value = self.broker.get_wallet_equity()
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
        portfolio_net_value = portfolio_value - (portfolio_value - self.broker.get_cash()) * 0.07 / 100 # CEDE 0.07 could be replaced by get_commission???
        variation_percent = utils.get_variation(self.original_portfolio_value, portfolio_net_value)
        msg += "current portfolio net value : $ {} ({}%)\n".format(utils.KeepNDecimals(portfolio_net_value, 2), utils.KeepNDecimals(variation_percent, 2))
        msg += "total opened positions : {} remaining open: {}\n".format(self.traces_trade_total_opened, self.traces_trade_total_opened - self.traces_trade_performed)
        win_rate = 100 * self.traces_trade_positive / self.traces_trade_performed
        msg += "win rate : {}% out of {} trades concluded\n".format(utils.KeepNDecimals(win_rate, 2), self.traces_trade_performed)
        variation_percent = utils.get_variation(self.minimal_portfolio_value, portfolio_value)
        msg += "max drawdown : $ {} ({}%) ({})\n".format(utils.KeepNDecimals(self.minimal_portfolio_value, 2), utils.KeepNDecimals(variation_percent, 2),self.minimal_portfolio_date)
        variation_percent = utils.get_variation(self.maximal_portfolio_value, portfolio_value)
        msg += "maximal portfolio value : $ {} ({}%) ({})\n".format(utils.KeepNDecimals(self.maximal_portfolio_value, 2), utils.KeepNDecimals(variation_percent, 2),self.maximal_portfolio_date)
        if self.rtstr.rtctrl.get_rtctrl_nb_symbols() > 0:
            msg += "symbols value roi:\n"
            list_symbols = self.rtstr.rtctrl.get_rtctrl_lst_symbols()
            list_value = self.rtstr.rtctrl.get_rtctrl_lst_values()
            list_roi = self.rtstr.rtctrl.get_rtctrl_lst_roi()
            for symbols in list_symbols:
                msg += "{} - {} - {}\n".format(list_symbols[0], utils.KeepNDecimals(list_value[0], 2), utils.KeepNDecimals(list_roi[0], 2))
                list_symbols.pop(0)
                list_value.pop(0)
                list_roi.pop(0)
        else:
            msg += "no positions\n"
        msg += "current cash = {}".format(utils.KeepNDecimals(self.broker.get_cash(), 2))
        self.log(msg, "step")
        if not self.zero_print:
            print("[Crag] ⌛")

        # update all the data
        # TODO : this call should be done once, during the initialization of the system
        ds = self.rtstr.get_data_description()
        ds.interval = self.interval # probably better to let the strategy provide this info

        if self.clear_unused_data:
            self.broker.check_data_description(ds)
            self.clear_unused_data = False

        prices_symbols = {symbol:self.broker.get_value(symbol) for symbol in ds.symbols}

        print('price: ', prices_symbols)
        current_datetime = self.broker.get_current_datetime()
        self.rtstr.update(current_datetime, self.current_trades, self.broker.get_cash(), self.broker.get_cash_borrowed(), prices_symbols, False, self.final_datetime, self.broker.get_balance())

        current_data = self.broker.get_current_data(ds)
        if current_data is None:
            if not self.zero_print:
                print("[Crag] 💥 no current data")
            # self.force_sell_open_trade()
            self.rtstr.update(current_datetime, self.current_trades, self.broker.get_cash(), prices_symbols, True, self.final_datetime, self.broker.get_balance())
            return False

        self.rtstr.set_current_data(current_data)

        # execute trading
        self.trade()

        self.rtstr.log_current_info()

        return not self.exit

    def export_history(self, target=None):
        self.broker.export_history(target)

    def _prepare_sell_trade_from_bought_trade(self, bought_trade, current_datetime, df_selling_symbols):
        sell_trade = trade.Trade(current_datetime)
        sell_trade.type = self.rtstr.get_close_type(bought_trade.symbol)
        sell_trade.sell_id = bought_trade.id
        sell_trade.buying_price = bought_trade.buying_price
        sell_trade.buying_time = bought_trade.time
        sell_trade.stimulus = df_selling_symbols["stimulus"][bought_trade.symbol]
        sell_trade.symbol = bought_trade.symbol
        sell_trade.symbol_price = self.broker.get_value(bought_trade.symbol)
        sell_trade.bought_gross_price = bought_trade.gross_price

        sell_trade.commission = self.broker.get_commission(sell_trade.symbol)
        sell_trade.minsize = self.broker.get_minimum_size(sell_trade.symbol)

        sell_trade.cash_borrowed = bought_trade.cash_borrowed

        # Clear one trade position partialy:
        # sell_trade.gross_size = df_selling_symbols['size'][sell_trade.symbol]
        # Clear one trade position totaly
        # Option chosen... so far...
        sell_trade.gross_size = bought_trade.net_size

        if sell_trade.type == self.rtstr.close_long:
            sell_trade.gross_price = sell_trade.gross_size * sell_trade.symbol_price
            # sell_trade.gross_price = sell_trade.gross_size * (sell_trade.symbol_price + sell_trade.symbol_price - sell_trade.buying_price)
        elif sell_trade.type == self.rtstr.close_short:
            sell_trade.gross_price = sell_trade.gross_size * sell_trade.symbol_price  # (-) to be added un broker_execute_trade
            # sell_trade.gross_price = sell_trade.gross_size * (sell_trade.buying_price + sell_trade.buying_price - sell_trade.symbol_price)
        elif sell_trade.type == self.rtstr.no_position: # CEDE WORKAROUND to be fixed
            sell_trade.type = self.rtstr.close_long # CEDE WORKAROUND grid trading
            sell_trade.gross_price = sell_trade.gross_size * sell_trade.symbol_price

        sell_trade.net_price = sell_trade.gross_price - sell_trade.gross_price * self.broker.get_commission(bought_trade.symbol)
        sell_trade.net_size = round(sell_trade.net_price / sell_trade.symbol_price, 8)
        sell_trade.buying_fee = bought_trade.buying_fee
        sell_trade.selling_fee = sell_trade.gross_price - sell_trade.net_price
        sell_trade.roi = 100 * (sell_trade.net_price - bought_trade.gross_price) / bought_trade.gross_price
        if sell_trade.type == self.rtstr.close_short:
            sell_trade.roi = -sell_trade.roi
        self.traces_trade_performed = self.traces_trade_performed + 1
        if sell_trade.roi < 0:
            self.traces_trade_negative = self.traces_trade_negative + 1
            '''
            # CEDE DEBUG:
            if (sell_trade.symbol_price - sell_trade.buying_price) < 0:
                print('NEGATIVE TRADE: $', sell_trade.net_price - bought_trade.gross_price)
            else:
                print('NEGATIVE TRADE DUE TO FEES: $', sell_trade.net_price - bought_trade.gross_price)
            print('BUYING AT: $', sell_trade.buying_price, ' SELLING AT: $', sell_trade.symbol_price)
            '''
        else:
            self.traces_trade_positive = self.traces_trade_positive + 1
        return sell_trade

    def trade(self):
        if not self.zero_print:
            print("[Crag.trade]")
        if self.cash == 0 and self.init_cash_value == 0:
            self.init_cash_value = self.broker.get_cash()
        self.cash = self.broker.get_cash()
        self.portfolio_value = self.rtstr.get_portfolio_value()
        current_datetime = self.broker.get_current_datetime()

        # sell symbols
        lst_symbols = [current_trade.symbol for current_trade in self.current_trades if self.rtstr.is_open_type(current_trade.type)]
        lst_symbols = list(set(lst_symbols))
        self.rtstr.reset_selling_limits()
        if (self.final_datetime and current_datetime >= self.final_datetime)\
                or self.rtstr.condition_for_global_sl_tp_signal():
            # final step - force all the symbols to be sold
            df_selling_symbols = self.rtstr.get_df_forced_selling_symbols()
            self.exit = True
            self.flush_current_trade = True
            self.rtstr.force_selling_limits()
        else:
            # identify symbols to sell
            df_selling_symbols = self.rtstr.get_df_selling_symbols(lst_symbols, self.rtstr.rtctrl.get_rtctrl_df_roi_sl_tp())
            self.rtstr.set_selling_limits(df_selling_symbols)

        list_symbols_to_sell = df_selling_symbols.symbol.to_list()
        df_selling_symbols.set_index("symbol", inplace=True)

        for current_trade in self.current_trades:
            if self.rtstr.is_open_type(current_trade.type) and current_trade.symbol in list_symbols_to_sell \
                    and (self.flush_current_trade
                         or ((self.rtstr.get_selling_limit(current_trade.symbol))
                             and (self.rtstr.get_grid_sell_condition(current_trade.symbol, current_trade.gridzone))
                             or self.rtstr.grid_exit_range_trend_down(current_trade.symbol))):
                sell_trade = self._prepare_sell_trade_from_bought_trade(current_trade, current_datetime, df_selling_symbols)
                done = self.broker.execute_trade(sell_trade)
                if done:
                    self.sell_performed = True
                    self.rtstr.count_selling_limits(current_trade.symbol)
                    current_trade.type = self.rtstr.get_close_type_and_close(current_trade.symbol)
                    self.cash = self.broker.get_cash()
                    sell_trade.cash = self.cash

                    # Update grid strategy
                    self.rtstr.set_lower_zone_unengaged_position(current_trade.symbol, current_trade.gridzone)
                    sell_trade.gridzone = current_trade.gridzone

                    self.current_trades.append(sell_trade)
                    
                    msg = "{} ({}) {} {:.2f} roi={:.2f}".format(sell_trade.type, sell_trade.stimulus, sell_trade.symbol, sell_trade.gross_price, sell_trade.roi)
                    self.log(msg, "symbol sold")

        if self.sell_performed:
            self.rtstr.update(current_datetime, self.current_trades, self.broker.get_cash(),  self.broker.get_cash_borrowed(), self.rtstr.rtctrl.prices_symbols, False, self.final_datetime, self.broker.get_balance())
            self.sell_performed = False

        # buy symbols
        df_buying_symbols = self.rtstr.get_df_buying_symbols()
        df_buying_symbols.set_index('symbol', inplace=True)
        df_buying_symbols.drop(df_buying_symbols[df_buying_symbols['size'] == 0].index, inplace=True)
        if current_datetime == self.final_datetime:
            df_buying_symbols.drop(df_buying_symbols.index, inplace=True)
        symbols_bought = {"symbol":[], "size":[], "percent":[], "gross_price":[], "gridzone":[], "pos_type": []}
        for symbol in df_buying_symbols.index.to_list():
            current_trade = trade.Trade(current_datetime)
            # current_trade.type = self.rtstr.get_open_type(symbol)
            current_trade.type = df_buying_symbols['pos_type'][symbol]
            current_trade.symbol = symbol

            current_trade.symbol_price = self.broker.get_value(symbol)
            current_trade.buying_price = current_trade.symbol_price
            current_trade.gridzone = df_buying_symbols["gridzone"][symbol]

            current_trade.commission = self.broker.get_commission(current_trade.symbol)
            current_trade.minsize = self.broker.get_minimum_size(current_trade.symbol)

            current_trade.gross_size = df_buying_symbols["size"][symbol]  # Gross size
            current_trade.gross_price = round(current_trade.gross_size * current_trade.symbol_price, 4)

            current_trade.net_price = round(current_trade.gross_price * (1 - current_trade.commission), 4)
            current_trade.net_size = round(current_trade.net_price / current_trade.symbol_price, 6)

            current_trade.buying_fee = abs(round(current_trade.gross_price - current_trade.net_price, 4)) # COMMENT CEDE abs to be confirmed
            current_trade.profit_loss = -current_trade.buying_fee
            if current_trade.type == self.rtstr.open_long:
                current_trade.cash_borrowed = 0
            elif current_trade.type == self.rtstr.open_short:
                current_trade.cash_borrowed = current_trade.net_price

            if abs(round(current_trade.gross_price, 4)) <= round(self.cash, 4):
                done = self.broker.execute_trade(current_trade)
                if done:
                    self.cash = self.broker.get_cash()
                    current_trade.cash = self.cash

                    # Update traces
                    self.traces_trade_total_opened = self.traces_trade_total_opened + 1

                    # Update grid strategy
                    self.rtstr.set_zone_engaged(current_trade.symbol, current_trade.symbol_price)

                    self.current_trades.append(current_trade)

                    symbols_bought["symbol"].append(current_trade.symbol)
                    symbols_bought["size"].append(utils.KeepNDecimals(current_trade.gross_size))
                    symbols_bought["percent"].append(utils.KeepNDecimals(df_buying_symbols["percent"][current_trade.symbol]))
                    symbols_bought["gross_price"].append(utils.KeepNDecimals(current_trade.gross_price))
                    symbols_bought["gridzone"].append(df_buying_symbols["gridzone"][current_trade.symbol])
                    symbols_bought["pos_type"].append(df_buying_symbols["pos_type"][current_trade.symbol])
                else:
                    self.rtstr.open_position_failed(symbol)
        df_symbols_bought = pd.DataFrame(symbols_bought)

        if not df_symbols_bought.empty:
            self.log(df_symbols_bought, "symbols bought")

        if self.temp_debug:
            self.debug_trace_current_trades('end_trade', self.current_trades)

        # Clear the current_trades for optimization
        if self.rtstr.authorize_clear_current_trades() and len(self.current_trades) > 1:
            lst_buy_trades = []
            lst_buy_symbols_trades = []
            for current_trade in self.current_trades:
                if self.rtstr.is_open_type(current_trade.type):
                    lst_buy_trades.append(current_trade)
                    lst_buy_symbols_trades.append(current_trade.symbol)
            self.current_trades = lst_buy_trades

        if self.temp_debug:
            self.debug_trace_current_trades('clear    ', self.current_trades)

        # Merge the current_trades for optimization

        if self.rtstr.authorize_merge_current_trades() \
                and len(self.current_trades) > 1:
            lst_buy_trades_merged = []
            lst_buy_symbols_trades_unique = list(set(lst_buy_symbols_trades))
            if len(lst_buy_symbols_trades_unique) < len(lst_buy_symbols_trades):
                for position_type in self.rtstr.get_lst_opening_type():
                    for symbol in lst_buy_symbols_trades_unique:
                        # if lst_buy_symbols_trades.count(symbol) > 1:
                        if utils.count_symbols_with_position_type(self.current_trades, symbol, position_type) > 1:
                            merged_trades = self.merge_current_trades_from_symbol(self.current_trades, symbol, current_datetime, position_type)
                            lst_buy_trades_merged.append(merged_trades)
                        else:
                            for current_trade in self.current_trades:
                                if self.rtstr.is_open_type(current_trade.type)\
                                        and current_trade.symbol == symbol\
                                        and current_trade.type == position_type:
                                    # unique trade
                                    lst_buy_trades_merged.append(current_trade)
                self.current_trades = lst_buy_trades_merged

        if self.temp_debug:
            self.debug_trace_current_trades('merge    ', self.current_trades)

        if current_datetime == self.final_datetime or self.exit:
            self.export_debug_traces()
            self.rtstr.rtctrl.display_summary_info(True)
            self.exit = True

    def export_status(self):
        return self.broker.export_status()

    def backup(self):
        with open(self.backup_filename, 'wb') as file:
            # print("[crah::backup]", self.backup_filename)
            pickle.dump(self, file)

    def export_debug_traces(self):
        if self.temp_debug:
            self.df_debug_traces.to_csv('debug_traces.csv')

    def merge_current_trades_from_symbol(self, current_trades, symbol, current_datetime, position_type):
        lst_trades = []

        lst_current_trade_symbol_price = []
        lst_current_trade_buying_price = []
        lst_current_trade_gridzone = []
        lst_current_trade_commission = []
        lst_current_trade_gross_size = []
        lst_current_trade_gross_price = []
        lst_current_trade_net_price = []
        lst_current_trade_net_size = []
        lst_current_trade_buying_fee = []
        lst_current_trade_profit_loss = []

        for current_trade in current_trades:
            if current_trade.type == position_type and current_trade.symbol == symbol:
                lst_trades.append(current_trade)

                lst_current_trade_symbol_price.append(current_trade.symbol_price)
                lst_current_trade_buying_price.append(current_trade.buying_price)

                lst_current_trade_gridzone.append(current_trade.gridzone)

                lst_current_trade_commission.append(current_trade.commission)

                lst_current_trade_gross_size.append(current_trade.gross_size)
                lst_current_trade_gross_price.append(current_trade.gross_price)

                lst_current_trade_net_price.append(current_trade.net_price)
                lst_current_trade_net_size.append(current_trade.net_size)

                lst_current_trade_buying_fee.append(current_trade.buying_fee)
                lst_current_trade_profit_loss.append(current_trade.profit_loss)

        merged_trade = trade.Trade(current_datetime)

        merged_trade.type = position_type
        merged_trade.symbol = symbol

        merged_trade.sell_id = ""
        merged_trade.stimulus = ""
        merged_trade.roi = ""
        merged_trade.selling_fee = ""

        merged_trade.buying_time = ""

        merged_trade.symbol_price = max(lst_current_trade_symbol_price)
        merged_trade.buying_price = max(lst_current_trade_buying_price)

        merged_trade.gridzone = max(lst_current_trade_gridzone)

        merged_trade.commission = sum(lst_current_trade_commission) / len(lst_current_trade_commission)

        merged_trade.gross_size = sum(lst_current_trade_gross_size)
        merged_trade.gross_price = sum(lst_current_trade_gross_price)

        merged_trade.net_price = sum(lst_current_trade_net_price)
        merged_trade.net_size = sum(lst_current_trade_net_size)

        merged_trade.buying_fee = sum(lst_current_trade_buying_fee)
        merged_trade.profit_loss = sum(lst_current_trade_profit_loss)

        merged_trade.portfolio_value = self.portfolio_value
        merged_trade.wallet_value = self.wallet_value
        merged_trade.wallet_roi = (self.wallet_value - self.init_cash_value) * 100 / self.init_cash_value

        self.traces_trade_total_opened = self.traces_trade_total_opened - len(lst_current_trade_symbol_price) + 1

        return merged_trade

    def debug_trace_current_trades(self, step_state, current_trades):
        iterration = 0
        msg = step_state
        for current_trade in current_trades:
            msg = msg + ' - trade: ' + str(iterration)
            iterration = iterration + 1
            msg = msg + ' symbol: ' + str(current_trade.symbol)
            msg = msg + ' type: ' + str(current_trade.type)
            msg = msg + ' price: ' + str(round(current_trade.net_price, 1))
            msg = msg + ' size: ' + str(round(current_trade.net_size, 4))
        print(msg)