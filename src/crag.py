import os
import shutil
import time
import pandas as pd
from . import trade,rtstr,utils
import pika
import threading
import pickle
from datetime import datetime, timedelta
from datetime import date

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
        self.safety_run = True
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
        self.minimal_portfolio_variation = 0
        self.maximal_portfolio_variation = 0
        self.previous_usdt_equity = 0
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

        self.traces_trade_total_opened = 0
        self.traces_trade_total_closed = 0
        self.traces_trade_total_remaining = 0
        if self.broker.resume_strategy():
            self.current_trades = self.get_current_trades_from_account()
        else:
            self.current_trades = []

        self.cash = 0
        self.init_cash_value = 0
        self.portfolio_value = 0
        self.wallet_value = 0
        self.sell_performed = False
        self.epsilon_size_reduce = 0.1

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

        self.traces_trade_positive = 0
        self.traces_trade_negative = 0
        self.start_date_from_resumed_data = ""

        if self.broker.broker_resumed():
            self.df_reboot_data = self.get_reboot_data()
            if self.df_reboot_data is not None:
                self.traces_trade_positive = self.df_reboot_data["positive trades"][0]
                self.traces_trade_negative = self.df_reboot_data["negative trades"][0]
                self.traces_trade_total_opened = self.df_reboot_data["transactions opened"][0]
                self.traces_trade_total_closed = self.df_reboot_data["closed"][0]
                self.start_date_from_resumed_data = self.df_reboot_data["original start"][0]
                self.original_portfolio_value = self.df_reboot_data["original portfolio value"][0]
                self.maximal_portfolio_value = self.df_reboot_data["max value"][0]
                self.maximal_portfolio_date = self.df_reboot_data["date max value"][0]
                self.minimal_portfolio_value = self.df_reboot_data["min value"][0]
                self.minimal_portfolio_date = self.df_reboot_data["date min value"][0]

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

        start = datetime.now()
        start = start.replace(second=0, microsecond=0)
        if self.interval == 24 * 60 * 60:  # 1d
            start = start.replace(hour=0, minute=0, second=0, microsecond=0)
            start += timedelta(days=1)
        elif self.interval == 60 * 60:  # 1h
            start = start.replace(minute=0, second=0, microsecond=0)
            start += timedelta(hours=1)
        elif self.interval == 60:  # 1m
            start = start.replace(second=0, microsecond=0)
            start += timedelta(minutes=1)
        elif self.interval == 1:  # 1s
            start = start.replace(second=0, microsecond=0)
            start += timedelta(seconds=1)
        print("start time: ", start)
        msg = "start time: " + start.strftime("%Y/%m/%d %H:%M:%S")
        self.log(msg, "start time")
        start = datetime.timestamp(start)

        done = False
        while not done:
            now = time.time()
            sleeping_time = start - now
            # sleeping_time = 0 # CEDE DEBUG TO SKIP THE SLEEPING TIME
            if self.start_date and self.end_date:
                # SIM MODE
                sleeping_time = 0
            if sleeping_time > 0:
                if self.safety_run:
                    start_minus_one_sec = datetime.timestamp(datetime.fromtimestamp(start) - timedelta(seconds=1))
                    while time.time() < start_minus_one_sec:
                        step_result = self.safety_step()
                        if not step_result:
                            os._exit(0) # tbc
                    while time.time() < start:
                        pass
                else:
                    time.sleep(sleeping_time)
            else:
                self.log(
                    "warning : time elapsed for the step ({}) is greater than the interval ({})".format(sleeping_time, self.interval))
            start = datetime.fromtimestamp(start)
            if self.interval == 24 * 60 * 60:  # 1d
                start += timedelta(days=1)
            elif self.interval == 60 * 60:  # 1h
                start += timedelta(hours=1)
            elif self.interval == 60:  # 1m
                start += timedelta(minutes=1)
            self.strategy_start_time = start
            start = datetime.timestamp(start)

            done = not self.step()
            if done:
                break

            self.broker.tick() # increment
            # self.backup() # backup for reboot

            if self.interval == 1:  # 1m # CEDE: CL to find cleaner solution
                start = datetime.now() + timedelta(seconds=2)
                start = datetime.timestamp(start)

        self.export_history(self.export_filename)

    def step(self):
        prices_symbols, ds = self.get_ds_and_price_symbols()
        current_datetime = self.broker.get_current_datetime()
        self.rtstr.update(current_datetime, self.current_trades, self.broker.get_cash(), self.broker.get_cash_borrowed(), prices_symbols, False, self.final_datetime, self.broker.get_balance())

        measure_time_fdp_start = datetime.now()

        nb_try = 0
        current_data_received = False
        while current_data_received != True:
            current_data = self.broker.get_current_data(ds)
            if current_data is None:
                print("current_data not received: ", nb_try)
                nb_try += 1
            else:
                current_data_received = True

        measure_time_fdp_end = datetime.now()
        print("measure time fdp:", measure_time_fdp_end - measure_time_fdp_start)

        if current_data is None:
            if not self.zero_print:
                print("[Crag] 💥 no current data")
            # self.force_sell_open_trade()
            self.rtstr.update(current_datetime, self.current_trades, self.broker.get_cash(), prices_symbols, True, self.final_datetime, self.broker.get_balance())
            return False

        self.rtstr.set_current_data(current_data)

        # portfolio_value = self.broker.get_portfolio_value()
        # portfolio_value = self.broker.get_wallet_equity()
        portfolio_value = self.broker.get_usdt_equity()
        current_date = self.broker.get_current_datetime("%Y/%m/%d %H:%M:%S")
        if portfolio_value < self.minimal_portfolio_value:
            self.minimal_portfolio_value = portfolio_value
            self.minimal_portfolio_date = current_date
            self.minimal_portfolio_variation = utils.get_variation(self.original_portfolio_value, self.minimal_portfolio_value)
        if portfolio_value > self.maximal_portfolio_value:
            self.maximal_portfolio_value = portfolio_value
            self.maximal_portfolio_date = current_date
            self.maximal_portfolio_variation = utils.get_variation(self.original_portfolio_value, self.maximal_portfolio_value)

        lst_stored_data_for_reboot = []
        msg = "start step current time : {}\n".format(current_date)
        if self.broker.is_reset_account():
            msg += "account reset\n"
            start_date_for_log = self.start_date
        else:
            msg += "account resumed\n"
            if self.start_date_from_resumed_data != "":
                start_date_for_log = self.start_date_from_resumed_data
            else:
                start_date_for_log = self.start_date
        msg += "original portfolio value : ${} ({})\n".format(utils.KeepNDecimals(self.original_portfolio_value, 2), start_date_for_log)
        lst_stored_data_for_reboot.append(self.start_date)
        lst_stored_data_for_reboot.append(self.original_portfolio_value)
        variation_percent = utils.get_variation(self.original_portfolio_value, portfolio_value)
        msg += "current portfolio value : ${} / %{}\n".format(utils.KeepNDecimals(portfolio_value, 2),
                                                              utils.KeepNDecimals(variation_percent, 2))
        msg += "max value : ${} %{} ({})\n".format(utils.KeepNDecimals(self.maximal_portfolio_value, 2),
                                                   utils.KeepNDecimals(self.maximal_portfolio_variation, 2),
                                                   self.maximal_portfolio_date)
        lst_stored_data_for_reboot.append(self.maximal_portfolio_value)
        lst_stored_data_for_reboot.append(self.maximal_portfolio_date)
        msg += "min value : ${} %{} ({})\n".format(utils.KeepNDecimals(self.minimal_portfolio_value, 2),
                                                   utils.KeepNDecimals(self.minimal_portfolio_variation, 2),
                                                   self.minimal_portfolio_date)
        lst_stored_data_for_reboot.append(self.minimal_portfolio_value)
        lst_stored_data_for_reboot.append(self.minimal_portfolio_date)
        self.traces_trade_total_remaining = self.traces_trade_total_opened - self.traces_trade_total_closed
        msg += "transactions opened : {} / closed : {} / remaining open : {}\n".format(int(self.traces_trade_total_opened),
                                                                                       int(self.traces_trade_total_closed),
                                                                                       int(self.traces_trade_total_remaining))
        lst_stored_data_for_reboot.append(self.traces_trade_total_opened)
        lst_stored_data_for_reboot.append(self.traces_trade_total_closed)
        lst_stored_data_for_reboot.append(self.traces_trade_total_remaining)
        if self.traces_trade_total_closed == 0:
            win_rate = 0
        else:
            win_rate = 100 * self.traces_trade_positive / self.traces_trade_total_closed
        msg += "positive / negative trades : {} / {}\n".format(int(self.traces_trade_positive),
                                                               int(self.traces_trade_negative)
                                                               )
        lst_stored_data_for_reboot.append(self.traces_trade_positive)
        lst_stored_data_for_reboot.append(self.traces_trade_negative)
        self.save_reboot_data(lst_stored_data_for_reboot)
        msg += "win rate : %{}\n\n".format(utils.KeepNDecimals(win_rate, 2))

        if self.rtstr.rtctrl.get_rtctrl_nb_symbols() > 0:
            list_symbols = self.rtstr.rtctrl.get_rtctrl_lst_symbols()
            msg += "open position: {}\n".format(len(list_symbols))
            list_value = self.rtstr.rtctrl.get_rtctrl_lst_values()
            list_roi_dol = self.rtstr.rtctrl.get_rtctrl_lst_roi_dol()
            list_roi_percent = self.rtstr.rtctrl.get_rtctrl_lst_roi_percent()
            dict = {'symbol': list_symbols, 'value': list_value, 'roi_dol': list_roi_dol, 'roi_perc': list_roi_percent}
            df_position_at_start = pd.DataFrame(dict)
            df_position_at_start.sort_values(by=['roi_dol'], ascending=True, inplace=True)
            df_position_at_start["value"] = df_position_at_start["value"].round(2)
            df_position_at_start["roi_dol"] = df_position_at_start["roi_dol"].round(2)
            df_position_at_start["roi_perc"] = df_position_at_start["roi_perc"].round(2)
            positions_at_step_start = True
        else:
            msg += "no position\n"
            positions_at_step_start = False

        msg += "\nglobal unrealized PL = ${} / %{}\n".format(utils.KeepNDecimals(self.broker.get_global_unrealizedPL(), 2),
                                                             utils.KeepNDecimals(self.broker.get_global_unrealizedPL() * 100 / self.original_portfolio_value, 2) )
        msg += "current cash = ${}\n".format(utils.KeepNDecimals(self.broker.get_cash(), 2))
        usdt_equity = self.broker.get_usdt_equity()
        variation_percent = utils.get_variation(self.original_portfolio_value, usdt_equity)
        msg += "account equity = ${} / %{}".format(utils.KeepNDecimals(usdt_equity, 2),
                                                   utils.KeepNDecimals(variation_percent, 2))

        self.log(msg, "start step")

        if positions_at_step_start and len(df_position_at_start) >= 0:
            log_title = "step start with {} open position".format(len(df_position_at_start))
            self.log(df_position_at_start, log_title)

        if not self.zero_print:
            print("[Crag] ⌛")

        # execute trading
        self.trade()

        self.rtstr.log_current_info()

        unrealised_PL_long = 0
        unrealised_PL_short = 0
        lst_symbol_position = self.broker.get_lst_symbol_position()

        print("lst_symbol_position", lst_symbol_position)  # CEDE DEBUG

        if len(lst_symbol_position) > 0:
            msg = "end step with {} open position\n".format(len(lst_symbol_position))
            df_open_positions = pd.DataFrame(columns=["symbol", "pos_type", "size", "equity", "PL", "PL%"])
            for symbol in lst_symbol_position:
                symbol_equity = self.broker.get_symbol_usdtEquity(symbol)
                symbol_unrealizedPL = self.broker.get_symbol_unrealizedPL(symbol)
                if (symbol_equity - symbol_unrealizedPL) == 0:
                    symbol_unrealizedPL_percent = 0
                else:
                    symbol_unrealizedPL_percent = symbol_unrealizedPL * 100 / (symbol_equity - symbol_unrealizedPL)

                list_row = [self.broker.get_coin_from_symbol(symbol),
                            self.broker.get_symbol_holdSide(symbol).upper(),
                            utils.KeepNDecimals(self.broker.get_symbol_available(symbol), 2),
                            utils.KeepNDecimals(symbol_equity, 2),
                            utils.KeepNDecimals(symbol_unrealizedPL, 2),
                            utils.KeepNDecimals(symbol_unrealizedPL_percent, 2)
                            ]
                df_open_positions.loc[len(df_open_positions)] = list_row

                if self.broker.get_symbol_holdSide(symbol).upper() == "LONG":
                    unrealised_PL_long += symbol_unrealizedPL
                else:
                    unrealised_PL_short += symbol_unrealizedPL

            if len(df_open_positions) > 0:
                self.log(df_open_positions, msg)
            else:
                msg = "no position\n"
                self.log(msg, "no open position")
        else:
            msg = "no position\n"
            self.log(msg, "no open position")

        current_date = self.broker.get_current_datetime("%Y/%m/%d %H:%M:%S")
        msg = "end step current time : {}\n".format(current_date)
        msg += "account equity at start : $ {} ({})\n".format(utils.KeepNDecimals(self.original_portfolio_value, 2), self.start_date)
        msg += "unrealized PL LONG : ${}\n".format(utils.KeepNDecimals(unrealised_PL_long, 2))
        msg += "unrealized PL SHORT : ${}\n".format(utils.KeepNDecimals(unrealised_PL_short, 2))
        msg += "global unrealized PL : ${} / %{}\n".format(utils.KeepNDecimals(self.broker.get_global_unrealizedPL(), 2),
                                                           utils.KeepNDecimals(self.broker.get_global_unrealizedPL() * 100 / self.original_portfolio_value, 2))
        msg += "current cash = {}\n".format(utils.KeepNDecimals(self.broker.get_cash(), 2))
        usdt_equity = self.broker.get_usdt_equity()
        if self.previous_usdt_equity == 0:
            self.previous_usdt_equity = usdt_equity
        variation_percent = utils.get_variation(self.original_portfolio_value, usdt_equity)
        msg += "account equity : ${} / %{}\n".format(utils.KeepNDecimals(usdt_equity, 2),
                                                   utils.KeepNDecimals(variation_percent, 2))
        variation_percent = utils.get_variation(self.previous_usdt_equity, usdt_equity)
        msg += "previous equity : ${} / ${} / %{}".format(utils.KeepNDecimals(self.previous_usdt_equity, 2),
                                                                   utils.KeepNDecimals(usdt_equity - self.previous_usdt_equity, 2),
                                                                   utils.KeepNDecimals(variation_percent, 2))
        self.previous_usdt_equity = usdt_equity
        self.log(msg, "end step")

        return not self.exit

    def export_history(self, target=None):
        self.broker.export_history(target)

    def _prepare_sell_trade_from_bought_trade(self, bought_trade, current_datetime):
        sell_trade = trade.Trade(current_datetime)
        sell_trade.type = self.rtstr.get_close_type(bought_trade.symbol)
        sell_trade.sell_id = bought_trade.id
        sell_trade.buying_price = bought_trade.buying_price
        sell_trade.buying_time = bought_trade.time
        # sell_trade.stimulus = df_selling_symbols["stimulus"][bought_trade.symbol]
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
        # if sell_trade.type == self.rtstr.close_short:
        #    sell_trade.roi = -sell_trade.roi
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
        df_sell_performed = pd.DataFrame(columns=["symbol", "price", "roi%", "pos_type"])
        for current_trade in self.current_trades:
            if self.rtstr.is_open_type(current_trade.type) and current_trade.symbol in list_symbols_to_sell \
                    and (self.flush_current_trade
                         or ((self.rtstr.get_selling_limit(current_trade.symbol))
                             and (self.rtstr.get_grid_sell_condition(current_trade.symbol, current_trade.gridzone))
                             or self.rtstr.grid_exit_range_trend_down(current_trade.symbol))):
                sell_trade = self._prepare_sell_trade_from_bought_trade(current_trade, current_datetime)
                done = self.broker.execute_trade(sell_trade)
                if done:
                    self.sell_performed = True
                    self.rtstr.count_selling_limits(current_trade.symbol)
                    current_trade.type = self.rtstr.get_close_type_and_close(current_trade.symbol)
                    self.cash = self.broker.get_cash()
                    sell_trade.cash = self.cash

                    self.traces_trade_total_closed += 1
                    # Update grid strategy
                    self.rtstr.set_lower_zone_unengaged_position(current_trade.symbol, current_trade.gridzone)
                    sell_trade.gridzone = current_trade.gridzone

                    self.current_trades.append(sell_trade)
                    df_sell_performed.loc[len(df_sell_performed.index)] = [sell_trade.symbol, round(sell_trade.gross_price, 2), round(sell_trade.roi, 2), sell_trade.type]
                    # msg = "{} : {} price: ${:.2f} roi: ${:.2f}".format(sell_trade.symbol, sell_trade.type, sell_trade.gross_price, sell_trade.roi)
                    if sell_trade.roi < 0:
                        self.traces_trade_negative += 1
                    else:
                        self.traces_trade_positive += 1

        if self.sell_performed:
            if len(df_sell_performed) > 0:
                self.log(df_sell_performed, "symbol sold - performed")
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
            print("buying symbol: ", symbol)
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
            current_trade.gross_price = current_trade.gross_size * current_trade.symbol_price
            while abs(round(current_trade.gross_price, 4)) >= round(self.cash, 4):
                print("=========== > gross price do not fit cash value:")
                print('=========== > current_trade.gross_size: ', current_trade.gross_size)
                print('=========== > current_trade.gross_price: ', current_trade.gross_price)
                print('=========== > self.cash: ', self.cash)
                current_trade.symbol_price = self.broker.get_value(symbol)
                df_buying_symbols["size"][symbol] = df_buying_symbols["size"][symbol] - df_buying_symbols["size"][symbol] * self.epsilon_size_reduce / 100
                current_trade.gross_size = df_buying_symbols["size"][symbol]
                current_trade.gross_price = current_trade.gross_size * current_trade.symbol_price
                print("=========== > size and price reduced:")
                print('=========== > reduced current_trade.gross_price: ', current_trade.gross_price)
                print('=========== > reduced current_trade.gross_size: ', current_trade.gross_size)
                print('=========== > self.cash: ', self.cash)

            current_trade.net_price = round(current_trade.gross_price * (1 - current_trade.commission), 4)
            current_trade.net_size = round(current_trade.net_price / current_trade.symbol_price, 6)

            current_trade.buying_fee = abs(round(current_trade.gross_price - current_trade.net_price, 4))
            current_trade.profit_loss = -current_trade.buying_fee
            if current_trade.type == self.rtstr.open_long:
                current_trade.cash_borrowed = 0
            elif current_trade.type == self.rtstr.open_short:
                current_trade.cash_borrowed = current_trade.net_price

            if abs(current_trade.gross_price) <= self.cash:
                done = self.broker.execute_trade(current_trade)
                if done:
                    self.cash = self.broker.get_cash()
                    current_trade.cash = self.cash

                    self.traces_trade_total_opened += 1

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
            else:
                print("=========== > execute trade not actioned - gross price do not fit cash value:")
                print("=========== >abs(round(current_trade.gross_price, 4)) <= round(self.cash, 4)",
                      abs(round(current_trade.gross_price, 4)) <= round(self.cash, 4))
                print('=========== >current_trade.gross_price: ', current_trade.gross_price)
                print('=========== >self.cash: ', self.cash)
                self.rtstr.open_position_failed(symbol)

        df_symbols_bought = pd.DataFrame(symbols_bought)

        if not df_symbols_bought.empty:
            df_traces = df_symbols_bought.copy()
            df_traces.drop(columns=['gridzone'], axis=1, inplace=True)
            self.log(df_traces, "symbols bought")

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

    def safety_step(self):
        global_unrealizedPL = self.broker.get_global_unrealizedPL()
        if self.original_portfolio_value == 0:
            global_unrealizedPL_percent = 0
        else:
            global_unrealizedPL_percent = self.broker.get_global_unrealizedPL() * 100 / self.original_portfolio_value
        # print("global_unrealizedPL: ", global_unrealizedPL, " - ", global_unrealizedPL_percent, "%") # DEBUG CEDE
        total_PL = self.broker.get_usdt_equity() - self.original_portfolio_value
        if self.original_portfolio_value == 0:
            total_PL_percent = 0
        else:
            total_PL_percent = total_PL * 100 / self.original_portfolio_value

        TOTAL_PERCENT = True # CEDE Test
        if TOTAL_PERCENT:
            if self.rtstr.condition_for_global_SLTP(total_PL_percent) \
                    or self.rtstr.condition_for_global_trailer_TP(total_PL_percent):
                print('reset - global TP')
                print('total PL: $', total_PL, " - ", total_PL_percent, "%")
                msg = "reset - total SL TP"
                self.log(msg, "total SL TP")

                self.broker.execute_reset_account()
                return False
        else:
            if self.rtstr.condition_for_global_SLTP(global_unrealizedPL_percent)\
                    or self.rtstr.condition_for_global_trailer_TP(global_unrealizedPL_percent):
                print('reset - global TP')
                print('unrealizedPL: $', global_unrealizedPL, " - ", global_unrealizedPL_percent, "%")
                msg = "reset - global unrealizedPL SL TP"
                self.log(msg, "global PL SL TP")

                self.broker.execute_reset_account()
                return False

        lst_symbol_position = self.broker.get_lst_symbol_position()
        lst_symbol_for_closure = []
        for symbol in lst_symbol_position:
            symbol_equity = self.broker.get_symbol_usdtEquity(symbol)
            symbol_unrealizedPL = self.broker.get_symbol_unrealizedPL(symbol)
            if (symbol_equity - symbol_unrealizedPL) == 0:
                symbol_unrealizedPL_percent = 0
            else:
                symbol_unrealizedPL_percent = symbol_unrealizedPL * 100 / (symbol_equity - symbol_unrealizedPL)
            # print("symbol", symbol, "symbol_unrealizedPL: $", symbol_unrealizedPL, " - ", symbol_unrealizedPL_percent, "%") # DEBUG CEDE
            if self.rtstr.condition_for_SLTP(symbol_unrealizedPL_percent) \
                    or self.rtstr.condition_trailer_TP(self.broker._get_coin(symbol), symbol_unrealizedPL_percent):
                lst_symbol_for_closure.append(symbol)

        if len(lst_symbol_for_closure) > 0:
            current_datetime = datetime.today().strftime("%Y/%m/%d %H:%M:%S")
            for current_trade in self.current_trades:
                symbol = self.broker._get_symbol(current_trade.symbol)
                coin = current_trade.symbol
                if self.rtstr.is_open_type(current_trade.type) and symbol in lst_symbol_for_closure:
                    print("SELL TRIGGERED SL TP: ", current_trade.symbol, " at: ", current_datetime) # DEBUG CEDE
                    msg = "SELL TRIGGERED SL TP: {} at: {}\n".format(current_trade.symbol, current_datetime)
                    sell_trade = self._prepare_sell_trade_from_bought_trade(current_trade,
                                                                            current_datetime)
                    done = self.broker.execute_trade(sell_trade)
                    if done:
                        print("SELL PERFORMED SL TP: ", current_trade.symbol, " at: ", current_datetime) # DEBUG CEDE
                        msg += "SELL PERFORMED SL TP"
                        self.log(msg, "SL TP PERFORMED")

                        self.sell_performed = True
                        self.rtstr.count_selling_limits(current_trade.symbol)
                        current_trade.type = self.rtstr.get_close_type_and_close(current_trade.symbol)
                        self.cash = self.broker.get_cash()
                        sell_trade.cash = self.cash

                        # Update grid strategy
                        self.rtstr.set_lower_zone_unengaged_position(current_trade.symbol, current_trade.gridzone)
                        sell_trade.gridzone = current_trade.gridzone

                        self.current_trades.append(sell_trade)
                    else:
                        print("SELL TRANSACTION FAILED SL TP: ", current_trade.symbol, " at: ", current_datetime)  # DEBUG CEDE
                        msg += "SELL TRANSACTION FAILED SL TP"
                        self.log(msg, "SL TP FAILED")

                    if self.sell_performed:
                        self.rtstr.update(current_datetime, self.current_trades, self.broker.get_cash(),
                                          self.broker.get_cash_borrowed(), self.rtstr.rtctrl.prices_symbols, False,
                                          self.final_datetime, self.broker.get_balance())
                        self.rtstr.set_symbol_trailer_tp_turned_off(coin)
                        self.sell_performed = False
        return True


    def get_current_trades_from_account(self):
        lst_symbol_position = self.broker.get_lst_symbol_position()
        current_open_trades = []
        for symbol in lst_symbol_position:
            current_datetime = datetime.today().strftime("%Y/%m/%d %H:%M:%S")
            current_trade = trade.Trade(current_datetime)
            current_trade.symbol = self.broker._get_coin(symbol)
            current_trade.buying_time = current_datetime
            current_trade.type = self.rtstr.get_bitget_position(current_trade.symbol, self.broker.get_symbol_holdSide(symbol))

            # current_trade.symbol_price = self.broker.get_value(symbol)
            current_trade.symbol_price = self.broker.get_symbol_marketPrice(symbol)
            current_trade.buying_price = self.broker.get_symbol_averageOpenPrice(symbol)

            current_trade.gridzone = -1

            current_trade.commission = self.broker.get_commission(current_trade.symbol)
            current_trade.minsize = self.broker.get_minimum_size(current_trade.symbol)

            current_trade.gross_size = self.broker.get_symbol_total(symbol)
            current_trade.gross_price = self.broker.get_symbol_usdtEquity(symbol)
            current_trade.bought_gross_price = current_trade.gross_price

            current_trade.net_price = self.broker.get_symbol_usdtEquity(symbol)
            current_trade.net_size = self.broker.get_symbol_available(symbol)

            if current_trade.net_size != current_trade.gross_size:
                print("warning size check: ", symbol, " - ", current_trade.net_size, " - ", current_trade.gross_size)

            current_trade.buying_fee = 0
            current_trade.profit_loss = 0

            if current_trade.type == self.rtstr.open_long:
                current_trade.cash_borrowed = 0
            elif current_trade.type == self.rtstr.open_short:
                current_trade.cash_borrowed = current_trade.net_price

            self.cash = self.broker.get_cash()
            current_trade.cash = self.cash
            current_trade.roi = self.broker.get_symbol_unrealizedPL(symbol)

            # Update traces
            self.traces_trade_total_opened = self.traces_trade_total_opened + 1

            current_open_trades.append(current_trade)

        return current_open_trades

    def get_ds_and_price_symbols(self):
        # update all the data
        # TODO : this call should be done once, during the initialization of the system
        ds = self.rtstr.get_data_description()

        if self.clear_unused_data:
            self.broker.check_data_description(ds)
            self.clear_unused_data = False
        try:
            prices_symbols = {symbol:self.broker.get_value(symbol) for symbol in ds.symbols}
        except:
            for symbol in ds.symbols:
                try:
                    self.broker.get_value(symbol)
                except:
                    print("symbol error: ", symbol)

        print('price: ', prices_symbols)

        return prices_symbols, ds

    def get_stored_data_list(self):
        return [
            "original start",             # OK
            "original portfolio value",   # OK
            "max value",            # OK
            "date max value",       # OK
            "min value",            # OK
            "date min value",       # OK
            "transactions opened",  # OK
            "closed",           # OK
            "remaining open",   # OK
            "positive trades",  # OK
            "negative trades"   # OK
        ]

    def save_reboot_data(self, lst_data):
        df = pd.DataFrame(columns=self.get_stored_data_list())
        df.loc[len(df)] = lst_data
        self.broker.save_reboot_data(df)

    def get_reboot_data(self):
        return self.broker.get_broker_boot_data()