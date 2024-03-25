import os
from pympler import asizeof
import shutil
import time
import pandas as pd
from . import trade,rtstr,utils,traces
from .toolbox import monitoring_helper
import pika
import json
import ast
import threading
import pickle
from pathlib import Path
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
        self.end_date = ""
        self.original_portfolio_value = 0
        self.minimal_portfolio_value = 0
        self.minimal_portfolio_date = ""
        self.maximal_portfolio_value = 0
        self.minimal_portfolio_variation = 0
        self.maximal_portfolio_variation = 0
        self.previous_usdt_equity = 0
        self.maximal_portfolio_date = ""
        self.id = str(utils.get_random_id())
        self.high_volatility_sleep_duration = 0
        self.activate_volatility_sleep = False
        self.drawdown = 0
        self.actual_drawdown_percent = 0
        self.total_SL_TP = 0
        self.total_SL_TP_percent = 0
        self.monitoring = monitoring_helper.SQLMonitoring("ovh_mysql")
        self.tradetraces = traces.TradeTraces()
        self.init_grid_position = True
        self.start_time_grid_strategy = None
        self.iteration_times_grid_strategy = []
        self.average_time_grid_strategy = 0
        self.average_time_grid_strategy_overall = 0
        self.start_time_grid_strategy_init = None
        self.grid_iteration = 0

        self.crag_size_previous = 0
        self.crag_size = 0
        self.crag_size_init = 0
        self.rtstr_size_previous = 0
        self.rtstr_size = 0
        self.rtstr_size_init = 0
        self.broker_size_previous = 0
        self.broker_size = 0
        self.broker_size_init = 0
        self.rtctrl_size_previous = 0
        self.rtctrl_size = 0
        self.rtctrl_size_init = 0
        self.rtstr_grid_size_previous = 0
        self.rtstr_grid_size = 0
        self.rtstr_grid_size_init = 0
        self.memory_used_mb = 0
        self.init_memory_used_mb = 0
        self.init_debug_memory = False

        self.init_memory_usage = {}
        self.previous_memory_usage = {}
        self.memory_usage = {}

        if params:
            self.broker = params.get("broker", self.broker)
            if self.broker:
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
        if self.broker and self.broker.resume_strategy():
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

        if self.rtstr:
            self.strategy_name = self.rtstr.get_info()
        if self.broker:
            self.final_datetime = self.broker.get_final_datetime()
            # self.start_date, self.end_date, self.interval = self.broker.get_info()
            self.start_date, self.end_date, _ = self.broker.get_info() # CEDE To be confirmed
        self.export_filename = "sim_broker_history" + "_" + self.strategy_name + "_" + str(self.start_date) + "_" + str(self.end_date) + "_" + str(self.interval) + ".csv"
        self.backup_filename = self.id + "_crag_backup.pickle"

        if not self.working_directory:
            self.working_directory = './output/' # CEDE NOTE: output directory name to be added to .xml / output as default value

        if not os.path.exists(self.working_directory):
            os.makedirs(self.working_directory)
        else:
            shutil.rmtree(self.working_directory)
            os.makedirs(self.working_directory)
        self.export_filename = os.path.join(self.working_directory, self.export_filename)
        self.backup_filename = os.path.join(self.working_directory, self.backup_filename)

        self.temp_debug = True

        self.traces_trade_positive = 0
        self.traces_trade_negative = 0
        self.start_date_from_resumed_data = ""
        self.start_date_for_log = ""

        if self.broker and self.broker.broker_resumed():
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
        self.send_alive_notification()

        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))
            channel = connection.channel()
            channel.queue_declare(queue="StrategyMonitoring")

            def callback(ch, method, properties, bbody):
                print(" [x] Received {}".format(bbody))
                body = bbody.decode()
                body = json.loads(body)
                if body and body["id"] == "command" and body["strategy_id"] == self.rtstr.id:
                    command = body["command"]
                    if command == "stop":
                        print("stopping ", self.rtstr.id)
                        os._exit(0)

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

            channel.basic_consume(queue="StrategyMonitoring", on_message_callback=callback, auto_ack=True)
            
            #channel.start_consuming()
            thread = threading.Thread(name='t', target=channel.start_consuming, args=())
            thread.setDaemon(True)
            thread.start()
        except:
            print("Problem encountered while configuring the rabbitmq receiver")


    def log(self, msg, header="", attachments=[]):
        if self.logger:
            self.logger.log(msg, header="["+self.id+"] "+header, author=type(self).__name__, attachments=attachments)

    def send_alive_notification(self):
        if self.broker and self.broker.account and self.rtstr:
            current_datetime = datetime.now()
            current_timestamp = datetime.timestamp(current_datetime)
            self.monitoring.send_alive_notification(current_timestamp, self.broker.account.get("id"), self.rtstr.id)

    def run(self):
        self.start_date = self.broker.get_current_datetime("%Y/%m/%d %H:%M:%S")

        self.minimal_portfolio_date = self.start_date
        self.maximal_portfolio_date = self.start_date
        msg_broker_info = self.broker.log_info()
        self.log(msg_broker_info, "run")
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
        elif self.interval == 1 \
                or self.interval is None:  # 1s
            start = start.replace(second=0, microsecond=0)
            start += timedelta(seconds=1)
        print("start time: ", start)
        msg = "start time: " + start.strftime("%Y/%m/%d %H:%M:%S")
        self.log(msg, "start time")
        start = datetime.timestamp(start)

        done = False
        while not done:
            print("[RUN] [CRAG] while step 1")
            now = time.time()
            sleeping_time = start - now
            # sleeping_time = 0 # CEDE DEBUG TO SKIP THE SLEEPING TIME
            if sleeping_time > 0:
                if self.safety_run:
                    start_minus_one_sec = datetime.timestamp(datetime.fromtimestamp(start) - timedelta(seconds=1))
                    while time.time() < start_minus_one_sec:
                        print("[RUN] [CRAG] while step 2")
                        step_result = self.safety_step()
                        print("[RUN] [CRAG] while step 3")
                        if not step_result:
                            print("safety_step result exit")
                            os._exit(0)
                        if self.rtstr.high_volatility.high_volatility_pause_status():
                            msg = "duration: " + str(self.rtstr.high_volatility.high_volatility_get_duration()) + "seconds"
                            self.log(msg, "PAUSE DUE HIGH VOLATILITY")
                            time.sleep(self.rtstr.high_volatility.high_volatility_get_duration())
                    while time.time() < start:
                        pass
                    print("[RUN] [CRAG] while step 4")
                else:
                    print("[RUN] [CRAG] while step 6")
                    time.sleep(sleeping_time)
                    print("[RUN] [CRAG] while step 7")
            else:
                # COMMENT CEDE REDUNDANT CODE
                if self.safety_run:
                    print("[RUN] [CRAG] while step 8")
                    step_result = self.safety_step()
                    print("[RUN] [CRAG] while step 9")
                    if self.interval != 1:  # 1s
                        self.log("safety run executed\n"
                                 + "warning : time elapsed for the step ({}) is greater than the interval ({})".format(sleeping_time, self.interval))
                    if not step_result:
                        os._exit(0)
                    print("[RUN] [CRAG] while step 10")
                    if self.rtstr.high_volatility.high_volatility_pause_status():
                        msg = "duration: " + str(self.rtstr.high_volatility.high_volatility_get_duration()) + "seconds"
                        self.log(msg, "PAUSE DUE HIGH VOLATILITY")
                        time.sleep(self.rtstr.high_volatility.high_volatility_get_duration())
                else:
                    print("[RUN] [CRAG] while step 11")
                    if self.interval != 1:  # 1s
                        self.log(
                            "warning : time elapsed for the step ({}) is greater than the interval ({})".format(
                                sleeping_time, self.interval))

            start = datetime.fromtimestamp(start)
            if self.interval == 24 * 60 * 60:  # 1d
                start += timedelta(days=1)
            elif self.interval == 60 * 60:  # 1h
                start += timedelta(hours=1)
            elif self.interval == 60:  # 1m
                start += timedelta(minutes=1)
            self.strategy_start_time = start
            start = datetime.timestamp(start)

            print("[RUN] [CRAG] while step 12")
            done = not self.step()
            if done:
                print("[RUN] [CRAG] while step 13")
                break
            print("[RUN] [CRAG] while step 14")

            self.broker.tick() # increment
            # self.backup() # backup for reboot

            if self.interval == 1:  # 1m # CEDE: CL to find cleaner solution
                start = datetime.now() + timedelta(seconds=2)
                start = datetime.timestamp(start)

        self.export_history(self.export_filename)

    def step(self):
        self.send_alive_notification()
        stop = self.monitoring.get_strategy_stop(self.rtstr.id)
        if stop:
            self.monitoring.send_strategy_stopped(self.rtstr.id)
            os._exit(0)
        prices_symbols, ds = self.get_ds_and_price_symbols()
        current_datetime = self.broker.get_current_datetime()
        self.rtstr.update(current_datetime, self.current_trades, self.broker.get_cash(), self.broker.get_cash_borrowed(), prices_symbols, False, self.final_datetime, self.broker.get_balance())

        if (self.rtstr.rtctrl.get_rtctrl_nb_symbols() > 0)\
                and (self.rtstr.position_recorder.get_total_position_engaged() == 0):
            # After reset PositionRecorder have to be updated
            print('reset PositionRecorder')
            self.rtstr.position_recorder.update_position_recorder(self.rtstr.rtctrl.get_rtctrl_lst_symbols())
        else:
            print('DEBUG - nb positions from rctctrl:          ', self.rtstr.rtctrl.get_rtctrl_nb_symbols())
            print('DEBUG - nb positions from PositionRecorder: ', self.rtstr.position_recorder.get_total_position_engaged())

        measure_time_fdp_start = datetime.now()

        nb_try = 0
        current_data_received = False

        # CEDE DEBUG
        print("request fdp [CRAG] [step]")
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
            self.start_date_for_log = self.start_date
        else:
            msg += "account resumed\n"
            if self.start_date_from_resumed_data != "":
                self.start_date_for_log = self.start_date_from_resumed_data
            else:
                self.start_date_for_log = self.start_date
        msg += "original value : ${} ({})\n".format(utils.KeepNDecimals(self.original_portfolio_value, 2),
                                                              self.start_date_for_log)
        lst_stored_data_for_reboot.append(self.start_date_for_log)
        lst_stored_data_for_reboot.append(self.original_portfolio_value)
        variation_percent = utils.get_variation(self.original_portfolio_value, portfolio_value)
        msg += "current value : ${} / %{}\n".format(utils.KeepNDecimals(portfolio_value, 2),
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

        self.log(msg.upper(), "start step".upper())

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
            df_open_positions = pd.DataFrame(columns=["symb", "type", "size", "eq", "PL", "PL%"])
            for symbol in lst_symbol_position:
                symbol_equity = self.broker.get_symbol_usdtEquity(symbol)
                symbol_unrealizedPL = self.broker.get_symbol_unrealizedPL(symbol)
                if (symbol_equity - symbol_unrealizedPL) == 0:
                    symbol_unrealizedPL_percent = 0
                else:
                    symbol_unrealizedPL_percent = symbol_unrealizedPL * 100 / (symbol_equity - symbol_unrealizedPL)

                total = self.broker.get_symbol_total(symbol)
                dec_total = utils.calculate_decimal_places(total)
                dec_unrealizedPL = utils.calculate_decimal_places(symbol_unrealizedPL)
                dec_unrealizedPL_percent = utils.calculate_decimal_places(symbol_unrealizedPL_percent)

                list_row = [self.broker.get_coin_from_symbol(symbol),
                            self.broker.get_symbol_holdSide(symbol).upper(),
                            utils.KeepNDecimals(total, dec_total),
                            utils.KeepNDecimals(symbol_equity, 1),
                            utils.KeepNDecimals(symbol_unrealizedPL, dec_unrealizedPL),
                            utils.KeepNDecimals(symbol_unrealizedPL_percent, dec_unrealizedPL_percent)
                            ]
                df_open_positions.loc[len(df_open_positions)] = list_row

                if self.broker.get_symbol_holdSide(symbol).upper() == "LONG":
                    unrealised_PL_long += symbol_unrealizedPL
                else:
                    unrealised_PL_short += symbol_unrealizedPL

            if len(df_open_positions) > 0:
                # self.log(df_open_positions, msg)
                self.log(df_open_positions.to_string().upper(), msg)
            else:
                msg = "no open position\n"
                self.log(msg.upper(), "no open position".upper())
        else:
            msg = "no open position\n"
            self.log(msg.upper(), "no open position".upper())

        current_date = self.broker.get_current_datetime("%Y/%m/%d %H:%M:%S")
        msg = "end step current time : {}\n".format(current_date)
        msg += "equity at start : $ {} ({})\n".format(utils.KeepNDecimals(self.original_portfolio_value, 2), self.start_date)
        msg += "unrealized PL LONG : ${}\n".format(utils.KeepNDecimals(unrealised_PL_long, 2))
        msg += "unrealized PL SHORT : ${}\n".format(utils.KeepNDecimals(unrealised_PL_short, 2))
        msg += "global unrealized PL : ${} / %{}\n".format(utils.KeepNDecimals(self.broker.get_global_unrealizedPL(), 2),
                                                           utils.KeepNDecimals(self.broker.get_global_unrealizedPL() * 100 / self.original_portfolio_value, 2))
        msg += "current cash = {}\n".format(utils.KeepNDecimals(self.broker.get_cash(), 2))
        usdt_equity = self.broker.get_usdt_equity()
        if self.previous_usdt_equity == 0:
            self.previous_usdt_equity = usdt_equity
        variation_percent = utils.get_variation(self.original_portfolio_value, usdt_equity)
        msg += "total SL TP: ${} / %{}\n".format(utils.KeepNDecimals(self.total_SL_TP, 2),
                                                 utils.KeepNDecimals(self.total_SL_TP_percent, 2))
        msg += "max drawdown: ${} / %{}\n".format(utils.KeepNDecimals(self.drawdown, 2),
                                                  utils.KeepNDecimals(self.actual_drawdown_percent, 2))
        msg += "account equity : ${} / %{}\n".format(utils.KeepNDecimals(usdt_equity, 2),
                                                   utils.KeepNDecimals(variation_percent, 2))
        variation_percent = utils.get_variation(self.previous_usdt_equity, usdt_equity)
        msg += "previous equity : ${} / ${} / %{}".format(utils.KeepNDecimals(self.previous_usdt_equity, 2),
                                                                   utils.KeepNDecimals(usdt_equity - self.previous_usdt_equity, 2),
                                                                   utils.KeepNDecimals(variation_percent, 2))
        self.previous_usdt_equity = usdt_equity
        self.log(msg.upper(), "end step".upper())

        self.tradetraces.export()

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
        try:
            sell_trade.trace_id = bought_trade.id
        except:
            print("DEBUG TRACES ERROR - NO bought_trade.id")
            sell_trade.trace_id = 0
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
        if (self.final_datetime and current_datetime >= self.final_datetime):
            # final step - force all the symbols to be sold
            df_selling_symbols = self.rtstr.get_df_forced_selling_symbols()
            self.exit = True
            self.flush_current_trade = True
        else:
            # identify symbols to sell
            df_selling_symbols = self.rtstr.get_df_selling_symbols(lst_symbols, self.rtstr.rtctrl.get_rtctrl_df_roi_sl_tp())

        list_symbols_to_sell = df_selling_symbols.symbol.to_list()
        df_selling_symbols.set_index("symbol", inplace=True)
        df_sell_performed = pd.DataFrame(columns=["symbol", "price", "roi%", "pos_type"])
        for current_trade in self.current_trades:
            if self.rtstr.is_open_type(current_trade.type) \
                    and current_trade.symbol in list_symbols_to_sell \
                    and self.flush_current_trade:
                sell_trade = self._prepare_sell_trade_from_bought_trade(current_trade, current_datetime)
                done = self.broker.execute_trade(sell_trade)
                if done:
                    self.sell_performed = True
                    current_trade.type = self.rtstr.get_close_type_and_close(current_trade.symbol)
                    self.cash = self.broker.get_cash()
                    sell_trade.cash = self.cash

                    self.traces_trade_total_closed += 1

                    self.tradetraces.set_sell(sell_trade.symbol, sell_trade.trace_id,
                                              sell_trade.symbol_price,
                                              sell_trade.gross_price,
                                              sell_trade.selling_fee,
                                              "CLOSURE")

                    self.current_trades.append(sell_trade)
                    df_sell_performed.loc[len(df_sell_performed.index)] = [sell_trade.symbol, round(sell_trade.gross_price, 2), round(sell_trade.roi, 2), sell_trade.type]

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
        symbols_bought = {"symbol":[], "size":[], "percent":[], "gross_price":[], "pos_type": []}
        for symbol in df_buying_symbols.index.to_list():
            print("buying symbol: ", symbol)
            current_trade = trade.Trade(current_datetime)
            # current_trade.type = self.rtstr.get_open_type(symbol)
            current_trade.type = df_buying_symbols['pos_type'][symbol]
            current_trade.symbol = symbol

            current_trade.symbol_price = self.broker.get_value(symbol)
            current_trade.buying_price = current_trade.symbol_price

            current_trade.commission = self.broker.get_commission(current_trade.symbol)
            current_trade.minsize = self.broker.get_minimum_size(current_trade.symbol)

            current_trade.gross_size = df_buying_symbols["size"][symbol]  # Gross size
            current_trade.gross_price = current_trade.gross_size * current_trade.symbol_price

            # TMP HACK CEDE
            if abs(round(current_trade.gross_price, 4)) >= round(self.cash, 4):
                print("=========== > gross price do not fit cash value:")
                print("===========================================================================")
                print('cash', self.cash)
                print('gross_price', current_trade.gross_price)
                current_trade.gross_price = self.cash - self.cash * 0.1
                current_trade.gross_size = current_trade.gross_price / current_trade.buying_price
                print("===========================================================================")

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

                    self.tradetraces.add_new_entry(current_trade.symbol, current_trade.id, current_trade.clientOid,
                                                   current_trade.gross_size, current_trade.buying_price,
                                                   current_trade.bought_gross_price, current_trade.buying_fee)

                    self.current_trades.append(current_trade)

                    symbols_bought["symbol"].append(current_trade.symbol)
                    symbols_bought["size"].append(utils.KeepNDecimals(current_trade.gross_size))
                    symbols_bought["percent"].append(utils.KeepNDecimals(df_buying_symbols["percent"][current_trade.symbol]))
                    symbols_bought["gross_price"].append(utils.KeepNDecimals(current_trade.gross_price))
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
            self.rtstr.rtctrl.display_summary_info(True)
            self.exit = True

    def export_status(self):
        return self.broker.export_status()

    def backup(self):
        with open(self.backup_filename, 'wb') as file:
            # print("[crah::backup]", self.backup_filename)
            pickle.dump(self, file)

    def merge_current_trades_from_symbol(self, current_trades, symbol, current_datetime, position_type):
        lst_trades = []

        lst_current_trade_symbol_price = []
        lst_current_trade_buying_price = []
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

    def create_directory(self, directory_path):
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            print(f"Directory '{directory_path}' created successfully.")
        else:
            print(f"Directory '{directory_path}' already exists.")

    def save_df_csv_broker_current_state(self, output_dir, current_state):
        self.create_directory(output_dir)
        # Get the current date and time
        current_date = datetime.now()

        # Format the date as a string
        formatted_date = current_date.strftime("%Y%m%d_%H%M%S")

        df_current_states = current_state["open_orders"]
        df_open_positions = current_state["open_positions"]
        df_price = current_state["prices"]

        filename_df_current_states = f"data_{formatted_date}_df_current_states.csv"
        full_path = os.path.join(output_dir, filename_df_current_states)
        df_current_states.to_csv(full_path)

        filename_df_open_positions = f"data_{formatted_date}_df_open_positions.csv"
        full_path = os.path.join(output_dir, filename_df_open_positions)
        df_open_positions.to_csv(full_path)

        filename_df_price = f"data_{formatted_date}_df_price.csv"
        full_path = os.path.join(output_dir, filename_df_price)
        df_price.to_csv(full_path)


    def get_current_state_from_csv(self, input_dir, cpt, df_orders, df_grids):
        exit_scenario = False
        str_cpt = str(cpt)

        if False: # CEDE SCENARIOS DATA IF NEEDED
            filename = "_df_open_positions.csv"
            utils.modify_strategy_data_files(input_dir, filename)
            exit(1)

        full_path = os.path.join(input_dir, "data_" + str_cpt + "_df_current_states.csv")
        file_path = Path(full_path)
        if file_path.exists():
            df_open_orders = pd.read_csv(full_path)
        else:
            exit_scenario = True

        full_path = os.path.join(input_dir, "data_" + str_cpt + "_df_open_positions.csv")
        file_path = Path(full_path)
        if file_path.exists():
            df_open_positions = pd.read_csv(full_path)
        else:
            exit_scenario = True

        full_path = os.path.join(input_dir, "data_" + str_cpt + "_df_price.csv")
        file_path = Path(full_path)
        if file_path.exists():
            df_prices = pd.read_csv(full_path)
        else:
            exit_scenario = True

        if exit_scenario:
            print("SCENARIO COMPLETED AT ROUND ", str_cpt)
            full_path = os.path.join(input_dir, "results_scenario_grid_df_current_states.csv")
            df_orders.to_csv(full_path)
            full_path = os.path.join(input_dir, "baseline_" + "results_scenario_grid_df_current_states.csv")
            file_path = Path(full_path)
            if file_path.exists():
                df_baseline = pd.read_csv(full_path, index_col=False)
                df_baseline = df_baseline.loc[:, ~df_baseline.columns.str.match('Unnamed')]
                # Check if DataFrames are identical
                identical = df_baseline.equals(df_orders)
                if identical:
                    print("ORDERS MATCHING 100%")
                else:
                    print("ORDERS NOT MATCHING")
            else:
                print("NO ORDER BASELINE AVAILABLE FOR THIS SCENARIO")

            full_path = os.path.join(input_dir, "results_scenario_grid_df_grids.csv")
            df_grids.to_csv(full_path)
            full_path = os.path.join(input_dir, "baseline_" + "results_scenario_grid_df_grids.csv")
            file_path = Path(full_path)
            if file_path.exists():
                df_baseline = pd.read_csv(full_path, index_col=False)
                # df_baseline = df_baseline.drop(columns=df_baseline.columns[0])
                df_baseline = df_baseline.loc[:, ~df_baseline.columns.str.match('Unnamed')]
                df_baseline = df_baseline.fillna('')
                # Check if DataFrames are identical
                identical = df_baseline.equals(df_grids)
                if identical:
                    print("GRIDS MATCHING 100%")
                else:
                    print("GRIDS NOT MATCHING")
            else:
                print("NO GRID BASELINE AVAILABLE FOR THIS SCENARIO")
            exit(0)

        broker_current_state = {
            "open_orders": df_open_orders,
            "open_positions": df_open_positions,
            "prices": df_prices
        }
        return broker_current_state


    def udpate_strategy_with_broker_current_state_scenario(self, scenario_id):
        cpt = 0
        input_dir = "./grid_test/" + str(scenario_id) + "_scenario_test"
        df_scenario_results_global = pd.DataFrame()
        df_grid_record = pd.DataFrame()
        df_grid_global = df_grid_record
        while True:
            self.start_time_grid_strategy = time.time()
            if self.start_time_grid_strategy_init == None:
                symbols = ["XRP"]
                self.start_time_grid_strategy_init = self.start_time_grid_strategy
                self.grid_iteration = 1
                df_symbol_minsize = self.broker.get_df_minimum_size(symbols)
                df_buying_size = self.rtstr.set_df_buying_size_scenario(df_symbol_minsize, self.broker.get_usdt_equity())
                df_buying_size_normalise = self.broker.normalize_grid_df_buying_size_size(df_buying_size)
                self.rtstr.set_df_normalize_buying_size(df_buying_size_normalise)
                self.rtstr.set_normalized_grid_price(self.broker.get_price_place_endstep(symbols))
            else:
                self.grid_iteration += 1

            break_pt = 4
            if cpt == break_pt:
                print("toto")
                pass
            print("cpt start: ", cpt)
            self.start_time_grid_strategy = time.time()
            broker_current_state = self.get_current_state_from_csv(input_dir, cpt, df_scenario_results_global, df_grid_global)
            lst_orders_to_execute = self.rtstr.set_broker_current_state(broker_current_state)

            if len(lst_orders_to_execute) > 0:
                df_scenario_results = pd.DataFrame(lst_orders_to_execute)
                df_scenario_results["round"] = cpt
                column_to_move = 'round'
                first_column = df_scenario_results.pop(column_to_move)  # Remove column 'C' from DataFrame
                df_scenario_results.insert(0, column_to_move, first_column)
                if len(df_scenario_results_global) == 0:
                    df_scenario_results_global = df_scenario_results
                else:
                    df_scenario_results_global = pd.concat([df_scenario_results_global, df_scenario_results], ignore_index=True)
            df_grid_record = self.rtstr.get_grid(cpt)
            df_grid_global = pd.concat([df_grid_global, df_grid_record], ignore_index=True)

            memory_used_bytes = utils.get_memory_usage()
            if self.memory_used_mb == 0:
                self.init_memory_used_mb = memory_used_bytes / (1024 * 1024)
                self.memory_used_mb = self.init_memory_used_mb
            else:
                self.memory_used_mb = memory_used_bytes / (1024 * 1024)  # Convert bytes to megabytes
            print("output lst_orders_to_execute: ", lst_orders_to_execute)
            msg = self.rtstr.get_info_msg_status()
            if msg != None:
                current_datetime = datetime.today().strftime("%Y/%m/%d - %H:%M:%S")
                msg = current_datetime + "\n" + msg
                msg += "CPT: " + str(cpt) + "\n"
                usdt_equity = self.broker.get_usdt_equity()
                msg += "- USDT EQUITY: " + str(round(usdt_equity, 2)) + " - PNL: " + str(round(self.broker.get_global_unrealizedPL(), 2)) + "\n"
                msg += "AVERAGE RUN TIME: " + str(self.average_time_grid_strategy) + "s\n"
                end_time = time.time()
                msg += "DURATION: " + utils.format_duration(round((end_time - self.start_time_grid_strategy_init), 2)) + "\n"
                delta_memory = self.memory_used_mb - self.init_memory_used_mb
                if delta_memory >= 0:
                    msg += f"MEMORY: {self.memory_used_mb:.1f}MB" + " (+" + str(round(delta_memory, 1)) + ")\n"
                else:
                    msg += f"MEMORY: {self.memory_used_mb:.1f}MB" + " (-" + str(round(abs(delta_memory), 1)) + ")\n"
                self.log(msg, "GRID STATUS")
            end_time = time.time()
            self.iteration_times_grid_strategy.append(end_time - self.start_time_grid_strategy)
            self.iteration_times_grid_strategy = self.iteration_times_grid_strategy[-10:]
            self.average_time_grid_strategy = round(sum(self.iteration_times_grid_strategy) / len(self.iteration_times_grid_strategy), 2)
            self.average_time_grid_strategy_overall = round((end_time - self.start_time_grid_strategy_init) / self.grid_iteration, 2)
            print("GRID ITERATION AVERAGE TIME: " + str(self.average_time_grid_strategy) + " seconds")
            print("CRAG AVERAGE TIME: " + str(self.average_time_grid_strategy_overall) + " seconds")
            print("OVERALL DURATION: ", utils.format_duration(round((end_time - self.start_time_grid_strategy_init), 2)))
            print("ITERATIONS: ", self.grid_iteration)
            print("MEMORY: ", round(self.memory_used_mb, 2), "MB DELTA: " + str(round(self.memory_used_mb - self.init_memory_used_mb,2)) + " MB" + "\n")

            if cpt == break_pt:
                print(cpt)
                pass
            print("cpt end: ", cpt)
            cpt += 1

    def get_memory_usage(self, class_memory):
        memory_usage = {}
        for attr_name, attr_value in vars(class_memory).items():
            # memory_usage[attr_name] = sys.getsizeof(attr_value)
            memory_usage[attr_name] = asizeof.asizeof(attr_value)
        return memory_usage

    def print_memory_usage(self, id, size_init, size_previous, size):
        if size_previous == 0:
            return
        for (key1, value1), (key2, value2), (key3, value3) in zip(size_init.items(), size_previous.items(), size.items()):
            if (((value3 - value1) > 0)
                or ((value3 - value2) > 0))\
                    and not("_size" in key1):
                print("MEMORY ", id, " - ", key1, " VALUE: ", value3, " PREV DIFF: ",  value3 - value2, " INIT DIFF: ", value3 - value1)

    def udpate_strategy_with_broker_current_state(self):
        GRID_SCENARIO_ON = False
        SCENARIO_ID = 6
        if GRID_SCENARIO_ON:
            self.udpate_strategy_with_broker_current_state_scenario(SCENARIO_ID)
        else:
            self.udpate_strategy_with_broker_current_state_live()

    def udpate_strategy_with_broker_current_state_live(self):
        self.start_time_grid_strategy = time.time()
        if self.start_time_grid_strategy_init == None:
            self.start_time_grid_strategy_init = self.start_time_grid_strategy
            self.grid_iteration = 1
        else:
            self.grid_iteration += 1

        symbols = self.rtstr.lst_symbols
        broker_current_state = self.broker.get_current_state(symbols)
        if self.init_grid_position:
            self.init_grid_position = False
            df_symbol_minsize = self.broker.get_df_minimum_size(symbols)
            df_buying_size = self.rtstr.set_df_buying_size(df_symbol_minsize, self.broker.get_usdt_equity())
            df_buying_size_normalise = self.broker.normalize_grid_df_buying_size_size(df_buying_size)
            self.rtstr.set_df_normalize_buying_size(df_buying_size_normalise)
            self.rtstr.set_normalized_grid_price(self.broker.get_price_place_endstep(symbols))
            lst_orders_to_execute = self.rtstr.activate_grid(broker_current_state)
            lst_orders_to_execute = []
            self.broker.execute_orders(lst_orders_to_execute)
            self.broker.reset_current_postion(broker_current_state)
            broker_current_state = self.broker.get_current_state(symbols)

        lst_orders_to_execute = self.rtstr.set_broker_current_state(broker_current_state)
        memory_used_bytes = utils.get_memory_usage()
        if self.memory_used_mb == 0:
            self.init_memory_used_mb = memory_used_bytes / (1024 * 1024)
            self.memory_used_mb = self.init_memory_used_mb
        else:
            self.memory_used_mb = memory_used_bytes / (1024 * 1024)  # Convert bytes to megabytes
        msg = self.rtstr.get_info_msg_status()
        if msg != None:
            current_datetime = datetime.today().strftime("%Y/%m/%d - %H:%M:%S")
            msg = current_datetime + "\n" + msg
            usdt_equity = self.broker.get_usdt_equity()
            # wallet_equity = self.broker.get_wallet_equity()
            msg += "# STATUS EQUITY:" + "\n"
            # msg += "WALLET EQUITY: " + str(round(wallet_equity, 2)) + " - PNL: " + str(round(self.broker.get_global_unrealizedPL(), 2)) + "\n"
            msg += "**USDT: " + str(round(usdt_equity, 2)) + " %: " + str(round(self.total_SL_TP * 100 / self.original_portfolio_value, 2)) + "**\n"
            msg += "**INITIAL: " + str(round(self.original_portfolio_value, 2)) + " $: " + str(round(self.total_SL_TP, 2)) + "**\n"
            msg += "MAX: " + str(round(self.maximal_portfolio_value, 2)) \
                   + " $: " + str(round(self.maximal_portfolio_value - self.original_portfolio_value, 2)) \
                   + " %: " + str(round((self.maximal_portfolio_value - self.original_portfolio_value) * 100 / self.original_portfolio_value, 2)) + "\n"
            msg += "MIN: " + str(round(self.minimal_portfolio_value, 2)) \
                   + " $: " + str(round(self.minimal_portfolio_value - self.original_portfolio_value, 2)) \
                   + " %: " + str(round((self.minimal_portfolio_value - self.original_portfolio_value) * 100 / self.original_portfolio_value, 2)) + "\n"
            lst_usdt_symbols = self.broker.get_lst_symbol_position()
            if len(symbols) == len(lst_usdt_symbols):
                for symbol, usdt_symbol in zip(symbols, lst_usdt_symbols):
                    total, available, leverage, averageOpenPrice, marketPrice, unrealizedPL, liquidation, side= self.broker.get_symbol_data(usdt_symbol)
                    msg += "# SYMBOL " + symbol + " :\n"
                    msg += "**market price: " + str(round(marketPrice, 4)) + "**\n"
                    msg += "**price average: " + str(round(averageOpenPrice, 4)) + "**\n"
                    msg += "**EQUITY: " + str(round(total * averageOpenPrice, 2)) + " PNL: " + str(round(unrealizedPL, 2)) + "**\n"
                    msg += "SIZE: " + str(round(total, 2)) + " leverage: " + str(round(leverage, 2)) + "\n"
                    msg += "side: " + side + " liquidation: " + str(round(liquidation, 2)) + "\n"
            else:
                if (len(symbols) != 0) and (len(lst_usdt_symbols) == 0):
                    for symbol in symbols:   # CEDE NOT WORKING FOR MULTI
                        msg += "# SYMBOL " + symbol + " :\n"
                        msg += "**no positions engaged" + "**\n"
                        df_price = broker_current_state["prices"]
                        price_for_symbol = df_price.loc[df_price['symbols'] == symbol, 'values'].values[0]
                        msg += "**market price: " + str(round(price_for_symbol, 4)) + "**\n"
                else:
                    print("ERROR LST SYMBOLS NOT MATCHING")
                    print("symbols", symbols)
                    print("lst_usdt_symbols", lst_usdt_symbols)
                    msg += "**ERROR LST SYMBOLS NOT MATCHING" + "**:\n"
            msg += "# TIME & MEMORY" + "\n"
            msg += "CRAG TIME: " + str(self.average_time_grid_strategy_overall) + "s\n"
            msg += "GRID TIME: " + str(self.average_time_grid_strategy) + "s\n"
            end_time = time.time()
            msg += "DURATION: " + utils.format_duration(round((end_time - self.start_time_grid_strategy_init), 2)) + "\n"
            delta_memory = self.memory_used_mb - self.init_memory_used_mb
            if delta_memory >= 0:
                msg += f"MEMORY: {self.memory_used_mb:.1f}MB" + " (+" + str(round(delta_memory,1)) + ")\n"
            else:
                msg += f"MEMORY: {self.memory_used_mb:.1f}MB" + " (-" + str(round(abs(delta_memory),1)) + ")\n"
            msg = msg.upper()
            self.log(msg, "GRID STATUS")

        if not self.zero_print:
            print("output lst_orders_to_execute: ", lst_orders_to_execute)

        self.broker.execute_orders(lst_orders_to_execute)

        msg_broker_trade_info = self.broker.log_info_trade()
        if len(msg_broker_trade_info) != 0:
            self.log(msg_broker_trade_info, "broker trade")
            self.broker.clear_log_info_trade()

        end_time = time.time()
        self.iteration_times_grid_strategy.append(end_time - self.start_time_grid_strategy)
        self.iteration_times_grid_strategy = self.iteration_times_grid_strategy[-10:]
        self.average_time_grid_strategy = round(sum(self.iteration_times_grid_strategy) / len(self.iteration_times_grid_strategy), 2)
        self.average_time_grid_strategy_overall = round((end_time - self.start_time_grid_strategy_init) / self.grid_iteration, 2)

        # if not self.zero_print:
        if True: # CEDE FOR DEBUG
            # CEDE MEASURE RUN TIME IN ORDER TO BENCHMARK PC VS RASPBERRY
            print("GRID ITERATION AVERAGE TIME:  " + str(self.average_time_grid_strategy) + " seconds")
            print("CRAG AVERAGE TIME:            " + str(self.average_time_grid_strategy_overall) + " seconds")
            print("OVERALL DURATION:             ", utils.format_duration(round((end_time - self.start_time_grid_strategy_init), 2)))
            print("ITERATIONS:                   ", self.grid_iteration)
            print("MEMORY:                       ", round(self.memory_used_mb, 2), "MB - DELTA: " + str(round(self.memory_used_mb - self.init_memory_used_mb,2)) + " MB" + "\n")

            if self.init_debug_memory:
                self.crag_size_previous = self.crag_size
                self.crag_size = self.get_memory_usage(self)
                if self.crag_size_init == 0:
                    self.crag_size_init = self.crag_size
                self.print_memory_usage("CRAG", self.crag_size_init, self.crag_size_previous, self.crag_size)

                self.rtstr_size_previous = self.rtstr_size
                self.rtstr_size = self.get_memory_usage(self.rtstr)
                if self.rtstr_size_init == 0:
                    self.rtstr_size_init = self.rtstr_size
                self.print_memory_usage("rtstr", self.rtstr_size_init, self.rtstr_size_previous, self.rtstr_size)

                self.broker_size_previous = self.broker_size
                self.broker_size = self.get_memory_usage(self.broker)
                if self.broker_size_init == 0:
                    self.broker_size_init = self.broker_size
                self.print_memory_usage("broker", self.broker_size_init, self.broker_size_previous, self.broker_size)

                self.rtstr_grid_size_previous = self.rtstr_grid_size
                self.rtstr_grid_size = self.get_memory_usage(self.rtstr.grid)
                if self.rtstr_grid_size_init == 0:
                    self.rtstr_grid_size_init = self.rtstr_grid_size
                self.print_memory_usage("rtstr_grid", self.rtstr_grid_size_init, self.rtstr_grid_size_previous, self.rtstr_grid_size)
            else:
                self.init_debug_memory = True

    def safety_step(self):
        usdt_equity = self.broker.get_usdt_equity()
        self.total_SL_TP = usdt_equity - self.original_portfolio_value
        if usdt_equity >= self.maximal_portfolio_value:
            self.maximal_portfolio_value = usdt_equity
            self.drawdown = 0
            self.actual_drawdown_percent = 0
        else:
            self.drawdown = usdt_equity - self.maximal_portfolio_value
            self.actual_drawdown_percent = self.drawdown * 100 / self.maximal_portfolio_value

        if self.original_portfolio_value == 0:
            self.total_SL_TP_percent = 0
        else:
            self.total_SL_TP_percent = self.total_SL_TP * 100 / self.original_portfolio_value

        if self.maximal_portfolio_value == 0:
            self.maximal_portfolio_value = max(usdt_equity, self.original_portfolio_value)
            self.actual_drawdown_percent = 0
        else:
            self.actual_drawdown_percent = self.drawdown * 100 / self.maximal_portfolio_value

        if self.rtstr.need_broker_current_state():
            # GRID TRADING STRATEGY
            self.udpate_strategy_with_broker_current_state()

        if self.rtstr.condition_for_global_SLTP(self.total_SL_TP_percent) \
                or self.rtstr.condition_for_global_trailer_TP(self.total_SL_TP_percent) \
                or self.rtstr.condition_for_global_trailer_SL(self.total_SL_TP_percent) \
                or self.rtstr.condition_for_max_drawdown_SL(self.actual_drawdown_percent):
            print('reset - global TP')
            print('total SL TP: $', self.total_SL_TP, "      ", self.total_SL_TP_percent, "%")
            print('max drawdown: $', self.drawdown, " - ", self.actual_drawdown_percent, "%")
            msg = "reset - total SL TP"
            msg += "total SL TP: ${} / %{}\n".format(utils.KeepNDecimals(self.total_SL_TP, 2),
                                                     utils.KeepNDecimals(self.total_SL_TP_percent, 2))
            msg += "max drawdown: ${} / %{}\n".format(utils.KeepNDecimals(self.drawdown, 2),
                                                      utils.KeepNDecimals(self.actual_drawdown_percent, 2))
            self.log(msg, "total SL TP")

            self.broker.execute_reset_account()
            return False

        if not self.rtstr.need_broker_current_state():
            # NOT AVAILABLE IN GRID TRADING STRATEGY
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
                        or self.rtstr.condition_trailer_TP(self.broker._get_coin(symbol), symbol_unrealizedPL_percent)\
                        or self.rtstr.condition_trailer_SL(self.broker._get_coin(symbol), symbol_unrealizedPL_percent):
                    lst_symbol_for_closure.append(symbol)

            if self.rtstr.trigger_high_volatility_protection():
                BTC_price = self.broker.get_value('BTC')
                current_datetime = datetime.now()
                current_timestamp = datetime.timestamp(current_datetime)
                equity = self.broker.get_usdt_equity()
                # ['datetime', 'timestamp', 'symbol', 'pice', 'pct_BTC', 'equity', 'pct_equity']
                lst_record_volatility = [current_datetime, current_timestamp, 'BTC', BTC_price, 0, equity, 0]
                self.rtstr.set_high_volatility_protection_data(lst_record_volatility)
                if self.rtstr.high_volatility_protection_activation(self.actual_drawdown_percent):
                    print("DUMP POSITIONS DUE TO HIGH VOLATILITY")
                    lst_symbol_for_closure = self.broker.get_lst_symbol_position()
                    self.maximal_portfolio_value

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
                            current_trade.type = self.rtstr.get_close_type_and_close(current_trade.symbol)
                            self.cash = self.broker.get_cash()
                            sell_trade.cash = self.cash

                            self.tradetraces.set_sell(sell_trade.symbol, sell_trade.trace_id,
                                                      current_trade.symbol_price,
                                                      current_trade.gross_price,
                                                      current_trade.selling_fee,
                                                      "SL-TP")

                            if sell_trade.roi < 0:
                                self.traces_trade_negative += 1
                            else:
                                self.traces_trade_positive += 1

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