import os
import sys

from pympler import asizeof
import shutil
import time
import pandas as pd
from . import rtstr,utils,logger,execute_time_recoder
from .toolbox import monitoring_helper
import pika
import json
# import ast
import threading
import pickle
import dill
from pathlib import Path
from datetime import datetime, timedelta
from datetime import date
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import threading
# import tracemalloc

import gc

# to launch crag as a rabbitmq receiver :
# > apt-get install rabbitmq-server
# > systemctl enable rabbitmq-server
# > systemctl start rabbitmq-server
# systemctl is not provided by WSL :
# > service rabbitmq-server start
# reference : https://medium.com/analytics-vidhya/how-to-use-rabbitmq-with-python-e0ccfe7fa959
class Crag:
    def __init__(self, params = None):
        self.safety_run = True
        self.broker = None
        self.rtstr = None
        self.working_directory = None
        self.logger = None
        self.loggers = []
        self.start_date = ""
        self.end_date = ""
        self.original_portfolio_value = 0
        self.minimal_portfolio_value = 0
        self.minimal_portfolio_date = ""
        self.maximal_portfolio_value = 0
        self.minimal_portfolio_variation = 0
        self.maximal_portfolio_variation = 0
        self.maximal_portfolio_date = ""
        self.id = str(utils.get_random_id())
        self.high_volatility_sleep_duration = 0
        self.activate_volatility_sleep = False
        self.drawdown = 0
        self.actual_drawdown_percent = 0
        self.total_SL_TP = 0
        self.total_SL_TP_percent = 0
        self.monitoring = monitoring_helper.SQLMonitoring("ovh_mysql")
        self.init_grid_position = True
        self.start_time_grid_strategy = None
        self.iteration_times_grid_strategy = []
        self.average_time_grid_strategy = 0
        self.average_time_grid_strategy_overall = 0
        self.start_time_grid_strategy_init = None
        self.grid_iteration = 0
        self.symbols = []

        self.lst_ds = None
        self.lst_symbol = None

        self.crag_size_previous = 0
        self.crag_size = 0
        self.crag_size_init = 0
        self.rtstr_size_previous = 0
        self.rtstr_size = 0
        self.rtstr_size_init = 0
        self.broker_size_previous = 0
        self.broker_size = 0
        self.broker_size_init = 0
        self.rtstr_grid_size_previous = 0
        self.rtstr_grid_size = 0
        self.rtstr_grid_size_init = 0
        self.memory_used_mb = 0
        self.init_memory_used_mb = 0
        self.init_market_price = 0
        self.market_price_max = 0
        self.market_price_min = 0

        self.usdt_equity = 0
        self.usdt_equity_previous = 0
        self.usdt_equity_thread = 0
        self.usdt_available_thread = 0

        self.resume = False
        self.safety_step_iterration = 0
        self.safety_step_iterations_max = None
        self.sum_duration_safety_step = 0
        self.reboot_iter = 0
        self.reboot_exception = False
        self.previous_start_reboot = datetime.now()
        self.init_start_date = int(time.time())
        self.total_duration_reboot = 0
        self.cpt_reboot = 0
        self.average_duration_reboot = 0
        self.max_duration_reboot = 0
        self.min_duration_reboot = 0
        self.dump_perf_dir = "./dump_timer"

        if params:
            self.broker = params.get("broker", self.broker)
            if self.broker:
                self.original_portfolio_value = self.broker.get_portfolio_value()
                self.minimal_portfolio_value = self.original_portfolio_value
                self.maximal_portfolio_value = self.original_portfolio_value
            self.rtstr = params.get("rtstr", self.rtstr)
            self.logger_discord = params.get("logger", self.logger)
            self.loggers = params.get("loggers", self.loggers)
            self.working_directory = params.get("working_directory", self.working_directory)
            self.id = params.get("id", self.id)
            self.working_directory = params.get("working_directory", self.working_directory)
            self.dump_perf_dir = self.dump_perf_dir + "_" + self.id
            self.safety_step_iterations_max = params.get("safety_step_iterations_max", self.safety_step_iterations_max)

        self.zero_print = False

        if self.rtstr:
            self.strategy_name = self.rtstr.get_info()
        if self.broker:
            self.final_datetime = self.broker.get_final_datetime()
            self.start_date, self.end_date, _ = self.broker.get_info() # CEDE To be confirmed
        self.export_filename = "sim_broker_history" + "_" + self.strategy_name + "_" + str(self.start_date) + "_" + str(self.end_date) + ".csv"
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

        self.reboot_timer_continue = False

        self.lock_usdt_equity_thread = threading.Lock()

        self.last_execution = {
            '1m': None,
            '5m': None,
            '15m': None,
            '30m': None,
            '1h': None,
            '2h': None,
            '4h': None,
            'reboot': None
        }

        # rabbitmq connection
        self.send_alive_notification()

        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))
            channel = connection.channel()
            channel.queue_declare(queue="StrategyMonitoring")

            def callback(ch, method, properties, bbody):
                self.log(" [x] Received {}".format(bbody))
                body = bbody.decode()
                body = json.loads(body)
                if body and body["id"] == "command" and body["strategy_id"] == self.rtstr.id:
                    command = body["command"]
                    if command == "stop":
                        self.log("stopping {}".format(self.rtstr.id))
                        os._exit(0)

                if body == "history" or body == "stop":
                    # self.export_history(self.export_filename)
                    self.log_discord(msg="> {}".format(self.export_filename), header="{}".format(body), attachments=[self.export_filename])
                    os.remove(self.export_filename)
                    if body == "stop":
                        os._exit(0)
                else:
                    self.log_discord(msg="> {} : unknown message".format(body))

            channel.basic_consume(queue="StrategyMonitoring", on_message_callback=callback, auto_ack=True)
            
            #channel.start_consuming()
            thread = threading.Thread(name='t', target=channel.start_consuming, args=())
            thread.setDaemon(True)
            thread.start()
        except:
            self.log("Problem encountered while configuring the rabbitmq receiver")

    def log(self, msg, header="", attachments=[]):
        if self.zero_print:
            return
        for iter_logger in self.loggers:
            iter_logger.log(msg, header="["+self.id+"] "+header, author=type(self).__name__, attachments=attachments)

    def log_discord(self, msg, header="", attachments=None):
        if attachments is None:
            attachments = []
        if self.logger_discord:
            self.logger_discord.log(msg, header="["+self.id+"] "+header, author=type(self).__name__, attachments=attachments)

    def send_alive_notification(self):
        if self.broker and self.broker.account and self.rtstr:
            current_datetime = datetime.now()
            current_timestamp = datetime.timestamp(current_datetime)
            self.monitoring.send_alive_notification(current_timestamp, self.broker.account.get("id"), self.rtstr.id)

    def run(self):
        self.safety_step_iterration = 0

        if not self.resume:
            self.start_date = self.broker.get_current_datetime("%Y/%m/%d %H:%M:%S")
            self.minimal_portfolio_date = self.start_date
            self.maximal_portfolio_date = self.start_date

            msg_broker_info = self.broker.log_info()
            self.log_discord(msg_broker_info, "run")
            del msg_broker_info
        else:
            msg = "REBOOT STARTEGY" + "\n"
            start_reboot = datetime.now()
            self.previous_start_reboot = start_reboot
            msg += "AT: " + start_reboot.strftime("%Y/%m/%d %H:%M:%S")
            self.log_discord(msg, "born again")
            self.safety_step_iterration = 0
            self.sum_duration_safety_step = 0
            del msg
            self.reboot_iter += 1


        # get usdt_euqity to run_in_background
        if self.broker.get_ws_activation_status():
            self.usdt_equity, self.usdt_available = self.broker.get_usdt_equity_available()
        else:
            self.usdt_equity_thread, self.usdt_available_thread = self.broker.get_usdt_equity_available()

            t = threading.Thread(target=self.update_status_for_TPSL, daemon=True)
            t.start()

        self.init_master_memory = utils.get_memory_usage()
        self.init_start_time = datetime.now()
        self.resume = True

        while True:

            step_result = self.safety_step()

            # self.execute_timer.set_end_time("crag", "run", "safety_step", self.main_cycle_safety_step)
            # self.execute_timer.set_time_to_zero("crag", "run", "step", self.main_cycle_safety_step)
            # self.main_cycle_safety_step += 1

            self.request_backup()

            if not step_result:
                self.log("safety_step result exit")
                os._exit(0)

            if self.rtstr.get_strategy_type() == "INTERVAL":
                now = datetime.now()
                lst_interval = []
                # Check if it's time to execute the 1 minute function at exact minutes
                if now.second == 0 and (
                        self.last_execution['1m'] is None or (now - self.last_execution['1m']).seconds >= 60):
                    lst_interval = ['1m']
                    self.last_execution['1m'] = now

                # Check if it's time to execute the 5 minute function at 0, 5, 10, 15, ..., 55 minutes
                if now.minute % 5 == 0 and now.second == 0 and (
                        self.last_execution['5m'] is None or (now - self.last_execution['5m']).seconds >= 300):
                    lst_interval = ['1m', '5m']
                    self.last_execution['5m'] = now

                # Check if it's time to execute the 15 minute function at 0, 15, 30, 45 minutes
                if now.minute % 15 == 0 and now.second == 0 and (
                        self.last_execution['15m'] is None or (now - self.last_execution['15m']).seconds >= 900):
                    lst_interval = ['1m', '5m', '15m']
                    self.last_execution['15m'] = now

                # Check if it's time to execute the 30 minute function at 0, 30 minutes
                if now.minute % 30 == 0 and now.second == 0 and (
                        self.last_execution['30m'] is None or (now - self.last_execution['30m']).seconds >= 1800):
                    lst_interval = ['1m', '5m', '15m', '30m']
                    self.last_execution['30m'] = now

                # Check if it's time to execute the 1 hour function at 0 minutes of each hour
                if now.minute == 0 and now.second == 0 and (
                        self.last_execution['1h'] is None or (now - self.last_execution['1h']).seconds >= 3600):
                    lst_interval = ['1m', '5m', '15m', '30m', '1h']
                    self.last_execution['1h'] = now

                # Check if it's time to execute the 2 hour function at 0h, 2h, 4h, 6h, etc.
                if now.hour % 2 == 0 and now.minute == 0 and now.second == 0 and (
                        self.last_execution['2h'] is None or (now - self.last_execution['2h']).seconds >= 7200):
                    lst_interval = ['1m', '5m', '15m', '30m', '1h', '2h']
                    self.last_execution['2h'] = now

                # Check if it's time to execute the 4 hour function at 0h, 4h, 8h, 12h, etc.
                if now.hour % 4 == 0 and now.minute == 0 and now.second == 0 and (
                        self.last_execution['4h'] is None or (now - self.last_execution['4h']).seconds >= 14400):
                    lst_interval = ['1m', '5m', '15m', '30m', '1h', '2h', '4h']
                    self.last_execution['4h'] = now
                if lst_interval:
                    print("############################ ", lst_interval, " ############################")
                    self.step(lst_interval)

                # --- Reboot Timer Check ---
                # Trigger the reboot timer every 2 hours at x:30:20.
                # Here we use a 'reboot' key in self.last_execution to prevent multiple triggers within the same window.
                now = datetime.now()
                if now.minute == 30 and 20 <= now.second <= 25 and (
                        self.last_execution.get('reboot') is None or (now - self.last_execution['reboot']).seconds >= 7200):
                    self.reboot_timer_continue = True
                    self.last_execution['reboot'] = now
                    print("############################ Reboot Timer triggered at", now, "############################")
                else:
                    self.reboot_timer_continue = False

    def step(self, lst_interval):
        if self.broker.get_ws_activation_status():
            self.usdt_equity, self.usdt_available = self.broker.get_usdt_equity_available()
        else:
            self.usdt_equity = self.usdt_equity_thread
            self.usdt_available = self.usdt_available_thread
        self.send_alive_notification()
        stop = self.monitoring.get_strategy_stop(self.rtstr.id)
        if stop:
            self.monitoring.send_strategy_stopped(self.rtstr.id)
            os._exit(100)
        # prices_symbols, lst_ds = self.get_ds_and_price_symbols(lst_interval)

        lst_ds = self.get_ds(lst_interval)

        measure_time_fdp_start = datetime.now()

        lst_current_data = self.deduplicate_and_get_current_data(lst_ds)

        measure_time_fdp_end = datetime.now()
        self.log("measure time fdp: {}".format(measure_time_fdp_end - measure_time_fdp_start))
        self.log("[Crag] ⌛")
        self.log("[Execute Trade - Crag] ⌛")

        self.update_sltp_status()
        df_current_prices_symbols = self.get_price_symbols()
        self.rtstr.set_current_data(lst_current_data, df_current_prices_symbols, self.usdt_available)

        # execute strategy trades
        lst_trades_to_execute = self.rtstr.get_lst_trade(lst_interval)
        if len(lst_trades_to_execute) > 0:
            lst_trades_to_execute_result = self.broker.execute_trades(lst_trades_to_execute)
            self.rtstr.update_executed_trade_status(lst_trades_to_execute_result)
            lst_stat = self.rtstr.get_strategy_stats(lst_interval)

            for stat in lst_stat:
                if isinstance(stat, str):
                    stat = stat.replace('_', ' ').replace(',', '').replace('{', '').replace('}', '').replace('"', '').strip()
                    self.log_discord(stat.upper(), "STRATEGY STAT:")

            df_open_positions = self.broker.get_open_position()
            if not df_open_positions.empty:
                df_open_positions['symbol'] = df_open_positions['symbol'].str.replace('USDT_UMCBL', '')
                df_open_positions.rename(columns={'holdSide': 'side'}, inplace=True)
                df_open_positions.rename(columns={'leverage': 'lev'}, inplace=True)
                df_open_positions.rename(columns={'total': 'tot'}, inplace=True)
                df_open_positions.rename(columns={'usdtEquity': 'equ'}, inplace=True)
                df_open_positions.rename(columns={'marketPrice': 'price'}, inplace=True)
                df_open_positions.rename(columns={'unrealizedPL': 'PL'}, inplace=True)
                df_open_positions.rename(columns={'liquidationPrice': 'liq'}, inplace=True)

                msg = "end step with {} open position\n".format(len(df_open_positions))
                df_open_positions = df_open_positions.drop(columns=['marginCoin', 'achievedProfits'])
                df = df_open_positions.copy()
                df = df[["symbol", "side", "equ"]]
                msg_df = df.to_string(index=False) + "\n" + "\n"
                df = df_open_positions.copy()
                df = df[["symbol", "side", "lev", "tot"]]
                msg_df += df.to_string(index=False) + "\n" + "\n"
                df = df_open_positions.copy()
                df = df[["symbol", "side", "PL", "liq"]]
                msg_df += df.to_string(index=False) + "\n" + "\n"

                self.log_discord(msg_df.upper(), msg)

    def export_history(self, target=None):
        self.broker.export_history(target)

    def backup(self):
        """
        with open(self.backup_filename, 'wb') as file:
            self.log("[crag::backup]", self.backup_filename)
            dill.dump(self, file)
        """
        with open(self.backup_filename, 'wb') as file:
            self.log("[crag::backup]", self.backup_filename)
            try:
                pickle.dump(self, file)
            except:
                print("pickle does not exist: ", file)
                exit(876)

            if os.path.exists(self.backup_filename):
                print("File exists: ", self.backup_filename)
            else:
                print("File does not exist: ", self.backup_filename)


    def request_backup(self):
        self.safety_step_iterration += 1
        if self.safety_step_iterations_max is None:
            return

        if self.reboot_exception \
                or self.reboot_timer_continue \
                or (self.safety_step_iterration > self.safety_step_iterations_max):
            delta_memory_used = round((utils.get_memory_usage() - self.init_master_memory) / (1024 * 1024), 2)
            memory_usage = round(utils.get_memory_usage() / (1024 * 1024), 1)

            # Current time
            current_time = datetime.now()
            time_difference = current_time - self.init_start_time
            average_time = time_difference / self.safety_step_iterration
            average_seconds = average_time.total_seconds()

            print("****************** memory: ", memory_usage, " ******************")
            print("****************** delta memory: ", delta_memory_used, " ******************")
            # print("****************** Crag size: ", sys.getsizeof(self), " ******************")
            print("****************** cycle duration: ", utils.format_duration(time_difference.total_seconds()) ," ******************")
            print("****************** average: ", round(average_seconds, 2)," ******************")
            print("****************** iter : ", self.safety_step_iterration," ******************")

            self.msg_backup = "REBOOT TRIGGERED" + "\n"
            start_reboot = current_time
            self.msg_backup += "at: " + start_reboot.strftime("%Y/%m/%d %H:%M:%S") + "\n"
            duration_sec = (start_reboot - self.previous_start_reboot).total_seconds()
            self.msg_backup += "since reboot: " + utils.format_duration(duration_sec) + "\n"
            self.total_duration_reboot += duration_sec
            self.cpt_reboot += 1
            self.average_duration_reboot = self.total_duration_reboot / self.cpt_reboot
            self.max_duration_reboot = max(self.max_duration_reboot, duration_sec)
            if self.min_duration_reboot == 0:
                self.min_duration_reboot = duration_sec
            else:
                self.min_duration_reboot = min(self.min_duration_reboot, duration_sec)
            self.msg_backup += "max: " + utils.format_duration(self.max_duration_reboot) + "\n"
            self.msg_backup += "min: " + utils.format_duration(self.min_duration_reboot) + "\n"
            self.msg_backup += "average: " + utils.format_duration(self.average_duration_reboot) + "\n"
            self.msg_backup += "reboots: " + str(int(self.cpt_reboot)) + "\n"

            duration = int(time.time()) - self.init_start_date
            self.msg_backup += "DURATION: " + utils.format_duration(duration) + "\n"
            del duration

            self.broker.send_zed_shutdown()

            self.log_discord(self.msg_backup.upper(), "REBOOT STATUS")
            self.backup()
            raise SystemExit(self.msg_backup)

    def save_df_csv_broker_current_state(self, output_dir, current_state):
        utils.create_directory(output_dir)
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

        if False: # CEDE SCENARIOS DATA IF NEEDED - DO NOT REMOVE
            filename = "_df_open_positions.csv"
            utils.modify_strategy_data_files(input_dir, filename)
            exit(1)
        if False: # CEDE SCENARIOS DATA IF NEEDED - DO NOT REMOVE
            filename = "_df_current_states.csv"
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
            self.log("SCENARIO COMPLETED AT ROUND {}".format(str_cpt))
            # if df_orders is None and df_grids is None:
            #     print("*********** EXIT UT ***********")
            #     return None
                # exit(0)
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
                    self.log("ORDERS MATCHING 100%")
                else:
                    self.log("ORDERS NOT MATCHING")
            else:
                self.log("NO ORDER BASELINE AVAILABLE FOR THIS SCENARIO")

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
                    self.log("GRIDS MATCHING 100%")
                else:
                    self.log("GRIDS NOT MATCHING")
            else:
                self.log("NO GRID BASELINE AVAILABLE FOR THIS SCENARIO")

            self.execute_timer.set_scenario_directory(input_dir)
            self.execute_timer.close_grid_scenario()
            self.execute_timer.plot_all_close_grid_scenario()
            exit(0)

        broker_current_state = {
            "open_orders": df_open_orders,
            "open_positions": df_open_positions,
            "prices": df_prices
        }
        return broker_current_state

    def get_memory_usage(self, class_memory):
        memory_usage = {}
        for attr_name, attr_value in vars(class_memory).items():
            # memory_usage[attr_name] = sys.getsizeof(attr_value)
            memory_usage[attr_name] = asizeof.asizeof(attr_value)
        return memory_usage

    def print_memory_usage(self, id, size_init, size_previous, size):
        if not self.zero_print:
            if size_previous == 0:
                return
            for (key1, value1), (key2, value2), (key3, value3) in zip(size_init.items(), size_previous.items(), size.items()):
                if (((value3 - value1) > 0)
                    or ((value3 - value2) > 0))\
                        and not("_size" in key1):
                    self.log("MEMORY " + id + " - " + key1 + " VALUE: " + str(value3) + " PREV DIFF: " +  str(value3 - value2) + " INIT DIFF: " + str(value3 - value1))

    def udpate_strategy_with_broker_current_state(self):
        self.udpate_strategy_with_broker_current_state_live()

    def udpate_strategy_with_broker_current_state_live(self):
        self.start_time_grid_strategy = time.time()

        self.reset_iteration_timers()

        if self.start_time_grid_strategy_init == None:
            self.start_time_grid_strategy_init = self.start_time_grid_strategy
            self.grid_iteration = 1
        else:
            self.grid_iteration += 1

        self.symbols = self.rtstr.get_lst_symbols()

        # self.execute_timer.set_start_time("crag", "current_state_live", "get_current_state", self.current_state_live)
        self.reboot_exception = False
        broker_current_state = self.broker.get_current_state(self.rtstr.get_lst_symbols())
        if broker_current_state["success"] == False:
            self.reboot_exception = True
            return

        if self.broker.get_ws_activation_status():
            self.usdt_equity, self.usdt_available = self.broker.get_usdt_equity_available()
        else:
            self.usdt_equity = self.usdt_equity_thread
            self.usdt_available = self.usdt_available_thread
        if self.init_grid_position:
            self.init_grid_position = False
            df_symbol_minsize = self.broker.get_df_minimum_size(self.symbols)
            dct_symbol_price_place = self.broker.get_price_place_endstep(self.symbols)
            df_symbol_price_place = pd.DataFrame(dct_symbol_price_place)
            df_symbol_features = pd.merge(df_symbol_minsize, df_symbol_price_place, on='symbol')
            df_buying_size = self.rtstr.set_df_buying_size(df_symbol_features, self.usdt_equity)
            del df_symbol_minsize
            # df_buying_size_normalise = self.broker.normalize_grid_df_buying_size_size(df_buying_size)
            self.rtstr.set_df_normalize_buying_size(df_buying_size)
            del df_buying_size
            # del df_buying_size_normalise
            self.rtstr.set_normalized_grid_price(dct_symbol_price_place)

        lst_orders_to_execute = self.rtstr.set_broker_current_state(broker_current_state)

        broker_current_state.clear()
        del broker_current_state

        self.msg_rtstr = self.rtstr.get_info_msg_status()
        # self.rtstr.record_status()
        if self.msg_rtstr != "":
            self.log_discord(self.msg_rtstr, "strategy status")

        del self.msg_rtstr

        self.log("output lst_orders_to_execute: {}".format(lst_orders_to_execute))

        # self.execute_timer.set_start_time("crag", "current_state_live", "execute_trades", self.current_state_live)
        if len(lst_orders_to_execute) > 0:
            lst_orders_to_execute_result = self.broker.execute_trades(lst_orders_to_execute)
            self.rtstr.update_executed_trade_status(lst_orders_to_execute)
        # self.execute_timer.set_end_time("crag", "current_state_live", "execute_trades", self.current_state_live)

        del lst_orders_to_execute

        msg_broker_trade_info = self.broker.log_info_trade()
        if len(msg_broker_trade_info) != 0:
            self.log_discord(msg_broker_trade_info, "broker trade")
            self.broker.clear_log_info_trade()
        del msg_broker_trade_info

        end_time = time.time()
        self.iteration_times_grid_strategy.append(end_time - self.start_time_grid_strategy)
        self.iteration_times_grid_strategy = self.iteration_times_grid_strategy[-10:]
        self.average_time_grid_strategy = round(sum(self.iteration_times_grid_strategy) / len(self.iteration_times_grid_strategy), 2)
        self.average_time_grid_strategy_overall = round((end_time - self.start_time_grid_strategy_init) / self.grid_iteration, 2)

        if not self.zero_print:
            # CEDE MEASURE RUN TIME IN ORDER TO BENCHMARK PC VS RASPBERRY
            self.log("GRID ITERATION AVERAGE TIME:  " + str(self.average_time_grid_strategy) + " seconds")
            self.log("CRAG AVERAGE TIME:            " + str(self.average_time_grid_strategy_overall) + " seconds")
            self.log("OVERALL DURATION:             {}".format(utils.format_duration(round((end_time - self.start_time_grid_strategy_init), 2))))
            self.log("ITERATIONS:                   {}".format(self.grid_iteration))
            self.log("MEMORY:                       " + str(round(self.memory_used_mb, 2)) + "MB - DELTA: " + str(round(self.memory_used_mb - self.init_memory_used_mb,2)) + " MB" + "\n")

        locals().clear()
        self.current_state_live += 1

    def update_status_for_TPSL(self):
        while True:
            usdt_equity_thread, usdt_available_thread = self.broker.get_usdt_equity_available()
            with self.lock_usdt_equity_thread:
                try:
                    self.usdt_equity_thread = usdt_equity_thread
                    self.usdt_available_thread = usdt_available_thread
                except:
                    exit(628)
            time.sleep(1)

    def safety_step(self):
        start_safety_step = time.time()
        self.broker.enable_cache()

        if self.broker.get_ws_activation_status():
            self.usdt_equity, self.usdt_available = self.broker.get_usdt_equity_available()
        else:
            self.usdt_equity = self.usdt_equity_thread
            self.usdt_available = self.usdt_available_thread

        self.usdt_equity_previous = self.usdt_equity
        self.total_SL_TP = self.usdt_equity - self.original_portfolio_value
        if self.usdt_equity >= self.maximal_portfolio_value:
            self.maximal_portfolio_value = self.usdt_equity
            self.drawdown = 0
            self.actual_drawdown_percent = 0
        else:
            self.drawdown = self.usdt_equity - self.maximal_portfolio_value
            self.actual_drawdown_percent = self.drawdown * 100 / self.maximal_portfolio_value

        if self.original_portfolio_value == 0:
            self.total_SL_TP_percent = 0
        else:
            self.total_SL_TP_percent = self.total_SL_TP * 100 / self.original_portfolio_value

        if self.maximal_portfolio_value == 0:
            self.maximal_portfolio_value = max(self.usdt_equity, self.original_portfolio_value)
            self.actual_drawdown_percent = 0
        else:
            self.actual_drawdown_percent = self.drawdown * 100 / self.maximal_portfolio_value

        if self.rtstr.get_strategy_type() == "CONTINUE":
            # GRID TRADING STRATEGY
            self.udpate_strategy_with_broker_current_state()

        if self.rtstr.condition_for_global_SLTP(self.total_SL_TP_percent) \
                or self.rtstr.condition_for_global_trailer_TP(self.total_SL_TP_percent) \
                or self.rtstr.condition_for_global_trailer_SL(self.total_SL_TP_percent) \
                or self.rtstr.condition_for_max_drawdown_SL(self.actual_drawdown_percent):
            msg = "reset - total SL TP\n"
            msg += "total SL TP: ${} / %{}\n".format(utils.KeepNDecimals(self.total_SL_TP, 2),
                                                     utils.KeepNDecimals(self.total_SL_TP_percent, 2))
            msg += "max drawdown: ${} / %{}\n".format(utils.KeepNDecimals(self.drawdown, 2),
                                                      utils.KeepNDecimals(self.actual_drawdown_percent, 2))
            self.log_discord(msg, "total SL TP")

            lst_active_symbols = self.rtstr.get_lst_symbols()
            self.broker.execute_reset_account(lst_active_symbols)
            return False

        if not self.rtstr.get_strategy_type() == "CONTINUE":
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
                if self.rtstr.condition_for_SLTP(symbol_unrealizedPL_percent) \
                        or self.rtstr.condition_trailer_TP(self.broker._get_coin(symbol), symbol_unrealizedPL_percent)\
                        or self.rtstr.condition_trailer_SL(self.broker._get_coin(symbol), symbol_unrealizedPL_percent):
                    lst_symbol_for_closure.append(symbol)

            if self.rtstr.trigger_high_volatility_protection():
                BTC_price = self.broker.get_value('BTC')
                current_datetime = datetime.now()
                current_timestamp = datetime.timestamp(current_datetime)
                lst_record_volatility = [current_datetime, current_timestamp, 'BTC', BTC_price, 0, self.usdt_equity, 0]
                self.rtstr.set_high_volatility_protection_data(lst_record_volatility)
                if self.rtstr.high_volatility_protection_activation(self.actual_drawdown_percent):
                    self.log("DUMP POSITIONS DUE TO HIGH VOLATILITY")
                    lst_symbol_for_closure = self.broker.get_lst_symbol_position()
                    self.maximal_portfolio_value

        self.broker.disable_cache()

        end_safety_step = time.time()
        self.duration_time_safety_step = end_safety_step - start_safety_step
        del end_safety_step
        del start_safety_step
        self.safety_step_iterration += 1
        return True

    def log_memory(self):
        gc.collect()

        if self.start_time_grid_strategy_init == None:
            self.start_time_grid_strategy_init = time.time()
            self.grid_iteration = 1
        else:
            self.grid_iteration += 1

        memory_used_bytes = utils.get_memory_usage()
        if self.memory_used_mb == 0:
            self.init_memory_used_mb = memory_used_bytes / (1024 * 1024)
            self.memory_used_mb = self.init_memory_used_mb
        else:
            self.memory_used_mb = memory_used_bytes / (1024 * 1024)  # Convert bytes to megabytes

        delta_memory = self.memory_used_mb - self.init_memory_used_mb
        msg = "# MEMORY:" + "\n"
        if delta_memory >= 0:
            msg += f"MEMORY: {self.memory_used_mb:.1f}MB" + " (+" + str(round(delta_memory, 1)) + ")\n"
        else:
            msg += f"MEMORY: {self.memory_used_mb:.1f}MB" + " (-" + str(round(abs(delta_memory), 1)) + ")\n"
        msg += "delta/iter" + str(round(delta_memory / self.grid_iteration, 4)) + "\n"

        end_time = time.time()
        msg += "# PERFORMANCE:" + "\n"
        msg += "CRAG TIME: " + str(round((end_time - self.start_time_grid_strategy_init) / self.grid_iteration, 2)) + "s\n"
        msg += "DURATION: " + utils.format_duration(round((end_time - self.start_time_grid_strategy_init), 2)) + "\n"
        delta_memory = self.memory_used_mb - self.init_memory_used_mb
        msg += "iter: " + str(self.grid_iteration) + " / " + str(round(delta_memory / self.grid_iteration, 4)) + " byte/it\n"
        msg += "MEM: " + str(round(self.memory_used_mb, 4)) + "\n"
        msg += "DELTA: " + str(round(delta_memory, 4)) + "\n"
        msg += "MEM LEAK: " + str(round(self.memory_used_bytes_leak, 4)) + "\n"
        self.memory_used_bytes_leak_sum += self.memory_used_bytes_leak
        msg += "MEM LEAK SUM: " + str(round(self.memory_used_bytes_leak_sum, 4)) + "\n"
        msg += "MEM LEAK AVE: " + str(round(self.memory_used_bytes_leak_sum / self.grid_iteration, 4)) + "\n"

        self.log_discord(msg, "MEMORY STATUS")

    def get_ds_and_price_symbols(self, lst_interval):
        # update all the data
        if self.lst_ds is None \
                or self.lst_symbol is None:
            self.lst_ds = self.rtstr.get_data_description(lst_interval)

            self.lst_symbol = []
            for ds in self.lst_ds:
                self.lst_symbol.extend(ds.symbols)  # Use extend to flatten the list of lists

            # Convert to a set to remove duplicates, then back to a list
            self.lst_symbol = list(set(self.lst_symbol))

        self.prices_symbols = {symbol:self.broker.get_value(symbol) for symbol in self.lst_symbol}

        return self.prices_symbols, self.lst_ds

    def update_sltp_status(self):
        self.lst_sltp = self.rtstr.get_lst_sltp()
        self.lst_sltp = self.broker.track_TPSL_status(self.lst_sltp)
        self.rtstr.update_lst_sltp_status(self.lst_sltp)

    def get_ds(self, lst_interval):
        # update all the data
        if self.lst_ds is None \
                or self.lst_symbol is None:
            self.lst_ds = self.rtstr.get_data_description(lst_interval)

            self.lst_symbol = []
            for ds in self.lst_ds:
                self.lst_symbol.extend(ds.symbols)  # Use extend to flatten the list of lists

            # Convert to a set to remove duplicates, then back to a list
            self.lst_symbol = list(set(self.lst_symbol))

        return self.lst_ds

    def get_price_symbols(self):
        self.prices_symbols = {symbol:self.broker.get_value(symbol) for symbol in self.lst_symbol}

        return self.prices_symbols

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

    def reset_iteration_timers(self):
        self.main_cycle_safety_step = 0
        self.state_live = 0
        self.current_state_live = 0
        if False:
            self.execute_timer.set_master_cycle(self.reboot_iter)
            self.broker.set_execute_time_recorder(self.execute_timer)
            self.rtstr.set_execute_time_recorder(self.execute_timer)

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop('lock_usdt_equity_thread', None)

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.lock_usdt_equity_thread = threading.Lock()

    def deduplicate_and_get_current_data(self, lst_ds):
        """
        Steps:
        1) Group items by hash_id.
        2) Call broker.get_lst_current_data once per unique item (one item per hash_id).
        3) broker.get_lst_current_data updates each unique item's 'current_data' in place.
        4) Copy that updated 'current_data' onto all duplicates sharing the same hash_id.
        5) Return the full list in original order, each item updated.
        """

        # 1) Group items by hash_id
        unique_map = {}  # hash_id -> {'unique_item': item, 'duplicates': []}
        for item in lst_ds:
            hid = item.hash_id  # If it's an attribute. If it's a dict: hid = item['hash_id']

            if hid not in unique_map:
                unique_map[hid] = {
                    'unique_item': item,
                    'duplicates': []
                }
            else:
                unique_map[hid]['duplicates'].append(item)

        # 2) Collect just the unique items (one per hash_id)
        unique_items = [info['unique_item'] for info in unique_map.values()]

        # 3) Call get_lst_current_data once for the unique items
        #    This should update each unique_item.current_data in place
        unique_items = self.broker.get_lst_current_data(unique_items)

        # 4) Replicate the updated 'current_data' to the duplicates
        for info in unique_map.values():
            updated_data = info['unique_item'].current_data  # the in-place updated field
            for dup_item in info['duplicates']:
                dup_item.current_data = updated_data

        # 5) Return the original list (same order) with updated current_data
        return lst_ds