from src import rtdp,rtdp_simulation,broker_simulation,broker_ftx,crag,rtstr_super_reversal,rtstr_trix,rtstr_cryptobot,rtstr_bigwill,rtstr_VMC,analyser,benchmark, automatic_test_plan
import pandas as pd
import os
import shutil
import fnmatch

import concurrent.futures

_usage_str = """
Options:
    [--record <csvfile>, --run <botname>]
"""

def _usage():
    print(_usage_str)

def crag_record():
    rtdp = rtdp_simulation.SimRealTimeDataProvider()
    ds = rtdp.DataDescription()
    rtdp.record(ds)

def crag_simulation(strategy_name):
    print("selected strategy: ",strategy_name)
    if strategy_name == "superreversal":
        strategy = rtstr_super_reversal.StrategySuperReversal(params={"rtctrl_verbose": False})
    if strategy_name == "trix":
        strategy = rtstr_trix.StrategyTrix(params={"rtctrl_verbose": False})
    if strategy_name == "cryptobot":
        strategy = rtstr_cryptobot.StrategyCryptobot(params={"rtctrl_verbose": False})
    if strategy_name == "bigwill":
        strategy = rtstr_bigwill.StrategyBigWill(params={"rtctrl_verbose": False})
    if strategy_name == "vmc":
        strategy = rtstr_VMC.StrategyVMC(params={"rtctrl_verbose": False})

    broker_params = {'cash':10000}
    simu_broker = broker_simulation.SimBroker(broker_params)

    crag_params = {'broker':simu_broker, 'rtstr':strategy}
    bot = crag.Crag(crag_params)

    bot.run()

    bot.export_history("sim_broker_history.csv")
    bot.export_status()

def crag_run(strategy_name, history):
    params = {}
    if history != "":
        params['infile'] = history

    if strategy_name == "superreversal":
        strategy_super_reversal = rtstr_super_reversal.StrategySuperReversal()
    else:
        print("💥 missing known strategy ({})".format(strategy_name))
        return

    broker_simu = broker_ftx.BrokerFTX({'simulation':True})
    broker_simu.initialize({'cash':100})

    params = {'broker':broker_simu, 'rtstr':strategy_super_reversal}
    bot = crag.Crag(params)
    bot.run()
    bot.export_history("broker_history.csv")
    bot.export_status()

def crag_analyse_resusts():
    params = {}

    my_analyser = analyser.Analyser(params)
    my_analyser.run_analyse()

def crag_benchmark_resusts():
    params = {}

    my_benchmark = benchmark.Benchmark(params)
    my_benchmark.run_benchmark()

def crag_benchmark_scenario(df, period):
    params = {'period': period}

    my_benchmark = benchmark.Benchmark(params)
    my_benchmark.set_benchmark_df_results(df)

def crag_ftx():
    my_broker_ftx = broker_ftx.BrokerFTX()
    authentificated = my_broker_ftx.initialize({})
    print("authentification : {}".format(authentificated))

    print("### balance ###")
    balance = my_broker_ftx.get_balance()
    print(balance)

    print("### positions ###")
    positions = my_broker_ftx.get_positions()
    print(positions)

    print("### my trades ###")
    my_broker_ftx.export_history()


def crag_simulation_scenario(strategy_name, start_date, end_date, interval, sl, tp):
    print("selected strategy: ",strategy_name)
    strategy_suffix = "_" + strategy_name + "_" + start_date + "_" + end_date + "_" + interval + "_sl" + str(sl) + "_tp" + str(tp)

    broker_params = {'cash':10000, 'start': start_date, 'end': end_date, "intervals": interval}
    simu_broker = broker_simulation.SimBroker(broker_params)

    if strategy_name == "superreversal":
        strategy = rtstr_super_reversal.StrategySuperReversal(params={"rtctrl_verbose": False, "sl": sl, "tp": tp, "suffix": strategy_suffix})
    if strategy_name == "trix":
        strategy = rtstr_trix.StrategyTrix(params={"rtctrl_verbose": False, "sl": sl, "tp": tp, "suffix": strategy_suffix})
    if strategy_name == "cryptobot":
        strategy = rtstr_cryptobot.StrategyCryptobot(params={"rtctrl_verbose": False, "sl": sl, "tp": tp, "suffix": strategy_suffix})
    if strategy_name == "bigwill":
        strategy = rtstr_bigwill.StrategyBigWill(params={"rtctrl_verbose": False, "sl": sl, "tp": tp, "suffix": strategy_suffix})
    if strategy_name == "vmc":
        strategy = rtstr_VMC.StrategyVMC(params={"rtctrl_verbose": False, "sl": sl, "tp": tp, "suffix": strategy_suffix})

    crag_params = {'broker':simu_broker, 'rtstr':strategy}
    bot = crag.Crag(crag_params)

    bot.run()

    bot.export_history("sim_broker_history"
                       + strategy_suffix
                       + ".csv")
    bot.export_status()

def crag_test_scenario(df):
    ds = rtdp.DataDescription()

    list_periods = df['period'].to_list()
    list_periods = list(set(list_periods))
    list_interval = df['interval'].to_list()     # multi interval to be implemented... one day maybe
    list_interval = list(set(list_interval))     # multi interval to be implemented... one day maybe
    list_strategy = df['strategy'].to_list()
    list_strategy = list(set(list_strategy))
    list_tp = df['tp'].to_list()
    list_tp = list(set(list_tp))
    list_sl = df['sl'].to_list()
    list_sl = list(set(list_sl))

    home_directory = os.getcwd()
    auto_test_directory = os.path.join(os.getcwd(), "./automatic_test_results")
    os.chdir(auto_test_directory)

    for period in list_periods:

        # CEDE for debug purpose
        # period = "2020-06-25_2021-06-13"

        start_date = period[0:10]
        end_date = period[11:21]

        strategy_directory = os.path.join(auto_test_directory, "./" + period)
        os.chdir(strategy_directory)

        recorder_params = {'start': start_date, 'end': end_date, "intervals": list_interval[0]}
        recorder = rtdp_simulation.SimRealTimeDataProvider(recorder_params)

        directory_data_target = os.path.join(strategy_directory, "./data/")
        recorder.record_for_data_scenario(ds, start_date, end_date, list_interval[0], directory_data_target)

        '''
        for strategy in list_strategy:
            for sl in list_sl:
                for tp in list_tp:
                    crag_simulation_scenario(strategy, start_date, end_date, list_interval[0], sl, tp)
        '''

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for strategy in list_strategy:
                for sl in list_sl:
                    for tp in list_tp:
                        futures.append(
                            executor.submit(crag_simulation_scenario,
                                            strategy, start_date, end_date, list_interval[0], sl, tp
                                            )
                        )

        list_output_filename = []
        for file in os.listdir("./"):
            if file.startswith('sim_broker_history'):
                list_output_filename.append(file)
        for file in os.listdir("./"):
            if file.startswith('wallet_tracking_records'):
                list_output_filename.append(file)
        print("output file list: ", list_output_filename)
        for filename in list_output_filename:
            if os.path.exists(filename):
                filename2 = './output/' + filename
                os.rename(filename, filename2)

        print('benchmark: ', period)
        # Move files for benchmark
        output_dir = os.path.join(strategy_directory, "./output")
        benchmark_dir = os.path.join(strategy_directory, "./benchmark")
        list_csv_files = fnmatch.filter(os.listdir(output_dir), '*.csv')
        for csv_file in list_csv_files:
            shutil.copy(os.path.join(output_dir, csv_file), os.path.join(benchmark_dir, csv_file))

        crag_benchmark_scenario(df, period)

        os.chdir(auto_test_directory)
        print(os.getcwd())
        df.to_csv(period + "_output.csv")

if __name__ == '__main__':
    import sys
    if len(sys.argv) >= 2:
        if len(sys.argv) == 2 and (sys.argv[1] == "--record"):
            crag_record()
        elif len(sys.argv) == 2 and (sys.argv[1] == "--simulation"):
            crag_simulation('superreversal')
        elif len(sys.argv) >= 2 and (sys.argv[1] == "--run"):
            strategy_name = ""
            if len(sys.argv) >= 3:
                strategy_name = sys.argv[2]
            history = ""
            if len(sys.argv) >= 4:
                history = sys.argv[3]
            crag_run(strategy_name, history)
        elif len(sys.argv) >= 2 and (sys.argv[1] == "--ftx"):
            crag_ftx()
        elif len(sys.argv) >= 2 and (sys.argv[1] == "--analyse"):
            crag_analyse_resusts()
        elif len(sys.argv) >= 2 and (sys.argv[1] == "--benchmark"):
            crag_benchmark_resusts()
        elif len(sys.argv) >= 2 and (sys.argv[1] == "--scenario"):
            params = {"start": '2020-01-01',
                      "end": '2022-06-01',
                      "split": 5,
                      "interval": '1h',
                      "startegies": ['superreversal', 'trix', 'cryptobot'],    # WARNING do not use _ in strategy names
                      "sl": [0, -5, -10],
                      "tp": [0, 10, 20]
            }
            df_test_plan = automatic_test_plan.build_automatic_test_plan(params)
            crag_test_scenario(df_test_plan)
        else:
            _usage()
    else:
        _usage()
