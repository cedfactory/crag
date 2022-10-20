from src import rtdp,rtdp_simulation,broker_simulation,broker_ftx,rtstr,rtstr_grid_trading,rtstr_grid_trading_multi,rtstr_balance_trading,rtstr_balance_trading_multi,rtstr_super_reversal,rtstr_trix,rtstr_cryptobot,rtstr_bigwill,rtstr_VMC,analyser,benchmark,automatic_test_plan
from src import crag,crag_helper
from src import logger
# from src import debug
import pandas as pd
import os, sys
import shutil
import fnmatch
import cProfile,pstats
from datetime import datetime
import concurrent.futures

_usage_str = """
Options:
    --record <csvfile>
    --simulation <StrategyName>
    --live <StrategyName>
"""

def blockPrint():
    sys.stdout = open(os.devnull, 'w')

def _usage():
    print(_usage_str)

def crag_record():
    rtdp_params = {"working_directory": "./data2/"}
    my_rtdp = rtdp_simulation.SimRealTimeDataProvider(rtdp_params)
    ds = rtdp.DataDescription()
    my_rtdp.record(ds)

def crag_simulation(strategy_name):
    print("selected strategy: ",strategy_name)
    available_strategies = rtstr.RealTimeStrategy.get_strategies_list()
    if strategy_name in available_strategies:
        strategy = rtstr.RealTimeStrategy.get_strategy_from_name(strategy_name, {"rtctrl_verbose": False})
    else:
        print("💥 missing known strategy ({})".format(strategy_name))
        print("available strategies : ", available_strategies)
        return

    if strategy == None:
        print("💥 can't instantiate strategy ({})".format(strategy_name))
        return

    broker_params = {'cash':10000}
    simu_broker = broker_simulation.SimBroker(broker_params)

    crag_params = {'broker':simu_broker, 'rtstr':strategy}
    bot = crag.Crag(crag_params)

    bot.run()

    bot.export_history("sim_broker_history.csv")
    bot.export_status()


def crag_live(configuration_file):
    bot = crag_helper.initialization_from_configuration_file(configuration_file)
    bot.run()
    # bot.export_history("broker_history.csv") # DEBUG CEDE
    bot.export_status()

def crag_reboot(picklefilename):
    bot = crag_helper.initialization_from_pickle(picklefilename)
    bot.run()
    # bot.export_history("broker_history.csv")
    bot.export_status() # DEBUG CEDE

def crag_analyse_results():
    params = {}

    my_analyser = analyser.Analyser(params)
    my_analyser.run_analyse()

def crag_benchmark_results(params):
    my_benchmark = benchmark.Benchmark(params)
    my_benchmark.run_benchmark()

def crag_benchmark_scenario(df, start_date, end_date, period):
    params = {"start": start_date,  # YYYY-MM-DD
              "end": end_date,  # YYYY-MM-DD
              "period": period}

    my_benchmark = benchmark.Benchmark(params)
    my_benchmark.set_benchmark_df_results(df)

def crag_ftx():
    my_broker_ftx = broker_ftx.BrokerFTX({'account':'test_bot', 'simulation':0})
    print("authentification : {}".format(my_broker_ftx.authentificated))

    print("### balance ###")
    balance = my_broker_ftx.get_balance()
    print(balance)

    print("### positions ###")
    positions = my_broker_ftx.get_positions()
    print(positions)

    print("### my trades ###")
    my_broker_ftx.export_history()

    print("### portfolio value ###")
    print("{}".format(my_broker_ftx.get_portfolio_value()))

    print("### sell everything ###")
    #my_broker_ftx.sell_everything()


def crag_simulation_scenario(strategy_name, start_date, end_date, interval, sl, tp, share_size, grid_step, global_tp, grid_threshold, upper_grid, working_directory):
    available_strategies = rtstr.RealTimeStrategy.get_strategies_list()

    os.chdir(working_directory)
    strategy_suffix = "_" + strategy_name + "_" + start_date + "_" + end_date + "_" + interval + "_sl" + str(sl) + "_tp" + str(tp) + "_ss" + str(share_size) + "_gs" + str(grid_step) + "_gtp" + str(global_tp) + "_thrh" + str(grid_threshold) + "_limit" + str(upper_grid)
    if strategy_name in available_strategies:
        strategy = rtstr.RealTimeStrategy.get_strategy_from_name(strategy_name, {"rtctrl_verbose": False,
                                                                                 "sl": sl,
                                                                                 "tp": tp,
                                                                                 "suffix": strategy_suffix,
                                                                                 "share_size": share_size,
                                                                                 "grid_step": grid_step,
                                                                                 "global_tp": global_tp,
                                                                                 "grid_threshold": grid_threshold,
                                                                                 "upper_grid": upper_grid,
                                                                                 "grid_df_params": '../../grid_df_params.csv',
                                                                                 "working_directory": working_directory})
    else:
        print("💥 missing known strategy ({})".format(strategy_name))
        print("available strategies : ", available_strategies)
        return

    broker_params = {'cash':10000, 'start': start_date, 'end': end_date, "intervals": interval, "working_directory": working_directory}
    simu_broker = broker_simulation.SimBroker(broker_params)

    crag_params = {'broker':simu_broker,
                   'rtstr':strategy,
                   "working_directory": working_directory
                   }
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
    list_share_size = df['share_size'].to_list()
    list_share_size = list(set(list_share_size))
    list_grid_step = df['grid_step'].to_list()
    list_grid_step = list(set(list_grid_step))
    list_global_tp = df['global_tp'].to_list()
    list_global_tp = list(set(list_global_tp))
    list_grid_threshold = df['grid_threshold'].to_list()
    list_grid_threshold = list(set(list_grid_threshold))
    list_upper_grid = df['upper_grid'].to_list()
    list_upper_grid = list(set(list_upper_grid))

    auto_test_directory = os.path.join(os.getcwd(), "./automatic_test_results")
    os.chdir(auto_test_directory)

    for period in list_periods:
        start_date = period[0:10]
        end_date = period[11:21]

        strategy_directory = os.path.join(auto_test_directory, "./" + period)
        os.chdir(strategy_directory)
        print("recorder: ",os.getcwd())

        recorder_params = {'start': start_date, 'end': end_date,
                           "intervals": list_interval[0], "working_directory": strategy_directory,
                           }
        recorder = rtdp_simulation.SimRealTimeDataProvider(recorder_params)

        # directory_data_target = os.path.join(strategy_directory, "./data/")
        recorder.record_for_data_scenario(ds, start_date, end_date, list_interval[0])

    os.chdir(auto_test_directory)
    print("recorder completed: ", os.getcwd())

    if False: # multithreading
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(list_periods)) as executor:
            futures = []
            for period in list_periods:
                futures.append(
                    executor.submit(
                        crag_test_scenario_for_period,
                        df, ds, period, auto_test_directory, list_interval, list_strategy, list_sl, list_tp, list_tp, list_share_size, list_grid_step, grid_threshold, upper_grid
                    )
                )
    else:
        for period in list_periods:
            crag_test_scenario_for_period(df, ds, period, auto_test_directory, list_interval, list_strategy, list_sl, list_tp, list_share_size, list_grid_step, list_global_tp, list_grid_threshold, list_upper_grid)

def crag_test_scenario_for_period(df, ds, period, auto_test_directory, list_interval, list_strategy, list_sl, list_tp, lst_share_size, lst_grid_step, lst_global_tp, lst_grid_threshold, lst_upper_grid):
    start_date = period[0:10]
    end_date = period[11:21]

    strategy_directory = os.path.join(auto_test_directory, "./" + period)
    os.chdir(strategy_directory)

    if False: # multithreading
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(list_strategy) * len(list_sl) * len(list_tp)) as executor:
            futures = []
            for strategy in list_strategy:
                for sl in list_sl:
                    for tp in list_tp:
                        for share_size in lst_share_size:
                            for grid_step in lst_grid_step:
                                for global_tp in lst_global_tp:
                                    for grid_threshold in lst_grid_threshold:
                                        for upper_grid in lst_upper_grid:
                                            futures.append(
                                                executor.submit(crag_simulation_scenario,
                                                                strategy, start_date, end_date, list_interval[0], sl, tp, share_size, grid_step, global_tp, grid_threshold, upper_grid, strategy_directory
                                                                )
                                            )
    else:
        for strategy in list_strategy:
            for sl in list_sl:
                for tp in list_tp:
                    for share_size in lst_share_size:
                        for grid_step in lst_grid_step:
                            for global_tp in lst_global_tp:
                                for grid_threshold in lst_grid_threshold:
                                    for upper_grid in lst_upper_grid:
                                        crag_simulation_scenario(strategy, start_date, end_date, list_interval[0], sl, tp, share_size, grid_step, global_tp, grid_threshold, upper_grid,strategy_directory)

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

    crag_benchmark_scenario(df, start_date, end_date, period)

    os.chdir(auto_test_directory)
    print(os.getcwd())
    df.to_csv(period + "_output.csv")

if __name__ == '__main__':
    # Bear market historical dates
    # https://cointelegraph.com/news/a-brief-history-of-bitcoin-crashes-and-bear-markets-2009-2022
    # INFO:
    # older data available for FTX is 2019-07-21
    # BEARMARKET FROM 2011-06-01 TO 2013-02-01
    # BEARMARKET FROM 2013-11-01 TO 2017-01-01
    # BEARMARKET FROM 2017-12-01 TO 2020-12-01
    # BEARMARKET FROM 2018-01-01 TO 2020-09-01
    # BEARMARKET FROM 2021-03-01 TO 2021-10-01
    # BEARMARKET FROM 2021-11-01 TO 2022-08-08
    #
    # BULLRUN FROM 2020-10-01 TO 2022-04-01
    ##########################################
    lst_bearmarket =[# ['2011-06-01', '2013-02-01'],
                     # ['2013-11-01', '2017-01-01'],
                     # ['2017-12-01', '2020-12-01'],
                     # ['2019-07-21', '2020-09-01'], # OK
                     # ['2021-03-01', '2021-10-01'], # OK
                     # ['2021-11-01', '2022-08-08'],
                       ['2022-06-15', '2022-10-08']
    ]

    path_initial_dir = os.getcwd()
    for interval in lst_bearmarket:
        os.chdir(path_initial_dir)
        params = {"start": interval[0],  # YYYY-MM-DD
                  "end": interval[1],  # YYYY-MM-DD
                  # "start": '2022-01-01',  # YYYY-MM-DD
                  # "end": '2022-08-08',  # YYYY-MM-DD
                  "split": 1,
                  "interval": '1h',
                  # "interval": '1d',
                  # "startegies": ['StrategySuperReversal', 'StrategyTrix', 'StrategyCryptobot'],
                  # "startegies": ['StrategySuperReversal', 'StrategyCryptobot'],
                  # "startegies": ['StrategySuperReversal'],
                  # "startegies": ['StrategyCryptobot'],
                  # "startegies": ['StrategyGridTrading'],
                  "startegies": ['StrategyGridTradingMulti'],
                  # "startegies": ['StrategyBalanceTrading'],
                  # "startegies": ['StrategyBalanceTradingMulti'],
                  # "sl": [0, -5, -10],
                  # "tp": [0, 10, 20]
                  "sl": [0],
                  "tp": [0],
                  "global_tp": [0],
                  # "share_size": [10, 20],
                  # "grid_step": [0.5, 1, 2, 5]
                  "share_size": [10],
                  "grid_step": [1],
                  "grid_threshold": [0.08],
                  "upper_grid": [20000],
                  }

    if len(sys.argv) >= 2:
        if len(sys.argv) == 2 and (sys.argv[1] == "--record"):
            crag_record()
        elif len(sys.argv) > 2 and (sys.argv[1] == "--simulation"):
            crag_simulation(sys.argv[2])
        elif len(sys.argv) > 2 and (sys.argv[1] == "--reboot"):
            crag_reboot(sys.argv[2])
        elif len(sys.argv) > 2 and (sys.argv[1] == "--live"):
            crag_live(sys.argv[2])
        elif len(sys.argv) >= 2 and (sys.argv[1] == "--ftx"):
            crag_ftx()
        elif len(sys.argv) >= 2 and (sys.argv[1] == "--analyse"):
            crag_analyse_results()
        elif len(sys.argv) >= 2 and (sys.argv[1] == "--benchmark"):
            crag_benchmark_results(params)
        elif len(sys.argv) >= 2 and (sys.argv[1] == "--scenario"):
            df_test_plan = automatic_test_plan.build_automatic_test_plan(params)
            crag_test_scenario(df_test_plan)
        elif len(sys.argv) > 2 and (sys.argv[1] == "--profiler"):
            strategy_name = sys.argv[2]

            #cProfile.run('crag_simulation(strategy_name)', 'simulation.prof')

            start = datetime.now()

            # ref : https://www.machinelearningplus.com/python/cprofile-how-to-profile-your-python-code/
            profiler = cProfile.Profile()
            profiler.enable()
            crag_simulation(strategy_name)
            profiler.disable()
            stats = pstats.Stats(profiler).sort_stats('cumtime')
            stats.strip_dirs() # removes all leading path information from file names
            stats.print_stats()
            stats.dump_stats('stats_dump.dat')

            end = datetime.now()
            elapsed_time = str(end - start)
            print(elapsed_time)

            # to visualize stats_dump.dat
            # gprof2dot -f pstats stats_dump.dat | dot -Tpng -o output.png


    else:
        _usage()
else:
    _usage()
