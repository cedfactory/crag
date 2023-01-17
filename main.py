from src import rtdp,rtdp_simulation,broker_simulation,broker_ccxt,rtstr,rtstr_dummy_test,rtstr_dummy_test_tp,rtstr_grid_trading_multi,rtstr_balance_trading,rtstr_bollinger_trend,rtstr_tv_recommendation_mid,rtstr_balance_trading_multi,rtstr_super_reversal,rtstr_trix,rtstr_cryptobot,rtstr_bigwill,rtstr_VMC,analyser,benchmark,automatic_test_plan
from src import crag,crag_helper
import pandas as pd
import os, sys
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

def crag_print_duration(start_time):
    end_time = datetime.now()
    duration = end_time - start_time
    duration_in_s = duration.total_seconds()
    hours = divmod(duration_in_s, 3600)[0]
    minutes = divmod(duration_in_s, 60)[0]
    seconds = duration.seconds
    print('duration : ', hours, 'h ', minutes, 'm ', seconds, 's')

def crag_plot_output(filename_csv, lst_column):
    prefixe = filename_csv.split(".")
    directory = './output/'
    path = directory + filename_csv

    if os.path.exists(path):
        df = pd.read_csv(path)
        for column in lst_column:
            if column in df.columns.tolist():
                df_plot = pd.DataFrame()
                df_plot[column] = df[column]
                ax = df_plot.plot.line()
                ax.grid()
                output_file_png = directory + prefixe[0] + '_' + column + '.png'
                ax.figure.savefig(output_file_png)
    else:
        print('not file: ', path)

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

def crag_benchmark(scenarios_df_lst):
    lst_configuration_file = crag_helper.get_configuration_files_list(scenarios_df_lst)
    for configuration_file in lst_configuration_file:
        crag_live(configuration_file)
    crag_report()

def crag_report():
    crag_helper.export_benchmark_df_report()

def crag_live(configuration_file):
    bot = crag_helper.initialization_from_configuration_file(configuration_file)
    start_time = datetime.now()
    bot.run()
    bot.export_status()
    crag_print_duration(start_time)
    crag_plot_output('wallet_tracking_records.csv', ['cash', 'wallet', 'portfolio'])
    crag_helper.benchmark_results(configuration_file)

def crag_reboot(picklefilename):
    bot = crag_helper.initialization_from_pickle(picklefilename)
    bot.run()
    # bot.export_history("broker_history.csv")
    bot.export_status() # DEBUG CEDE

def crag_analyse_results():
    params = {}

    my_analyser = analyser.Analyser(params)
    my_analyser.run_analyse()

def crag_broker():
    my_broker = broker_ccxt.BrokerCCXT({'exchange':'bitget', 'account':'room2', 'simulation':0})

    print("### balance ###")
    balance = my_broker.get_balance()
    print(balance)

    print("### positions ###")
    positions = my_broker.get_positions()
    print(positions)
    
    print("### orders ###")
    orders = my_broker.get_orders("BTC/USDT")
    print(orders)

    print("### my trades ###")
    my_broker.export_history()

    print("### portfolio value ###")
    print("{}".format(my_broker.get_portfolio_value()))

    print("### BTC ###")
    usdt_value = my_broker.get_value("BTC/USDT")
    print("USDT value = ", usdt_value)

    usdt_position_risk = my_broker.get_positions_risk(["BTC/USDT"])
    print("USDT oposition risk = ", usdt_position_risk)

    print("### sell everything ###")
    #my_broker.sell_everything()

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

    if len(sys.argv) >= 2:
        if len(sys.argv) > 2 and (sys.argv[1] == "--simulation"):
            crag_simulation(sys.argv[2])
        elif len(sys.argv) > 2 and (sys.argv[1] == "--reboot"):
            crag_reboot(sys.argv[2])
        elif len(sys.argv) > 2 and (sys.argv[1] == "--live"):
            crag_live(sys.argv[2])
        elif len(sys.argv) > 2 and (sys.argv[1] == "--benchmark"):
            crag_benchmark(sys.argv[2])
        elif len(sys.argv) >= 2 and (sys.argv[1] == "--report"):
            crag_report()
        elif len(sys.argv) >= 2 and (sys.argv[1] == "--broker"):
            crag_broker()
        elif len(sys.argv) >= 2 and (sys.argv[1] == "--analyse"):
            crag_analyse_results()
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
