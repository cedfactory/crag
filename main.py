from src import rtdp,rtdp_simulation,broker_simulation,broker_bitget,broker_bitget_api
from src import rtstr,strategies
from src import crag,crag_helper,logger
from src.toolbox import settings_helper
import requests
import pandas as pd
import os, sys
import time
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

def crag_live(configuration_file, ForceDoNotResetAccount = False):
    # previous code
    #crag_params = crag_helper.initialization_from_configuration_file(configuration_file)
    #bot = crag.Crag(crag_params)

    configuration = crag_helper.load_configuration_file(configuration_file)
    if not configuration:
        print("💥 A problem occurred while loading {}".format(configuration_file))
        return

    # if configuration needs to be overriden, that's where it can be done
    if ForceDoNotResetAccount:
        broker_params = configuration.get("broker", None)
        reset_account = broker_params.get("reset_account", False)
        if reset_account:
            broker_params["reset_account"] = False

    params = crag_helper.get_crag_params_from_configuration(configuration)
    if not params:
        print("💥 A problem occurred while treating {}".format(configuration_file))
        return
    bot = crag.Crag(params)

    start_time = datetime.now()
    bot.run()
    bot.export_status()
    crag_print_duration(start_time)
    crag_plot_output('wallet_tracking_records.csv', ['cash', 'wallet', 'portfolio'])
    crag_helper.benchmark_results(configuration_file)

def crag_reboot(picklefilename):
    bot = crag_helper.initialization_from_pickle(picklefilename)
    if not bot:
        print("[crag_reboot] Couldn't reboot from ", picklefilename)
        return

    directory_path = './output'  # Replace with the path to your directory
    # List all files in the directory
    files = os.listdir(directory_path)
    # Iterate over the files and delete those with the .pickle extension
    for file in files:
        if file.endswith('.pickle'):
            file_path = os.path.join(directory_path, file)
            os.remove(file_path)

    bot.run()
    # bot.export_history("broker_history.csv")
    bot.export_status() # DEBUG CEDE

def check_broker():
    params = {"exchange": "bitget", "account": "bitget_ayato", "reset_account": False}
    my_broker = broker_bitget_api.BrokerBitGetApi(params)
    my_logger = logger.LoggerConsole()

    my_logger.log_time_start("get_coin")
    value = my_broker._get_coin("XRPUSDT_UMCBL")
    my_logger.log_time_stop("get_coin")
    print(value)

def check_fdp():
    fdp_url = settings_helper.get_fdp_url_info("gc1").get("url", None)
    if not fdp_url or fdp_url == "":
        return None

    url = fdp_url+"/history?exchange=bitget&symbol=BTC&start=2023-01-01&interval=1d"
    response = requests.get(url)
    response.close()
    response_json = response.json()
    print(response_json)

def check_crag():
    configuration_file = write_file("./crag.xml", '''<configuration>
        <strategy name="StrategyGridTradingLong">
            <params symbols="XRP" grid_df_params="./test/data/multigrid_df_params.csv"/>
        </strategy>
        <broker name="bitget">
            <params exchange="bitget" account="bitget_ayato" leverage="2" simulation="1" reset_account="False" reset_account_orders="False"/>
        </broker>
        <crag interval="20" />
    </configuration>''')
    configuration = crag_helper.load_configuration_file(configuration_file, ".")
    params = crag_helper.get_crag_params_from_configuration(configuration)
    bot = crag.Crag(params)

def check_strategies():
    available_strategies = rtstr.RealTimeStrategy.get_strategies_list()
    print("Available strategies :", available_strategies)

def write_file(filename, string):
    with open(filename, 'w') as f:
        f.write(string)
    return filename

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
        if len(sys.argv) > 1 and (sys.argv[1] == "--check"):
            check_broker()
            check_fdp()
            #check_crag()
        elif len(sys.argv) > 2 and (sys.argv[1] == "--simulation"):
            crag_simulation(sys.argv[2])
        elif len(sys.argv) > 2 and (sys.argv[1] == "--reboot"):
            crag_reboot(sys.argv[2])
        elif len(sys.argv) > 2 and (sys.argv[1] == "--live"):
            if (sys.argv[2] == "--autorestart"):
                print("automatic restart is on")
                forceDoNotReset = False
                while True:
                    try:
                        crag_live(sys.argv[3], forceDoNotReset)
                    except Exception as e:
                        print("!!!!!!! EXCEPTION RAISED !!!!!!!")
                        print(e)
                        print("!!!!!!!   CRAG RESUMED   !!!!!!!")
                        time.sleep(30)
                        forceDoNotReset = True
                        pass
            else:
                crag_live(sys.argv[2])
        elif len(sys.argv) >= 2 and (sys.argv[1] == "--broker"):
            # crag_broker()
            print("--broker is gone" )
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
