from src import rtdp,rtdp_simulation,broker_simulation,broker_ftx,crag,rtstr_super_reversal,rtstr_trix,rtstr_cryptobot,rtstr_bigwill,rtstr_VMC,analyser,benchmark, automatic_test_plan
import pandas as pd
import os

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
    if strategy_name == "super_reversal":
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

    if strategy_name == "super_reversal":
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


def crag_simulation_scenario(strategy_name, start_date, end_date, interval):
    print("selected strategy: ",strategy_name)
    if strategy_name == "super_reversal":
        strategy = rtstr_super_reversal.StrategySuperReversal(params={"rtctrl_verbose": False})
    if strategy_name == "trix":
        strategy = rtstr_trix.StrategyTrix(params={"rtctrl_verbose": False})
    if strategy_name == "cryptobot":
        strategy = rtstr_cryptobot.StrategyCryptobot(params={"rtctrl_verbose": False})
    if strategy_name == "bigwill":
        strategy = rtstr_bigwill.StrategyBigWill(params={"rtctrl_verbose": False})
    if strategy_name == "vmc":
        strategy = rtstr_VMC.StrategyVMC(params={"rtctrl_verbose": False})

    broker_params = {'cash':10000, 'start': start_date, 'end': end_date, "intervals": interval}
    simu_broker = broker_simulation.SimBroker(broker_params)

    crag_params = {'broker':simu_broker, 'rtstr':strategy}
    bot = crag.Crag(crag_params)

    bot.run()

    bot.export_history("sim_broker_history.csv")
    bot.export_status()

def crag_test_scenario(df):

    ds = rtdp.DataDescription()
    list_periods = df['period'].to_list()
    list_periods = list(set(list_periods))
    list_interval = df['interval'].to_list()     # multi interval to be implemented...
    list_interval = list(set(list_interval))     # multi interval to be implemented...
    list_strategy = df['strategy'].to_list()
    list_strategy = list(set(list_strategy))

    home_directory = os.getcwd()
    auto_test_directory = os.path.join(os.getcwd(), "./automatic_test_results")
    print(auto_test_directory)
    os.chdir(auto_test_directory)

    print(os.getcwd())

    for period in list_periods:
        start_date = period[0:10]
        end_date = period[11:21]

        # strategy_directory = auto_test_directory + period
        strategy_directory = os.path.join(auto_test_directory, "./" + period)
        os.chdir(strategy_directory)

        recorder_params = {'start': start_date, 'end': end_date, "intervals": list_interval[0]}
        recorder = rtdp_simulation.SimRealTimeDataProvider(recorder_params)

        directory_data_target = os.path.join(strategy_directory, "./data/")
        recorder.record_for_data_scenario(ds, start_date, end_date, list_interval[0], directory_data_target)


        print(os.getcwd())

        for strategy in list_strategy:
            crag_simulation_scenario(strategy, start_date, end_date, list_interval[0])
            list_output_filename = ['sim_broker_history.csv', 'wallet_tracking_records.csv']
            for filename in list_output_filename:
                prefixe = filename.split(".")[0]
                extention = filename.split(".")[1]
                interval = list_interval[0]
                if os.path.exists(filename):
                    filename2 = './output/' + prefixe + '_' + period + '_' + strategy + '_' + interval + extention
                    os.rename(filename, filename2)
        print('benchmark: ')
        crag_benchmark_resusts(df)
        os.chdir(auto_test_directory)
        print(os.getcwd())





if __name__ == '__main__':
    import sys
    if len(sys.argv) >= 2:
        if len(sys.argv) == 2 and (sys.argv[1] == "--record"):
            crag_record()
        elif len(sys.argv) == 2 and (sys.argv[1] == "--simulation"):
            crag_simulation('super_reversal')
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
            df_test_plan = automatic_test_plan.build_automatic_test_plan('2020-01-01', '2022-06-01', 5)
            crag_test_scenario(df_test_plan)
        else:
            _usage()
    else:
        _usage()
