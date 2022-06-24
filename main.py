from src import rtdp,rtdp_simulation,broker_simulation,broker_ftx,crag,rtstr,rtstr_super_reversal,rtstr_trix,rtstr_cryptobot,rtstr_bigwill,rtstr_VMC,analyser,benchmark
import pandas as pd

_usage_str = """
Options:
    --record <csvfile>
    --simulation <StrategyName>
    --live <StrategyName>
"""

def _usage():
    print(_usage_str)

def crag_record():
    my_rtdp = rtdp_simulation.SimRealTimeDataProvider()
    ds = rtdp.DataDescription()
    my_rtdp.record(ds, "./data2/")

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

def crag_live(strategy_name):

    params = {}
    available_strategies = rtstr.RealTimeStrategy.get_strategies_list()
    if strategy_name in available_strategies:
        my_strategy = rtstr.RealTimeStrategy.get_strategy_from_name(strategy_name, params)
    else:
        print("💥 missing known strategy ({})".format(strategy_name))
        print("available strategies : ", available_strategies)
        return

    my_broker = broker_ftx.BrokerFTX({'account':'test_bot', 'simulation':False})

    params = {'broker':my_broker, 'rtstr':my_strategy, 'interval':5}
    bot = crag.Crag(params)
    bot.run()
    bot.export_history("broker_history.csv")
    bot.export_status()

def crag_analyse_results():
    params = {}

    my_analyser = analyser.Analyser(params)
    my_analyser.run_analyse()

def crag_benchmark_results():
    params = {}

    my_benchmark = benchmark.Benchmark(params)
    my_benchmark.run_benchmark()


def crag_ftx():
    my_broker_ftx = broker_ftx.BrokerFTX()
    print("authentification : {}".format(my_broker_ftx.authentificated))

    print("### balance ###")
    balance = my_broker_ftx.get_balance()
    print(balance)

    print("### positions ###")
    positions = my_broker_ftx.get_positions()
    print(positions)

    print("### my trades ###")
    my_broker_ftx.export_history()


if __name__ == '__main__':
    import sys
    if len(sys.argv) >= 2:
        if len(sys.argv) == 2 and (sys.argv[1] == "--record"):
            crag_record()
        elif len(sys.argv) > 2 and (sys.argv[1] == "--simulation"):
            strategy_name = sys.argv[2]
            crag_simulation(strategy_name)
        elif len(sys.argv) > 2 and (sys.argv[1] == "--live"):
            strategy_name = sys.argv[2]
            crag_live(strategy_name)
        elif len(sys.argv) >= 2 and (sys.argv[1] == "--ftx"):
            crag_ftx()
        elif len(sys.argv) >= 2 and (sys.argv[1] == "--analyse"):
            crag_analyse_results()
        elif len(sys.argv) >= 2 and (sys.argv[1] == "--benchmark"):
            crag_benchmark_results()
        else:
            _usage()
    else:
        _usage()
