from src import rtdp,rtdp_simulation,broker_simulation,broker_ftx,crag,rtstr_super_reversal,rtstr_trix,analyser
import pandas as pd

_usage_str = """
Options:
    [--record <csvfile>, --run <botname>]
"""

def _usage():
    print(_usage_str)

def crag_record():
    my_rtdp = rtdp_simulation.SimRealTimeDataProvider()
    ds = rtdp.DataDescription()
    my_rtdp.record(ds, "./data2/")

def crag_simulation(strategy_name):
    if strategy_name == "super_reversal":
        strategy = rtstr_super_reversal.StrategySuperReversal(params={"rtctrl_verbose": False})
    if strategy_name == "trix":
        strategy = rtstr_trix.StrategyTrix(params={"rtctrl_verbose": False})

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

    broker_simu = broker_ftx.BrokerFTX({'simulation':True, 'cash':100})

    params = {'broker':broker_simu, 'rtstr':strategy_super_reversal}
    bot = crag.Crag(params)
    bot.run()
    bot.export_history("broker_history.csv")
    bot.export_status()

def crag_analyse_resusts():
    params = {}

    my_analyser = analyser.Analyser(params)
    my_analyser.run_analyse()

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
        elif len(sys.argv) == 2 and (sys.argv[1] == "--simulation"):
            crag_simulation('trix')
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
        else:
            _usage()
    else:
        _usage()
