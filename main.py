from src import rtdp_tv,broker,crag
import pandas as pd

_usage_str = """
Options:
    [--record <csvfile>, --run <botname>]
"""

def _usage():
    print(_usage_str)

def crag_record(csvfile):
    rtdp = rtdp_tv.RTDPTradingView()
    rtdp.record(20, 1, csvfile)

def crag_run(botname):
    params = {'infile':'history2.csv'}
    rtdp = rtdp_tv.RTDPTradingView(params)

    broker_simu = broker.BrokerSimulation()
    broker_simu.initialize({'cash':100})

    params = {'rtdp':rtdp, 'broker':broker_simu}
    bot = crag.Crag(params)
    bot.run()
    bot.export_history("broker_history.csv")
    print(bot.broker.get_cash())

if __name__ == '__main__':
    import sys
    if len(sys.argv) >= 2:
        if len(sys.argv) == 3 and (sys.argv[1] == "--record"):
            csvfile = sys.argv[2]
            crag_record(csvfile)
        elif len(sys.argv) >= 2 and (sys.argv[1] == "--run"):
            if len(sys.argv) == 3:
                botname = sys.argv[2]
            crag_run(botname)
        else:
            _usage()
    else:
        _usage()
