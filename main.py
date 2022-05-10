from src import rtdp,broker,broker_ftx,crag,rtstr_tv,rtstr_super_reversal
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

def crag_run(strategy_name, history):
    params = {}
    if history != "":
        params['infile'] = history
    my_rtdp = rtdp.RealTimeDataProvider(params)

    if strategy_name == "super_reversal":
        strategy_super_reversal = rtstr_super_reversal.StrategySuperReversal()
    else:
        print("💥 missing known strategy ({})".format(strategy_name))
        return

    broker_simu = broker.BrokerSimulation()
    broker_simu.initialize({'cash':100})

    params = {'rtdp':my_rtdp, 'broker':broker_simu, 'rtstr':strategy_super_reversal}
    bot = crag.Crag(params)
    bot.run()
    bot.export_history("broker_history.csv")
    bot.export_status()

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

    '''
    # test to buy
    print("### create order ###")
    buy_trade = trade.Trade()
    buy_trade.type = "BUY"
    buy_trade.symbol = "BTC/USD"
    buy_trade.net_price = 0.0005
    result = my_broker_ftx.execute_trade(buy_trade)
    print(result)
    '''


if __name__ == '__main__':
    import sys
    if len(sys.argv) >= 2:
        if len(sys.argv) == 3 and (sys.argv[1] == "--record"):
            csvfile = sys.argv[2]
            crag_record(csvfile)
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
        else:
            _usage()
    else:
        _usage()
