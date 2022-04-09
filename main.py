from src import rtdp_tv,broker,crag
import pandas as pd

if __name__ == '__main__':
    print('CRAG')


    params = {'infile':'history.csv'}
    rtdp = rtdp_tv.RTDPTradingView(params)
    #rtdp.record(3, 1, "history.csv")
    #exit(0)
    #selection = rtdp_tv.next()
    #print(selection)

    broker = broker.BrokerSimulation()
    broker.initialize({'cash':100})

    params = {'rtdp':rtdp, 'broker':broker}
    bot = crag.Crag(params)
    bot.run()
    bot.export_history("broker_history.csv")
    print(bot.broker.get_cash())
    exit(0)

    for i in range(5):
        rtdp.next()
        json_data = rtdp.get_current_data()
        if json_data is None:
            continue
        df_portfolio = pd.read_json(json_data["portfolio"])
        if isinstance(df_portfolio, pd.DataFrame):
            print(df_portfolio)
            print(df_portfolio['symbol'].to_list())
        json_symbols = json_data["symbols"]
        print(json_symbols.keys())
