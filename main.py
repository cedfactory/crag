from src import rtdp_tv
import pandas as pd

if __name__ == '__main__':
    print('CRAG')

    params = {'infile':'history.csv'}
    rtdp_tv = rtdp_tv.RTDPTradingView(params)
    #rtdp_tv.record(1, 1, "history.csv")
    #exit(0)
    #selection = rtdp_tv.next()
    #print(selection)

    for i in range(5):
        rtdp_tv.next()
        json_data = rtdp_tv.get_current_data()
        if json_data is None:
            continue
        df_portfolio = pd.read_json(json_data["portfolio"])
        if isinstance(df_portfolio, pd.DataFrame):
            print(df_portfolio)
            print(df_portfolio['symbol'].to_list())
        json_symbols = json_data["symbols"]
        print(json_symbols.keys())
