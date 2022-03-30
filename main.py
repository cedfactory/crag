from src import rtdp_tv
import pandas as pd

if __name__ == '__main__':
    print('CRAG')

    params = {'infile':'out.csv'}
    rtdp_tv = rtdp_tv.RTDPTradingView(params)
    #rtdp_tv.record(3, "out.csv")
    #selection = rtdp_tv.next()
    #print(selection)

    for i in range(5):
        rtdp_tv.next()
        df_data = rtdp_tv.get_current_data()
        if isinstance(df_data, pd.DataFrame):
            print(df_data['symbol'].to_list())
