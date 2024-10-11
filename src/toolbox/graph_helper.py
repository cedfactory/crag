import pandas as pd
import numpy as np
from datetime import datetime

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from binance.client import Client
import mplfinance as mpf

#####
def get_historical_ohlc_data(symbol, past_days=None, interval=None):
    """Returns historcal klines from past for given symbol and interval
    past_days: how many days back one wants to download the data"""

    if not interval:
        interval = '1h'  # default interval 1 hour
    if not past_days:
        past_days = 30  # default past days 30.

    start_str = str((pd.to_datetime('today') - pd.Timedelta(str(past_days) + ' days')).date())

    client = Client()
    data = client.get_historical_klines(symbol=symbol, start_str=start_str, interval=interval)
    D = pd.DataFrame(data)
    D.columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'qav', 'num_trades',
                 'taker_base_vol', 'taker_quote_vol', 'is_best_match']
    D['open_date_time'] = [datetime.fromtimestamp(x / 1000) for x in D.open_time]
    D['symbol'] = symbol
    D = D[['symbol', 'open_date_time', 'open', 'high', 'low', 'close', 'volume', 'num_trades', 'taker_base_vol',
           'taker_quote_vol']]

    return D


def get_ohlcv_values(df_ohlvc, time):
    open = df_ohlvc.at[time, 'open']
    close = df_ohlvc.at[time, 'close']
    high = df_ohlvc.at[time, 'high']
    low = df_ohlvc.at[time, 'low']
    volume = df_ohlvc.at[time, 'volume']

    return open, high, low, volume, close

def export_btcusd(filename, past_days):
    df_ohlvc = get_historical_ohlc_data("BTCUSDT", past_days=past_days)

    #print(df_ohlvc)

    df_ohlvc["date"] = pd.to_datetime(df_ohlvc["open_date_time"])
    df_ohlvc.reset_index(inplace=True)
    df_ohlvc.set_index('date', inplace=True)

    df_ohlvc['open'] = df_ohlvc['open'].astype(float)
    df_ohlvc['close'] = df_ohlvc['close'].astype(float)
    df_ohlvc['high'] = df_ohlvc['high'].astype(float)
    df_ohlvc['low'] = df_ohlvc['low'].astype(float)
    df_ohlvc['volume'] = df_ohlvc['volume'].astype(float)

    mpf.plot(df_ohlvc,type="line", volume=False, title="BTC",savefig=filename, ylabel="")

    return df_ohlvc
#####


'''
filename
title
[
{
dataframe
[lst_columns]
[lst_labels]
}
]df, lst_columns, df2=None, lst_columns2=None
'''
def export_graph(filename, title, dataframe_infos):
    #plt.subplots()
    fig = plt.figure(figsize=(10, 4))

    ax = plt.gca()
    xfmt = mdates.DateFormatter('%d-%m-%Y')
    ax.xaxis.set_major_formatter(xfmt)

    ymin = []
    ymax = []

    # go through the dataframes
    for df_info in dataframe_infos:
        df = df_info["dataframe"]

        if df["timestamp"].dtype == np.float64 or df["timestamp"].dtype == np.int64:
            dates = [datetime.fromtimestamp(ts) for ts in df["timestamp"]]
        else:
            dates = df["timestamp"]
        datenums = mdates.date2num(dates)

        for column_info in df_info["plots"]:
            column_name = column_info.get("column", None)
            if not column_name:
                continue
            y = df[column_name]
            ymin.append(min(y))
            ymax.append(max(y))
            column_label = column_info.get("label", column_name)
            style = column_info.get("style", None)
            if style == "stairs":
                ax.step(datenums, y, where="post", label=column_label)
            else:
                ax.plot(datenums, y, label=column_label)
                plt.fill_between(datenums, y, alpha=0.3)

    y_min = min(ymin)
    y_max = max(ymax)
    margin = 0.1*(y_max-y_min)
    y_min = y_min - margin
    y_max = y_max + margin
    if y_min == y_max:
        y_max = y_min + 10
    ax.set_ylim([y_min, y_max])

    plt.title(title)
    plt.legend()
    plt.xticks(rotation=25)
    plt.subplots_adjust(bottom=0.2)

    plt.savefig(filename)
    #plt.show()
    plt.close()

    return fig
