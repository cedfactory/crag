import pandas as pd
import numpy as np
from datetime import datetime
import requests
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

def get_historical_ohlcv_2(symbol, start_date, interval='1h'):
    """
    Fetch OHLCV data for a specific symbol and interval, starting from a specified date and time.

    Args:
        symbol (str): Symbol to fetch, e.g., 'BTCUSDT'.
        interval (str): Interval (e.g., '1m', '1h', '1d').
        start_date (str): The start date in 'YYYY-MM-DD HH:MM' format.

    Returns:
        pd.DataFrame: OHLCV data from the specified start date to the current date.
    """
    url = f'https://api.binance.com/api/v3/klines'
    all_data = []
    limit = 1000  # Binance's maximum limit per request
    start_time = int(datetime.strptime(start_date, '%Y-%m-%d %H:%M').timestamp() * 1000)
    end_time = int(datetime.now().timestamp() * 1000)  # Current time in milliseconds

    while True:
        # Set up parameters for each batch
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': limit,
            'startTime': start_time,
            'endTime': end_time
        }
        response = requests.get(url, params=params)
        data = response.json()

        if not data:
            break

        all_data.extend(data)

        # Update start_time to continue from where the last batch ended
        start_time = data[-1][6] + 1  # +1 to avoid duplicate records

        # Break if we reach the most recent data
        if len(data) < limit:
            break

    # Convert to DataFrame and format
    df = pd.DataFrame(all_data, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
        'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
        'taker_buy_quote_asset_volume', 'ignore'
    ])

    # Convert timestamp to datetime and cast OHLC values to float
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    df['volume'] = df['volume'].astype(float)

    return df[['open_time', 'open', 'high', 'low', 'close', 'volume']]

def get_ohlcv_values(df_ohlvc, time):
    open = df_ohlvc.at[time, 'open']
    close = df_ohlvc.at[time, 'close']
    high = df_ohlvc.at[time, 'high']
    low = df_ohlvc.at[time, 'low']
    volume = df_ohlvc.at[time, 'volume']

    return open, high, low, volume, close

def plot_ohlcv_and_line_on_first_value(filename, df, title):
    """
    Plot OHLCV data with mplfinance and add a horizontal line at the average of the first candle's open and close.

    Args:
        df (pd.DataFrame): OHLCV data.
    """
    df.set_index('open_time', inplace=True)
    first_avg = (df['open'].iloc[0] + df['close'].iloc[0]) / 2  # Average of the first open and close

    # Plot with mplfinance and add a horizontal line
    add_line = [mpf.make_addplot([first_avg] * len(df), color='red', linestyle='--')]  # Horizontal line
    mpf.plot(df, type='candle', volume=True, style='yahoo', title=title,
             savefig=filename,
             addplot=add_line)

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
