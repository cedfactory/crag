import requests
import pandas as pd
import mplfinance as mpf
from datetime import datetime


def get_ohlcv_data_from_date(symbol, interval='1h', start_date='2024-10-29 18:00'):
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


def plot_with_mplfinance(df):
    """
    Plot OHLCV data with mplfinance and add a horizontal line at the average of the first candle's open and close.

    Args:
        df (pd.DataFrame): OHLCV data.
    """
    df.set_index('open_time', inplace=True)
    first_avg = (df['open'].iloc[0] + df['close'].iloc[0]) / 2  # Average of the first open and close

    # Plot with mplfinance and add a horizontal line
    add_line = [mpf.make_addplot([first_avg] * len(df), color='red', linestyle='--')]  # Horizontal line
    mpf.plot(df, type='candle', volume=True, style='yahoo', title='OHLC Candlestick Chart with mplfinance',
             addplot=add_line)


# Usage
symbol = 'PEPEUSDT'  # Replace with the desired coin pair
interval = '1h'  # 1-hour interval
start_date = '2024-10-29 18:00'  # Start date and time for fetching data

# Fetch OHLCV data from the specified start date
df = get_ohlcv_data_from_date(symbol, interval, start_date)

# Plot with mplfinance
plot_with_mplfinance(df)
