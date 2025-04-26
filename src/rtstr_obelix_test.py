import pandas as pd
import os
from rtstr_obelix import StrategyObelix

param = {'id': 'prod_BTC',
         'name': 'StrategyObelix',
         'candle_stick': 'released',
         'sl': '0',
         'tp': '0',
         'global_sl': '-50',
         'global_tp': '20',
         'trailer_tp': '0',
         'trailer_delta_tp': '0',
         'trailer_global_tp': '0',
         'trailer_global_delta_tp': '0',
         'trailer_sl': '0',
         'high_volatility': 'False',
         'path_strategy_param': './param_intervals_obelix_prod_AWS.csv',
         'loggers': 'file=log',
         'debug_mode': True,
         'interval': '1m',
         'interval_trend': '1h',
         'side': 'long',
         'grouped': False,
         'ma_type': 'HMA',
         'high_offset': 1.002,
         'low_offset': 1,
         'zema_len_buy': 60,
         'zema_len_sell': 70,
         'trend_type': 'PRICE_ACTION',
         'ssl_atr_period': 5,
         'conversion_line_period': 20,
         'base_line_periods': 60,
         'lagging_span': 120,
         'displacement': 30,
         'SL': 2,
         'margin': 100,
         'grouped_id': None,
         'strategy_symbol': 'BTC'
         }

"""

        zerolag_ma_1  close_1 source_1  released_dt_1     index_ws_1  zerolag_ma_buy_adj_1  zerolag_ma_sell_adj_1
symbol                                                                                                           
BTC          94750.1  94750.1       WS  1745605140000  1745605140000          94738.601111           94928.078313

        trend_indicator_1  trend_signal_1 source_1
symbol                                            
BTC               94471.8               0       WS

"""

strategy = StrategyObelix(param)

columns_trend = ["trend_indicator_1", "trend_signal_1", "source_1"]
df_trend = pd.DataFrame(columns=columns_trend)

columns_zerolag = ["zerolag_ma_1", "close_1", "source_1", "released_dt_1", "index_ws_1", "zerolag_ma_buy_adj_1", "zerolag_ma_sell_adj_1"]
df_zerolag = pd.DataFrame(columns=columns_zerolag)

path = r"C:\Users\despo\PycharmProjects\crag_dev"
fn = "trimmed_49_BITGET_BTCUSDT.csv"
full_path = os.path.join(path, fn)

# 👉 read + set index in one go:
ohlcv = pd.read_csv(full_path, index_col="datetime")
ohlcv["BUY_CRAG"] = False
ohlcv["SELL_CRAG"] = False

for idx, row in ohlcv.iterrows():
    # df1: exactly one row with your three fields
    df1 = pd.DataFrame([{
        "trend_indicator_1": "",
        "trend_signal_1": row["TREND"],
        "source_1": ""
    }])
    df1.index = [strategy.symbol]
    strategy.set_current_data(df1)

    # df2: exactly one row with your seven fields
    df2 = pd.DataFrame([{
        "zerolag_ma_1": row["close"],
        "close_1":      row["close"],
        "source_1":     "",
        "released_dt_1":"",
        "index_ws_1":   "",
        "zerolag_ma_buy_adj_1":  row["buy_adj"],
        "zerolag_ma_sell_adj_1": row["sell_adj"],
    }])
    df2.index = [strategy.symbol]
    strategy.set_current_data(df2)

    buy = strategy.condition_for_opening_long_position(strategy.symbol)
    sell = strategy.condition_for_closing_long_position(strategy.symbol)

    # write them back to ohlcv at this index
    ohlcv.at[idx, "BUY_CRAG"]  = buy
    ohlcv.at[idx, "SELL_CRAG"] = sell

fn = "trimmed_49_BITGET_BTCUSDT_with_BUY_SELL_SIGNAL.csv"
full_path = os.path.join(path, fn)

# 👉 read + set index in one go:
ohlcv.to_csv(full_path)