import pandas as pd
from finta import TA # temporary before fdp
import numpy as np
import ta

class SuperTrend():
    def __init__(
        self,
        high,
        low,
        close,
        atr_window=10,
        atr_multi=3
    ):
        self.high = high
        self.low = low
        self.close = close
        self.atr_window = atr_window
        self.atr_multi = atr_multi
        self._run()
        
    def _run(self):
        # calculate ATR
        price_diffs = [self.high - self.low, 
                    self.high - self.close.shift(), 
                    self.close.shift() - self.low]
        true_range = pd.concat(price_diffs, axis=1)
        true_range = true_range.abs().max(axis=1)
        # default ATR calculation in supertrend indicator
        atr = true_range.ewm(alpha=1/self.atr_window,min_periods=self.atr_window).mean() 
        # atr = ta.volatility.average_true_range(high, low, close, atr_period)
        # df['atr'] = df['tr'].rolling(atr_period).mean()
        
        # HL2 is simply the average of high and low prices
        hl2 = (self.high + self.low) / 2
        # upperband and lowerband calculation
        # notice that final bands are set to be equal to the respective bands
        final_upperband = upperband = hl2 + (self.atr_multi * atr)
        final_lowerband = lowerband = hl2 - (self.atr_multi * atr)
        
        # initialize Supertrend column to True
        supertrend = [True] * len(self.close)
        
        for i in range(1, len(self.close)):
            curr, prev = i, i-1
            
            # if current close price crosses above upperband
            if self.close[curr] > final_upperband[prev]:
                supertrend[curr] = True
            # if current close price crosses below lowerband
            elif self.close[curr] < final_lowerband[prev]:
                supertrend[curr] = False
            # else, the trend continues
            else:
                supertrend[curr] = supertrend[prev]
                
                # adjustment to the final bands
                if supertrend[curr] == True and final_lowerband[curr] < final_lowerband[prev]:
                    final_lowerband[curr] = final_lowerband[prev]
                if supertrend[curr] == False and final_upperband[curr] > final_upperband[prev]:
                    final_upperband[curr] = final_upperband[prev]

            # to remove bands according to the trend direction
            if supertrend[curr] == True:
                final_upperband[curr] = np.nan
            else:
                final_lowerband[curr] = np.nan
                
        self.st = pd.DataFrame({
            'Supertrend': supertrend,
            'Final Lowerband': final_lowerband,
            'Final Upperband': final_upperband
        })
        
    def super_trend_upper(self):
        return self.st['Final Upperband']
        
    def super_trend_lower(self):
        return self.st['Final Lowerband']
        
    def super_trend_direction(self):
        return self.st['Supertrend']
    

def add_features(df, features):
    df["ema_short"] = TA.EMA(df, period = 5).copy()
    df["ema_short"] = df["ema_short"].shift(1)
    df["ema_long"] = TA.EMA(df, period = 400).copy()
    df["ema_long"] = df["ema_long"].shift(1)
    super_trend = SuperTrend(
            df['high'], 
            df['low'], 
            df['close'], 
            15, # self.st_short_atr_window
            5 # self.st_short_atr_multiplier
        )
        
    df['super_trend_direction'] = super_trend.super_trend_direction()
    df['super_trend_direction'] = df['super_trend_direction'].shift(1)

    return df

def addOBV(df):
    """Add the On-Balance Volume (OBV) to the DataFrame"""

    df["obv"] = TA.OBV(df)
    df["obv_pc"] = df["obv"].pct_change() * 100
    df["obv_pc"] = round(df["obv_pc"].fillna(0), 2)

    return df

def add_golden_cross(df):
    df['sma50'] = ta.trend.sma_indicator(close=df['close'], window=50)
    df['sma200'] = ta.trend.sma_indicator(close=df['close'], window=200)
    df["goldencross"] = df["sma50"] > df["sma200"]

    return df

def add_macd(df):
    df["ema12"] = ta.trend.ema_indicator(close=df['close'], window=12)
    df["ema26"] = ta.trend.ema_indicator(close=df['close'], window=26)

    df["macd"] = df["ema12"] - df["ema26"]
    df["signal"] = df["macd"].ewm(span=9, adjust=False).mean()

    # true if MACD is above the Signal
    df["macdgtsignal"] = df["macd"] > df["signal"]
    # true if the current frame is where MACD crosses over above
    df["macdgtsignalco"] = df["macdgtsignal"].ne(df["macdgtsignal"].shift())
    df.loc[df["macdgtsignal"] == False, "macdgtsignalco"] = False

    # true if the MACD is below the Signal
    df["macdltsignal"] = df["macd"] < df["signal"]
    # true if the current frame is where MACD crosses over below
    df["macdltsignalco"] = df["macdltsignal"].ne(df["macdltsignal"].shift())
    df.loc[df["macdltsignal"] == False, "macdltsignalco"] = False

    return df

def add_ema_cross_over(df, window1=12, window2=26):
    ema_window1 = "ema" + str(window1)
    ema_window2 = "ema" + str(window2)
    df[ema_window1] = ta.trend.ema_indicator(close=df['close'], window=window1)
    df[ema_window2] = ta.trend.ema_indicator(close=df['close'], window=window2)
    # true if EMA12 is above the EMA26
    ema_greater_than = "ema" + str(window1) + "gtema" + str(window2)
    df[ema_greater_than] = df[ema_window1] > df[ema_window2]
    # true if the current frame is where SMA12 crosses over above
    ema_greater_than_co = "ema" + str(window1) + "gtema" + str(window2) + "co"
    df[ema_greater_than_co] = df[ema_greater_than].ne(df[ema_greater_than].shift())
    df.loc[df[ema_greater_than] == False, ema_greater_than_co] = False

    # true if the EMA12 is below the EMA26
    ema_lower_than = "ema" + str(window1) + "ltema" + str(window2)
    df[ema_lower_than] = df[ema_window1] < df[ema_window2]
    # true if the current frame is where EMA12 crosses over below
    ema_lower_than_co = "ema" + str(window1) + "ltema" + str(window2) + "co"
    df[ema_lower_than_co] = df[ema_lower_than].ne(df[ema_lower_than].shift())
    df.loc[df[ema_lower_than] == False, ema_lower_than_co] = False

    return df

def add_trix_indicators(df):
    trixLength = 9
    trixSignal = 21
    df['TRIX'] = ta.trend.ema_indicator(
        ta.trend.ema_indicator(ta.trend.ema_indicator(close=df['close'], window=trixLength),
                               window=trixLength), window=trixLength)
    df['TRIX_PCT'] = df["TRIX"].pct_change() * 100
    df['TRIX_SIGNAL'] = ta.trend.sma_indicator(df['TRIX_PCT'], trixSignal)
    df['TRIX_HISTO'] = df['TRIX_PCT'] - df['TRIX_SIGNAL']

    # -- Stochasitc RSI --
    df['STOCH_RSI'] = ta.momentum.stochrsi(close=df['close'], window=14, smooth1=3, smooth2=3)

    return df


def addElderRayIndex(df):
    """Add Elder Ray Index"""

    if "ema13" not in df:
        df["ema13"] = ta.trend.ema_indicator(close=df['close'], window=13)

    df["elder_ray_bull"] = df["high"] - df["ema13"]
    df["elder_ray_bear"] = df["low"] - df["ema13"]

    # bear power’s value is negative but increasing (i.e. becoming less bearish)
    # bull power’s value is increasing (i.e. becoming more bullish)
    df["eri_buy"] = (
        (df["elder_ray_bear"] < 0)
        & (df["elder_ray_bear"] > df["elder_ray_bear"].shift(1))
    ) | ((df["elder_ray_bull"] > df["elder_ray_bull"].shift(1)))

    # bull power’s value is positive but decreasing (i.e. becoming less bullish)
    # bear power’s value is decreasing (i.e., becoming more bearish)
    df["eri_sell"] = (
        (df["elder_ray_bull"] > 0)
        & (df["elder_ray_bull"] < df["elder_ray_bull"].shift(1))
    ) | ((df["elder_ray_bear"] < df["elder_ray_bear"].shift(1)))

    return df