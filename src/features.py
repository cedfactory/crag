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

'''
class VMC_strat():
    def __init__(
            self,
            df_list,
            oldest_pair,
            EMA_long_window=200,
            EMA_short_window=50,
            chop_window=14,
            take_profit=0.05,
            stop_loss=0.05
    ):
        self.df_list = df_list
        self.oldest_pair = oldest_pair
        self.EMA_long_window = EMA_long_window
        self.EMA_short_window = EMA_short_window
        self.chop_window = chop_window
        self.take_profit = take_profit
        self.stop_loss = stop_loss

    def populate_indicators(self, show_log=False):
        # -- Clear dataset --
        for pair in self.df_list:
            df = self.df_list[pair]
            df.drop(columns=df.columns.difference(['open', 'high', 'low', 'close', 'volume']), inplace=True)

            # -- Populate indicators --

            df['HLC3'] = (df['high'] + df['close'] + df['low']) / 3
            vmc = VMC(high=df['high'], low=df['low'], close=df['HLC3'], open=df['open'])
            df['VMC_WAVE1'] = vmc.wave_1()
            df['VMC_WAVE2'] = vmc.wave_2()
            vmc = VMC(high=df['high'], low=df['low'], close=df['close'], open=df['open'])
            df['MONEY_FLOW'] = vmc.money_flow()

            df['ema_short'] = ta.trend.ema_indicator(close=df['close'], window=self.EMA_short_window)
            df['ema_long'] = ta.trend.ema_indicator(close=df['close'], window=self.EMA_long_window)

            df["CHOP"] = chop(df['high'], df['low'], df['close'], window=self.chop_window)

            df = get_n_columns(df, ["VMC_WAVE1", "VMC_WAVE2", "MONEY_FLOW", "ema_short", "ema_long", "CHOP"], 1)

            self.df_list[pair] = df
            # -- Log --
            if (show_log):
                print(self.df_list[self.oldest_pair])

        return self.df_list[self.oldest_pair]
'''

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
    df['super_trend_direction'] = df['super_trend_direction'].shift(1)

    # -- Trix Indicator --
    df = add_trix_indicators(df)

    # -- cryptobot -- #
    # add sma cross over 12/26
    df = add_ema_cross_over(df, 12, 26)

    # add macd
    df = add_macd(df)

    # add golden cross
    df = add_golden_cross(df)

    # Add the On-Balance Volume (OBV)
    df = addOBV(df)

    # Add Elder Ray Index
    df = addElderRayIndex(df)

    # BIGWILL features
    df = addAO(df, 6, 22)
    df = addEMA100(df)
    df = addEMA200(df)
    df = addSTOCHRSI(df, 14)
    df = addWILLR(df, 14)

    # VuManChu Cipher B
    df = add_feature_VMC(df, 200, 50, 14)

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


# BIGWILL features
def addAO(df, aoParam1=6, aoParam2=22):
    df['AO'] = ta.momentum.awesome_oscillator(df['high'], df['low'], window1=aoParam1, window2=aoParam2)
    df['previous_AO'] = df['AO'].shift(1)
    return df

# DEBUG generic EMA could be used for to avoid errors for now...
def addEMA100(df):
    df['EMA100'] = ta.trend.ema_indicator(close=df['close'], window=100)
    return df

def addEMA200(df):
    df['EMA200'] = ta.trend.ema_indicator(close=df['close'], window=200)
    return df

def addSTOCHRSI(df, stochWindow=14):
    df['STOCH_RSI'] = ta.momentum.stochrsi(close=df['close'], window=stochWindow)
    return df

def addWILLR(df, willWindow=14):
    df['WILLR'] = ta.momentum.williams_r(high=df['high'], low=df['low'], close=df['close'], lbp=willWindow)
    return df

class VMC():
    """ VuManChu Cipher B + Divergences
        Args:
            high(pandas.Series): dataset 'High' column.
            low(pandas.Series): dataset 'Low' column.
            close(pandas.Series): dataset 'Close' column.
            wtChannelLen(int): n period.
            wtAverageLen(int): n period.
            wtMALen(int): n period.
            rsiMFIperiod(int): n period.
            rsiMFIMultiplier(int): n period.
            rsiMFIPosY(int): n period.
    """

    def __init__(
        self: pd.Series,
        open: pd.Series,
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        wtChannelLen: int = 9,
        wtAverageLen: int = 12,
        wtMALen: int = 3,
        rsiMFIperiod: int = 60,
        rsiMFIMultiplier: int = 150,
        rsiMFIPosY: int = 2.5
    ) -> None:
        self._high = high
        self._low = low
        self._close = close
        self._open = open
        self._wtChannelLen = wtChannelLen
        self._wtAverageLen = wtAverageLen
        self._wtMALen = wtMALen
        self._rsiMFIperiod = rsiMFIperiod
        self._rsiMFIMultiplier = rsiMFIMultiplier
        self._rsiMFIPosY = rsiMFIPosY

        self._run()
        self.wave_1()

    def _run(self) -> None:
        self.hlc3 = (self._close + self._high + self._low)
        self._esa = ta.trend.ema_indicator(
            close=self.hlc3, window=self._wtChannelLen)
        self._de = ta.trend.ema_indicator(
            close=abs(self.hlc3 - self._esa), window=self._wtChannelLen)
        self._rsi = ta.trend.sma_indicator(self._close, self._rsiMFIperiod)
        self._ci = (self.hlc3 - self._esa) / (0.015 * self._de)

    def wave_1(self) -> pd.Series:
        """VMC Wave 1
        Returns:
            pandas.Series: New feature generated.
        """
        wt1 = ta.trend.ema_indicator(self._ci, self._wtAverageLen)
        return pd.Series(wt1, name="wt1")

    def wave_2(self) -> pd.Series:
        """VMC Wave 2
        Returns:
            pandas.Series: New feature generated.
        """
        wt2 = ta.trend.sma_indicator(self.wave_1(), self._wtMALen)
        return pd.Series(wt2, name="wt2")

    def money_flow(self) -> pd.Series:
        """VMC Money Flow
        Returns:
            pandas.Series: New feature generated.
        """
        mfi = ((self._close - self._open) /
               (self._high - self._low)) * self._rsiMFIMultiplier
        rsi = ta.trend.sma_indicator(mfi, self._rsiMFIperiod)
        money_flow = rsi - self._rsiMFIPosY
        return pd.Series(money_flow, name="money_flow")

def chop(high, low, close, window=14):
    # Choppiness indicator

    tr1 = pd.DataFrame(high - low).rename(columns={0: 'tr1'})
    tr2 = pd.DataFrame(abs(high - close.shift(1))
                       ).rename(columns={0: 'tr2'})
    tr3 = pd.DataFrame(abs(low - close.shift(1))
                       ).rename(columns={0: 'tr3'})
    frames = [tr1, tr2, tr3]
    tr = pd.concat(frames, axis=1, join='inner').dropna().max(axis=1)
    atr = tr.rolling(1).mean()
    highh = high.rolling(window).max()
    lowl = low.rolling(window).min()
    chop_serie = 100 * np.log10((atr.rolling(window).sum()) /
                          (highh - lowl)) / np.log10(window)

    return pd.Series(chop_serie, name="CHOP")

def add_feature_VMC(df, EMA_long_window=200, EMA_short_window=50,chop_window=14):

    # -- Populate indicators --
    df['HLC3'] = (df['high'] + df['close'] + df['low']) / 3

    vmc = VMC(high=df['high'], low=df['low'], close=df['HLC3'], open=df['open'])

    df['VMC_WAVE1'] = vmc.wave_1()
    df['VMC_WAVE2'] = vmc.wave_2()
    vmc = VMC(high=df['high'], low=df['low'], close=df['close'], open=df['open'])
    df['MONEY_FLOW'] = vmc.money_flow()

    df['ema_short_vmc'] = ta.trend.ema_indicator(close=df['close'], window=EMA_short_window)
    df['ema_long_vmc'] = ta.trend.ema_indicator(close=df['close'], window=EMA_long_window)

    df["CHOP"] = chop(df['high'], df['low'], df['close'], window=chop_window)

    df = get_n_columns(df, ['VMC_WAVE1', 'VMC_WAVE2'], n=1)

    return df

def get_n_columns(df, columns, n=1):
    dt = df.copy()
    for col in columns:
        dt["n"+str(n)+"_"+col] = dt[col].shift(n)
    return dt
