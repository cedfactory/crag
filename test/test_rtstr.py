import pytest
from . import test_rtctrl
from src import rtstr
from src import rtstr_bigwill, rtstr_bollinger_trend, rtstr_bollinger_trend_long
from src import rtstr_cryptobot, rtstr_envelope, rtstr_envelopestochrsi
from src import rtstr_grid_protect_long, rtstr_grid_trading_long, rtstr_grid_trading_short
from src import rtstr_super_reversal, rtstr_trix, rtstr_VMC
import pandas as pd

def update_rtctrl(rtstr):
    current_trades = test_rtctrl.get_current_trades_sample()

    # action
    prices_symbols = {'symbol1': 0.01, 'symbol2': 0.02, 'symbol3': 0.03, 'symbol4': 0.04}
    current_datetime = "2022-04-01"
    rtstr.rtctrl.update_rtctrl(current_datetime, current_trades, 100, 100, prices_symbols, None, None)

    return rtstr

class TestRTSTR:

    def test_get_strategies_list(self):
        # action
        available_strategies = rtstr.RealTimeStrategy.get_strategies_list()

        # expectations
        #expected_strategies = ['StrategyDummyTest', 'StrategyEnvelope', 'StrategyEnvelopeStochRSI', 'StrategyDummyTestTP', 'StrategyBollingerTrend', 'StrategyGridTrading', 'StrategyBollingerTrendLong', 'StrategyTvRecommendationMid', 'StrategySuperReversal', 'StrategyVolatilityTest', 'StrategyTrix', 'StrategyCryptobot', 'StrategySLTPOnly', 'StrategyBigWill', 'StrategyVMC']
        expected_strategies = ['StrategyEnvelope', 'StrategyEnvelopeStochRSI',
                               'StrategyBollingerTrend', 'StrategyGridTradingLong',
                               'StrategyGridTradingShort', 'StrategyBollingerTrendLong',
                               'StrategyDummyTest', 'StrategyDummyTestTP', 'StrategySLTPOnly',
                               'StrategyTvRecommendationMid', 'StrategyVolatilityTest',
                               'StrategySuperReversal', 'StrategyTrix', 'StrategyCryptobot',
                               'StrategyGridTradingProtectLong', 'StrategyBigWill', 'StrategyVMC']
        print(expected_strategies)
        print(available_strategies)
        assert(set(available_strategies) == set(expected_strategies))

    def test_get_strategy_from_name_ok(self):
        # action
        strategy = rtstr.RealTimeStrategy.get_strategy_from_name("StrategySuperReversal")

        # expectations
        assert(strategy != None)

    def test_get_strategy_from_name_ko(self):
        # action
        strategy = rtstr.RealTimeStrategy.get_strategy_from_name("StrategyFoobar")

        # expectations
        assert(strategy == None)

    def test_get_df_forced_selling_symbols(self):
        pass
        ''' # TO RESTORE
        # action
        df = rtstr.RealTimeStrategy.get_df_forced_selling_symbols(["SYMBOL1", "SYMBOL2"])

        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert("symbol" in df.columns.tolist())
        assert(df["symbol"].tolist() == ["SYMBOL1", "SYMBOL2"])
        assert("stimulus" in df.columns.tolist())
        assert(df["stimulus"].tolist() == ["SELL", "SELL"])
        '''
