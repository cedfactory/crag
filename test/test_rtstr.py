import pytest
from src import rtstr

class TestRTSTR:

    def test_get_strategies_list(self):
        return
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
                               'StrategyGridTradingBreakOut', 'StrategyBigWill', 'StrategyVMC']
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
