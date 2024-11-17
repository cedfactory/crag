import pytest
from src import rtstr

def get_current_trades_sample():
    current_trades_content = [
        {'type':'SELL', 'symbol':'symbol1', 'size':1, 'buying_fee':0.1, 'gross_price':10},
        {'type':'BUY', 'symbol':'symbol2', 'size':2, 'buying_fee':0.2, 'gross_price':20},
        {'type':'BUY', 'symbol':'symbol2', 'size':3, 'buying_fee':0.3, 'gross_price':30},
        {'type':'BUY', 'symbol':'symbol3', 'size':4, 'buying_fee':0.4, 'gross_price':40}
        ]
    current_trades = []
    for content in current_trades_content:
        current_trade = trade.Trade()
        current_trade.type = content["type"]
        current_trade.symbol = content["symbol"]
        current_trade.net_size = content["size"]
        current_trade.buying_fee = content["buying_fee"]
        current_trade.gross_price = content["gross_price"]
        current_trades.append(current_trade)

    return current_trades

def update_rtctrl(rtstr):
    current_trades = get_current_trades_sample()

    # action
    prices_symbols = {'symbol1': 0.01, 'symbol2': 0.02, 'symbol3': 0.03, 'symbol4': 0.04}
    current_datetime = "2022-04-01"
    rtstr.rtctrl.update_rtctrl(current_datetime, current_trades, 100, 100, prices_symbols, None, None)

    return rtstr

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
