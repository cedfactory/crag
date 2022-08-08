import pytest
from . import test_rtctrl
from src import rtstr,rtstr_bigwill,rtstr_cryptobot,rtstr_super_reversal,rtstr_trix,rtstr_VMC


def update_rtctrl(rtstr):
    current_trades = test_rtctrl.get_current_trades_sample()

    # action
    prices_symbols = {'symbol1': 0.01, 'symbol2': 0.02, 'symbol3': 0.03, 'symbol4': 0.04}
    current_datetime = "2022-04-01"
    rtstr.rtctrl.update_rtctrl(current_datetime, current_trades, 100, prices_symbols, None)

    return rtstr

class TestRTSTR:

    def test_get_strategies_list(self):
        # action
        available_strategies = rtstr.RealTimeStrategy.get_strategies_list()

        # expectations
        expected_strategies = ['StrategyBigWill', 'StrategyCryptobot', 'StrategySuperReversal', 'StrategyTrix', 'StrategyVMC']
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
