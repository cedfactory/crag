import pytest
import pandas as pd
from src import rtstr_bigwill,rtdp
from . import test_rtstr

class TestRTSTRBigWill:

    def test_constructor(self):
        # action
        strategy = rtstr_bigwill.StrategyBigWill()

        # expectations
        assert(strategy.willOverSold == -80)
        assert(strategy.willOverBought == -10)
        assert(strategy.AO_Threshold == 0)
        assert(strategy.stochOverBought == 0.8)
        assert(strategy.stochOverSold == 0.2)
        
        assert(strategy.SL == 0)
        assert(strategy.TP == 0)
        assert(strategy.MAX_POSITION == 1)
        assert(strategy.match_full_position == False)

    def test_get_data_description(self, mocker):
        # context
        strategy = rtstr_bigwill.StrategyBigWill()

        # action
        ds = strategy.get_data_description()

        # expectations
        assert(ds.symbols == rtdp.default_symbols)
        #assert(not set(ds.features) ^ set(['ao', 'n1_ao', 'ema_100', 'ema-200', 'stoch_rsi', 'willr']))
        assert (sorted(ds.features) == sorted(['ao', 'n1_ao', 'ema_100', 'ema_200', 'stoch_rsi', 'willr']))

    def _initialize_current_data(self, strategy, data):
        ds = strategy.get_data_description()
        df_current_data = pd.DataFrame(data=data)
        df_current_data.set_index("symbol", inplace=True)
        strategy.set_current_data(df_current_data)
        return strategy

    def test_get_df_buying_symbols(self):
        # context
        strategy = rtstr_bigwill.StrategyBigWill()
        data = {"index":[0, 1], "symbol":["BTC/USD", "ETH/USD"], "low":[1, 2], "high":[0.7, 0.9], "ao":[1, -1], "n1_ao":[2, 2], "ema_100":[50, 50], "ema_200":[25, 25], "stoch_rsi":[1, 1.1], "willr":[-100, -100]}
        strategy = self._initialize_current_data(strategy, data)

        # action
        df = strategy.get_df_buying_symbols()

        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(any(item in df.columns.to_list() for item in ['symbol', 'size', 'percent']))
        assert(len(df) == 1)
        assert(df.iloc[0]['symbol'] == "BTC/USD")
        assert(df.iloc[0]['size'] == 0)
        assert(df.iloc[0]['percent'] == 0)

    def test_get_df_buying_symbols_with_rtctrl(self):
        # context
        strategy = rtstr_bigwill.StrategyBigWill()
        data = {"index":[0, 1], "symbol":["BTC/USD", "ETH/USD"], "low":[1, 2], "high":[0.7, 0.9], "ao":[1, -1], "n1_ao":[2, 2], "ema_100":[50, 50], "ema_200":[25, 25], "stoch_rsi":[1, 1.1], "willr":[-100, -100]}
        strategy = self._initialize_current_data(strategy, data)
        strategy.rtctrl.init_cash_value = 100
        test_rtstr.update_rtctrl(strategy)

        # action
        df = strategy.get_df_buying_symbols()

        # expectations
        assert(isinstance(df, pd.DataFrame))
        print(df.columns.to_list())
        assert(any(item in df.columns.to_list() for item in ['symbol', 'size', 'percent']))
        assert(len(df) == 1)
        assert(df.iloc[0]['symbol'] == "BTC/USD")
        assert(df.iloc[0]['size'] == 0)
        assert(df.iloc[0]['percent'] == 0)       

    def test_get_df_selling_symbols(self):
        return  # TODO : reactivate the test

        # context
        strategy = rtstr_bigwill.StrategyBigWill()
        lst_symbols = ["BTC/USD", "ETH/USD"]
        data = {"index":[0, 1], "symbol":lst_symbols, "low":[-1, 2], "high":[0.7, 0.9], "ao":[1, 1], "n1_ao":[2, 2], "ema_100":[50, 50], "ema_200":[25, 25], "stoch_rsi":[0.3, 0.1], "willr":[-5, -100]}
        strategy = self._initialize_current_data(strategy, data)

        # action
        df = strategy.get_df_selling_symbols(lst_symbols, None)

        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(any(item in df.columns.to_list() for item in ['symbol', 'stimulus']))
        assert(len(df) == 1)
        assert(df.iloc[0]['symbol'] == "BTC/USD")
        assert(df.iloc[0]['stimulus'] == "SELL")
