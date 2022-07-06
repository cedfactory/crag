import pytest
import pandas as pd
from src import rtstr_bigwill,rtdp

class TestRTSTRBigWill:

    def test_constructor(self):
        # action
        strategy = rtstr_bigwill.StrategyBigWill()

        # expectations
        assert(strategy.willOverSold == -85)
        assert(strategy.willOverBought == -10)
        assert(strategy.AO_Threshold == 0)
        assert(strategy.stochOverBought == 0.8)
        assert(strategy.stochOverSold == 0.2)
        
        assert(strategy.SL == -1000)
        assert(strategy.TP == 1000)
        assert(strategy.MAX_POSITION == 5)
        assert(strategy.SPLIT == 5)
        assert(strategy.match_full_position == True)

    def test_get_data_description(self, mocker):
        # context
        strategy = rtstr_bigwill.StrategyBigWill()

        # action
        ds = strategy.get_data_description()

        # expectations
        assert(ds.symbols == rtdp.default_symbols)
        assert(not set(list(ds.features.keys())) ^ set(['low', 'high', 'AO', 'previous_AO', 'EMA100', 'EMA200', 'STOCH_RSI', 'WILLR']))

    def _initialize_current_data(self, strategy, data):
        ds = strategy.get_data_description()
        df_current_data = pd.DataFrame(data=data)
        df_current_data.set_index("symbol", inplace=True)
        strategy.set_current_data(df_current_data)
        return strategy

    def test_get_df_buying_symbols(self):
        # context
        strategy = rtstr_bigwill.StrategyBigWill()
        data = {"index":[0, 1], "symbol":["BTC/USD", "ETH/USD"], "low":[1, 2], "high":[0.7, 0.9], "AO":[1, -1], "previous_AO":[2, 2], "EMA100":[50, 50], "EMA200":[25, 25], "STOCH_RSI":[1, 1.1], "WILLR":[-100, -100]}
        strategy = self._initialize_current_data(strategy, data)

        # action
        df = strategy.get_df_buying_symbols()

        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(df.columns.to_list() == ['symbol', 'size', 'percent'])
        assert(len(df) == 1)
        assert(df.iloc[0]['symbol'] == "BTC/USD")
        assert(df.iloc[0]['size'] == 0)
        assert(df.iloc[0]['percent'] == 0)

    def test_get_df_selling_symbols(self):
        # context
        strategy = rtstr_bigwill.StrategyBigWill()
        data = {"index":[0, 1], "symbol":["BTC/USD", "ETH/USD"], "low":[-1, 2], "high":[0.7, 0.9], "AO":[1, 1], "previous_AO":[2, 2], "EMA100":[50, 50], "EMA200":[25, 25], "STOCH_RSI":[0.3, 0.1], "WILLR":[-5, -100]}
        strategy = self._initialize_current_data(strategy, data)

        # action
        df = strategy.get_df_selling_symbols([], None)

        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(df.columns.to_list() == ['symbol', 'stimulus'])
        assert(len(df) == 1)
        assert(df.iloc[0]['symbol'] == "BTC/USD")
        assert(df.iloc[0]['stimulus'] == "SELL")
