import pytest
import pandas as pd
from src import rtstr_trix,rtdp

class TestRTSTRTrix:

    def test_constructor(self):
        # action
        strategy = rtstr_trix.StrategyTrix()

        # expectations
        assert(strategy.SL == -1000)
        assert(strategy.TP == 1000)
        assert(strategy.MAX_POSITION == 5)
        assert(strategy.SPLIT == 5)
        assert(strategy.match_full_position == True)

    def test_get_data_description(self, mocker):
        # context
        strategy = rtstr_trix.StrategyTrix()

        # action
        ds = strategy.get_data_description()

        # expectations
        assert(ds.symbols == rtdp.default_symbols)
        assert(not set(list(ds.features.keys())) ^ set(['TRIX_HISTO', 'STOCH_RSI']))

    def _initialize_current_data(self, strategy, data):
        ds = strategy.get_data_description()
        df_current_data = pd.DataFrame(data=data)
        df_current_data.set_index("symbol", inplace=True)
        strategy.set_current_data(df_current_data)
        return strategy

    def test_get_df_buying_symbols(self):
        # context
        strategy = rtstr_trix.StrategyTrix()
        data = {"index":[0, 1, 2, 3], "symbol":["BTC/USD", "ETH/USD", "BNB/USD", "SUSHI/USD"], "TRIX_HISTO":[1, 1.1, -1, -1.1], "STOCH_RSI":[0.7, 0.9, 0.7, 0.9]}
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

    def test_get_df_selling_symbols(self):
        # context
        strategy = rtstr_trix.StrategyTrix()
        data = {"index":[0, 1, 2, 3], "symbol":["BTC/USD", "ETH/USD", "BNB/USD", "SUSHI/USD"], "TRIX_HISTO":[-1, -1.1, 1, 1.1], "STOCH_RSI":[0.3, 0.1, 0.3, 0.1]}
        strategy = self._initialize_current_data(strategy, data)

        # action
        df = strategy.get_df_selling_symbols([], None)

        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(any(item in df.columns.to_list() for item in ['symbol', 'stimulus']))
        assert(len(df) == 1)
        assert(df.iloc[0]['symbol'] == "BTC/USD")
        assert(df.iloc[0]['stimulus'] == "SELL")
