import pytest
import pandas as pd
from src import rtstr_super_reversal,rtdp

class TestRTSTRSuperReversal:

    def test_constructor(self):
        # action
        strategy = rtstr_super_reversal.StrategySuperReversal()

        # expectations
        assert(strategy.SL == 0)
        assert(strategy.TP == 0)
        assert(strategy.MAX_POSITION == 1)


    def test_constructor_with_params(self):
        # action
        strategy = rtstr_super_reversal.StrategySuperReversal(params = {"sl":-10, "tp": 20})

        # expectations
        assert(strategy.SL == -10)
        assert(strategy.TP == 20)
        assert(strategy.MAX_POSITION == 1)

    def test_get_data_description(self, mocker):
        # context
        strategy = rtstr_super_reversal.StrategySuperReversal()

        # action
        ds = strategy.get_data_description()

        # expectations
        assert(ds.symbols == rtdp.default_symbols)
        assert(not set(list(ds.features.keys())) ^ set(['close', 'ema_long', 'ema_short', 'high', 'low', 'n1_ema_long', 'n1_ema_short', 'n1_super_trend_direction', 'super_reversal', 'super_trend_direction', 'superreversal']))

    def _initialize_current_data(self, strategy, data):
        ds = strategy.get_data_description()
        df_current_data = pd.DataFrame(data=data)
        df_current_data.set_index("symbol", inplace=True)
        strategy.set_current_data(df_current_data)
        return strategy

    ''' deprecated
    def test_get_df_buying_symbols(self):
        # context
        strategy = rtstr_super_reversal.StrategySuperReversal()
        data = {"index":[0, 1], "symbol":["BTC/USD", "ETH/USD"], "low":[1.1, 1.1], "high":[2.2, 2.2], "n1_ema_short":[2.2, 1.9], "n1_ema_long":[2, 2], "n1_super_trend_direction":[True, True]}
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
        strategy = rtstr_super_reversal.StrategySuperReversal()
        lst_symbols = ["BTC/USD"]
        data = {"index":[0], "symbol":lst_symbols, "low":[1.1], "high":[2.2], "ema_5":[1], "ema_400":[2], "super_trend_direction":[True]}
        strategy = self._initialize_current_data(strategy, data)

        # action
        df = strategy.get_df_selling_symbols(lst_symbols, None)

        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(any(item in df.columns.to_list() for item in ['symbol', 'stimulus']))
        assert(len(df) == 1)
        assert(df.iloc[0]['symbol'] == "BTC/USD")
        assert(df.iloc[0]['stimulus'] == "SELL")
    '''
