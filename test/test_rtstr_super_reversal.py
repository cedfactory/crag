import pytest
import pandas as pd
from src import rtstr_super_reversal,rtdp

class TestRTSTRSuperReversal:

    def test_constructor(self):
        # action
        strategy = rtstr_super_reversal.StrategySuperReversal()

        # expectations
        assert(strategy.SL == -0.2)
        assert(strategy.TP == 0.2)
        assert(strategy.TimerSL == -0.1)
        assert(strategy.TimerTP == 0.1)
        assert(strategy.Timer == 12)
        assert(strategy.SPLIT == 10)

    def test_get_data_description(self, mocker):
        # context
        strategy = rtstr_super_reversal.StrategySuperReversal()

        # temporary...
        def get_list_of_actual_prices():
            return [1, 1]
        mocker.patch.object(strategy.rtctrl, "get_list_of_actual_prices", get_list_of_actual_prices)

        # action
        ds = strategy.get_data_description()

        # expectations
        assert(ds.symbols == rtdp.default_symbols)
        assert(ds.features == rtdp.default_features)

    def test_get_df_buying_symbols(self):
        # context
        strategy = rtstr_super_reversal.StrategySuperReversal()
        ds = strategy.get_data_description()
        df_current_data = pd.DataFrame(data={"index":ds.symbols, "symbol":ds.symbols})
        df_current_data.set_index("index", inplace=True)
        strategy.set_current_data(df_current_data)

        # action
        df = strategy.get_df_buying_symbols()

        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(df.columns.to_list() == ['symbol', 'size'])
        assert(len(df.index) == len(ds.symbols))


    def test_get_df_selling_symbols(self):
        # context
        strategy = rtstr_super_reversal.StrategySuperReversal()

        # action
        df = strategy.get_df_selling_symbols([])

        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(df.columns.to_list() == ['symbol', 'size'])
