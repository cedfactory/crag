import pytest
import pandas as pd
from src import rtstr_VMC,rtdp

class TestRTSTRVMC:

    def test_constructor(self):
        # action
        strategy = rtstr_VMC.StrategyVMC()

        # expectations
        assert(strategy.positive_Threshold == 0)
        assert(strategy.chop_Threshold == 50)
        assert(strategy.AO_Threshold == 0)
        assert(strategy.stochOverSold == 0.2)
        assert(strategy.willOverBought == -10)
        
        assert(strategy.SL == -1000)
        assert(strategy.TP == 1000)
        assert(strategy.MAX_POSITION == 5)
        assert(strategy.SPLIT == 5)
        assert(strategy.match_full_position == True)

    def test_get_data_description(self, mocker):
        # context
        strategy = rtstr_VMC.StrategyVMC()

        # action
        ds = strategy.get_data_description()

        # expectations
        expected_features = ["low", "high", "close", "AO", "previous_AO", "STOCH_RSI", "ema_short_vmc", "ema_long_vmc", "MONEY_FLOW", "VMC_WAVE1", "VMC_WAVE2", "n1_VMC_WAVE1", "n1_VMC_WAVE2", "CHOP", "WILLR"]
        assert(ds.symbols == rtdp.default_symbols)
        assert(not set(list(ds.features.keys())) ^ set(expected_features))

    def _initialize_current_data(self, strategy, data):
        ds = strategy.get_data_description()
        df_current_data = pd.DataFrame(data=data)
        df_current_data.set_index("symbol", inplace=True)
        strategy.set_current_data(df_current_data)
        return strategy

    def _get_strategy_for_buying_and_selling(self):
        strategy = rtstr_VMC.StrategyVMC()
        data = {"index":[0, 1], "symbol":["BTC/USD", "ETH/USD"],
            "low":[1, 1],
            "high":[1, 1],
            "close":[3, 1],
            "AO":[-1, -1],
            "previous_AO":[1000, 1000],
            "STOCH_RSI":[0.3, 0.1],
            "ema_short_vmc":[2, 0],
            "ema_long_vmc":[1, 1],
            "MONEY_FLOW":[1, -1],
            "VMC_WAVE1":[-1, 1],
            "VMC_WAVE2":[-2, 1],
            "n1_VMC_WAVE1":[1, 2],
            "n1_VMC_WAVE2":[2, 1],
            "CHOP":[40, 60],
            "WILLR":[-5, -15]
        }
        strategy = self._initialize_current_data(strategy, data)
        return strategy
    
    def test_get_df_buying_symbols(self):
        # context
        strategy = self._get_strategy_for_buying_and_selling()

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
        strategy = self._get_strategy_for_buying_and_selling()

        # action
        df = strategy.get_df_selling_symbols([], None)

        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(any(item in df.columns.to_list() for item in ['symbol', 'stimulus']))
        assert(len(df) == 1)
        assert(df.iloc[0]['symbol'] == "BTC/USD")
        assert(df.iloc[0]['stimulus'] == "SELL")
