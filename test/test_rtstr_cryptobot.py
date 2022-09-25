import pytest
import pandas as pd
from src import rtstr_cryptobot,rtdp

class TestRTSTRCryptobot:

    def test_constructor(self):
        # action
        strategy = rtstr_cryptobot.StrategyCryptobot()

        # expectations
        assert(strategy.SL == -1000)
        assert(strategy.TP == 1000)
        assert(strategy.MAX_POSITION == 5)
        assert(strategy.SPLIT == 5)
        assert(strategy.match_full_position == True)

    def test_get_data_description(self, mocker):
        # context
        strategy = rtstr_cryptobot.StrategyCryptobot()

        # action
        ds = strategy.get_data_description()

        # expectations
        assert(ds.symbols == rtdp.default_symbols)
        assert(not set(list(ds.features.keys())) ^ set(['ema12gtema26co', 'macdgtsignal', 'goldencross', 'obv_pc', 'eri_buy', 'ema12ltema26co', 'macdltsignal']))

    def _initialize_current_data(self, strategy, data):
        ds = strategy.get_data_description()
        df_current_data = pd.DataFrame(data=data)
        df_current_data.set_index("symbol", inplace=True)
        strategy.set_current_data(df_current_data)
        return strategy

    def test_get_df_buying_symbols(self):
        # context
        strategy = rtstr_cryptobot.StrategyCryptobot()
        data = {"index":[0, 1], "symbol":["BTC/USD", "ETH/USD"], "ema12gtema26co":[True, False], "macdgtsignal":[True, False], "goldencross":[True, False], "obv_pc":[-4, -6], "eri_buy":[True, False], "ema12ltema26co":[True, False], "macdltsignal":[True, False]}
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
        strategy = rtstr_cryptobot.StrategyCryptobot()
        data = {"index":[0, 1], "symbol":["BTC/USD", "ETH/USD"], "ema12gtema26co":[True, False], "macdgtsignal":[True, False], "goldencross":[True, False], "obv_pc":[-4, -6], "eri_buy":[True, False], "ema12ltema26co":[True, False], "macdltsignal":[True, False]}
        strategy = self._initialize_current_data(strategy, data)

        # action
        df = strategy.get_df_selling_symbols([], None)

        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(any(item in df.columns.to_list() for item in ['symbol', 'stimulus']))
        assert(len(df) == 1)
        assert(df.iloc[0]['symbol'] == "BTC/USD")
        assert(df.iloc[0]['stimulus'] == "SELL")
