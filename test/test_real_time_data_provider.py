import pytest
import pandas as pd
from src import rtdp

class TestRealTimeDataProvider:
    src_utils_fdp_request = 'src.utils.fdp_request'
    fdp_response = {'result': {'ETH_EURS': {'status': 'ok', 'info': '{"index":{"0":1638662400000},"open":{"0":3525.0},"high":{"0":3732.8855,},"low":{"0":3460.0},"close":{"0":3732.8855},"volume":{"0":0.3798}}'}, 'BTC_EURS': {'status': 'ok', 'info': '{"index":{"0":1638662400000},"open":{"0":43388.73},"high":{"0":43481.66},"low":{"0":42105.38},"close":{"0":43035.5},"volume":{"0":0.00628}}'}}, 'status': 'ok', 'elapsed_time': '0:00:00.476569'}

    def test_get_current_data(self, mocker):
        # context
        dp = rtdp.RealTimeDataProvider()
        ds = rtdp.DataDescription()
        ds.symbols = ["ETH/EURS", "BTC/EURS"]
        ds.features = ["open", "high", "low", "close"]
        mocker.patch(self.src_utils_fdp_request, side_effect=[self.fdp_response])

        # action
        df = dp.get_current_data(ds)

        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(df.columns.to_list() == ['open', 'high', 'low', 'close'])
        assert("ETH/EURS" in df.index)
        assert(df['open']['ETH/EURS'] == 3525.00)
        assert(df['high']['ETH/EURS'] == 3732.8855)
        assert(df['low']['ETH/EURS'] == 3460.00)
        assert(df['close']['ETH/EURS'] == 3732.8855)

        assert("BTC/EURS" in df.index)
        assert(df['open']['BTC/EURS'] == 43388.73)
        assert(df['high']['BTC/EURS'] == 43481.6600)
        assert(df['low']['BTC/EURS'] == 42105.38)
        assert(df['close']['BTC/EURS'] == 43035.5000)

    def test_get_current_data_ko_no_response_json(self, mocker):
        # context
        dp = rtdp.RealTimeDataProvider()
        ds = rtdp.DataDescription()
        ds.symbols = ["ETH/EURS", "BTC/EURS"]
        ds.features = ["open", "high", "low", "close"]
        mocker.patch(self.src_utils_fdp_request, side_effect=[{"status":"ko"}])

        # action
        df = dp.get_current_data(ds)

        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(df.columns.to_list() == ['open', 'high', 'low', 'close'])
        assert("ETH/EURS" not in df.index)
        assert("BTC/EURS" not in df.index)


    def test_get_value(self):
        # context
        dp = rtdp.RealTimeDataProvider()

        # action
        value = dp.get_value("AAVE/USD")

        # expectations
        assert(value == None)

