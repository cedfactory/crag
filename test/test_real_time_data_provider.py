import pytest
import pandas as pd
from src import rtdp

class TestRealTimeDataProvider:
    src_utils_fdp_request_post = 'src.utils.fdp_request_post'
    fdp_response = {'result': {'ETH_EURS': {'status': 'ok', 'info': '{"index":{"0":1638662400000},"open":{"0":3525.0},"high":{"0":3732.8855,},"low":{"0":3460.0},"close":{"0":3732.8855},"volume":{"0":0.3798}}'}, 'BTC_EURS': {'status': 'ok', 'info': '{"index":{"0":1638662400000},"open":{"0":43388.73},"high":{"0":43481.66},"low":{"0":42105.38},"close":{"0":43035.5},"volume":{"0":0.00628}}'}}, 'status': 'ok', 'elapsed_time': '0:00:00.476569'}

    def test_get_lst_current_data(self, mocker):
        # context
        dp = rtdp.RealTimeDataProvider()
        ds = rtdp.DataDescription()
        ds.symbols = ["ETH/EURS", "BTC/EURS"]
        ds.fdp_features = ["open", "high", "low", "close"]
        ds.features = ["open", "high", "low", "close"]
        ds.interval = 20
        ds.str_interval = "1h"
        mocker.patch(self.src_utils_fdp_request_post, side_effect=[self.fdp_response])

        # action
        fdp_url_id = ""
        lst_cd = dp.get_lst_current_data([ds], fdp_url_id)

        # expectations
        assert(isinstance(lst_cd, list))
        assert(len(lst_cd) == 1)
        dd = lst_cd[0]
        df = dd.current_data
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

    def test_get_lst_current_data_ko_no_response_json(self, mocker):
        # context
        dp = rtdp.RealTimeDataProvider()
        ds = rtdp.DataDescription()
        ds.symbols = ["ETH/EURS", "BTC/EURS"]
        ds.fdp_features = ["open", "high", "low", "close"]
        ds.features = ["open", "high", "low", "close"]
        ds.interval = 20
        ds.str_interval = "1h"
        mocker.patch(self.src_utils_fdp_request_post, side_effect=[{"status":"ko"}])

        # action
        fdp_url_id = ""
        lst_cd = dp.get_lst_current_data([ds], fdp_url_id)

        # expectations
        assert(isinstance(lst_cd, list))
        assert(len(lst_cd) == 0)

