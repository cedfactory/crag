import pytest
import pandas as pd
from src import rtdp

class TestRealTimeDataProvider:
    src_utils_fdp_request = 'src.utils.fdp_request'
    fdp_response = {'elapsed_time': '0:00:02.537734', 'result': {'status': 'ok', 'AAVE_USD': {'info':'{"symbol":{"0":"AAVE/USD"},"open":{"0":"10.0"},"high":{"0":"12.0"},"low":{"0":"9.0"},"close":{"0":"11.0"}}'}}, 'status': 'ok'}

    def test_next(self, mocker):
        #data = [['AAVE/USD', 10., 12., 9., 11.]]
        #df = pd.DataFrame(data, columns = ['symbol', 'open', 'high', 'low', 'close'])

        # context
        dp = rtdp.RealTimeDataProvider()
        ds = rtdp.DataDescription()
        ds.symbols = ["AAVE/USD"]
        ds.features = ["open", "high", "low", "close"]
        mocker.patch(self.src_utils_fdp_request, side_effect=[self.fdp_response])

        # action
        df = dp.next(ds)

        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(df.columns.to_list() == ['open', 'high', 'low', 'close'])
        assert(df['open']['AAVE/USD'] == 10.0)
        assert(df['high']['AAVE/USD'] == 12.0)
        assert(df['low']['AAVE/USD'] == 9.0)
        assert(df['close']['AAVE/USD'] == 11.0)

    def test_get_value(self):
        # context
        dp = rtdp.RealTimeDataProvider()

        # action
        value = dp.get_value("AAVE/USD")

        # expectations
        assert(value == None)

