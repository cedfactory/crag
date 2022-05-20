import pytest
import pandas as pd
from src import rtdp

class TestSimRealTimeDataProvider:

    def test_constructor(self):
        # context
        input = "./test/data_sim_real_time_data_provider"

        # action
        dp = rtdp.SimRealTimeDataProvider({'input': input})

        # expectations
        assert(dp.input == input)
        assert(dp.offset == 0)
        assert(dp.current_position == -1)
        assert(len(dp.data) == 1)
        assert("AAVE/USD" in dp.data)
        df = dp.data["AAVE/USD"]
        assert(isinstance(df, pd.DataFrame))
        assert(df.columns.to_list() == ['Unnamed: 0', 'open', 'high', 'low', 'close', 'volume'])
        assert(len(df) == 47)
