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
        assert(dp.current_position == -1)
        assert(len(dp.data) == 1)
        assert("AAVE/USD" in dp.data)
        df = dp.data["AAVE/USD"]
        assert(isinstance(df, pd.DataFrame))
        assert(df.columns.to_list() == ['datetime', 'open', 'high', 'low', 'close', 'volume'])
        assert(len(df) == 47)

    def test_get_value_ko_bad_current_position(self):
        # context
        input = "./test/data_sim_real_time_data_provider"
        dp = rtdp.SimRealTimeDataProvider({'input': input})

        # action
        value = dp.get_value("AAVE/USD")
        
        # expectations
        assert(value == -1)

    def test_get_value_ko_bad_symbol(self):
        # context
        input = "./test/data_sim_real_time_data_provider"
        dp = rtdp.SimRealTimeDataProvider({'input': input})

        # action
        value = dp.get_value("FOOBAR/USD")
        
        # expectations
        assert(value == -1)

    def test_get_value_ok(self):
        # context
        input = "./test/data_sim_real_time_data_provider"
        dp = rtdp.SimRealTimeDataProvider({'input': input})
        ds = rtdp.DataDescription()
        ds.symbols = ["AAVE/USD"]
        ds.features = ["high", "low"]
        dp.next(ds)

        # action
        value = dp.get_value("AAVE/USD")
        
        # expectations
        assert(value == 244.53)

    def test_get_current_datetime_ok(self):
        # context
        input = "./test/data_sim_real_time_data_provider"
        dp = rtdp.SimRealTimeDataProvider({'input': input})
        ds = rtdp.DataDescription()
        ds.symbols = ["AAVE/USD"]
        ds.features = ["high", "low"]
        dp.next(ds)

        # action
        current_time = dp.get_current_datetime()
        print(current_time)
        
        # expectations
        assert(current_time == "2022-04-01")

    def test_next(self):
        # context
        input = "./test/data_sim_real_time_data_provider"
        dp = rtdp.SimRealTimeDataProvider({'input': input})
        ds = rtdp.DataDescription()
        ds.symbols = ["AAVE/USD"]
        ds.features = ["high", "low"]

        # action
        df = dp.next(ds)
        
        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(len(df.index) == 1)
        assert(df["high"]["AAVE/USD"] == 261.29)
        assert(df["low"]["AAVE/USD"] == 206.25)
        
    def test_next_next(self):
        # context
        input = "./test/data_sim_real_time_data_provider"
        dp = rtdp.SimRealTimeDataProvider({'input': input})
        ds = rtdp.DataDescription()
        ds.symbols = ["AAVE/USD"]
        ds.features = ["high", "low"]

        # action
        df = dp.next(ds)
        df = dp.next(ds)
        print(df)
        
        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(len(df.index) == 1)
        assert(df["high"]["AAVE/USD"] == 258.02)
        assert(df["low"]["AAVE/USD"] == 235.4)
        value = dp.get_value("AAVE/USD")
        assert(value == 238.01)
        