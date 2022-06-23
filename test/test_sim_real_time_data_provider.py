import pytest
import pandas as pd
from src import rtdp,rtdp_simulation

class TestSimRealTimeDataProvider:

    def test_constructor(self):
        # context
        input = "./test/data_sim_real_time_data_provider"

        # action
        dp = rtdp_simulation.SimRealTimeDataProvider({'input': input})

        # expectations
        assert(dp.input == input)
        assert(dp.current_position == 400)
        assert(len(dp.data) == 1)
        assert("AAVE/USD" in dp.data)
        df = dp.data["AAVE/USD"]
        assert(isinstance(df, pd.DataFrame))
        expected_columns = ['Unnamed: 0', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'ema_short', 'ema_long', 'super_trend_direction']
        assert(df.columns.to_list() == expected_columns)
        assert(len(df) == 402)

    def test_get_value_last_in_dataframe(self):
        # context
        input = "./test/data_sim_real_time_data_provider"
        dp = rtdp_simulation.SimRealTimeDataProvider({'input': input})
        ds = rtdp.DataDescription()
        ds.symbols = ["AAVE/USD"]
        ds.features = ["high", "low"]

        # action
        dp.tick()
        dp.tick()
        dp.get_current_data(ds)
        value = dp.get_value("AAVE/USD")
        
        # expectations
        assert(value == 196.48)

    def test_get_value_ko_bad_symbol(self):
        # context
        input = "./test/data_sim_real_time_data_provider"
        dp = rtdp_simulation.SimRealTimeDataProvider({'input': input})

        # action
        value = dp.get_value("FOOBAR/USD")
        
        # expectations
        assert(value == -1)

    def test_get_value_ok(self):
        # context
        input = "./test/data_sim_real_time_data_provider"
        dp = rtdp_simulation.SimRealTimeDataProvider({'input': input})
        ds = rtdp.DataDescription()
        ds.symbols = ["AAVE/USD"]
        ds.features = ["high", "low"]

        # action
        value = dp.get_value("AAVE/USD")
        
        # expectations
        assert(value == 189.53)

    def test_get_current_datetime_ok(self):
        # context
        input = "./test/data_sim_real_time_data_provider"
        dp = rtdp_simulation.SimRealTimeDataProvider({'input': input})
        ds = rtdp.DataDescription()
        ds.symbols = ["AAVE/USD"]
        ds.features = ["high", "low"]

        # action
        current_time = dp.get_current_datetime()
        
        # expectations
        assert(current_time == "2021-01-17 15:00:00")

    def test_get_current_data(self):
        # context
        input = "./test/data_sim_real_time_data_provider"
        dp = rtdp_simulation.SimRealTimeDataProvider({'input': input})
        ds = rtdp.DataDescription()
        ds.symbols = ["AAVE/USD"]
        ds.features = ["high", "low"]

        # action
        dp.tick()
        df = dp.get_current_data(ds)
        
        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(len(df.index) == 1)
        assert(df["high"]["AAVE/USD"] == 199.01)
        assert(df["low"]["AAVE/USD"] == 189.53)
