import pytest
import pandas as pd
from src import rtdp,rtdp_simulation, chronos

class TestSimRealTimeDataProvider:

    def test_constructor(self):
        # context
        input = "./test/data_sim_real_time_data_provider"

        # action
        dp = rtdp_simulation.SimRealTimeDataProvider({'input': input, 'chronos': chronos.Chronos()})

        # expectations
        assert(dp.input == input)
        assert(dp.current_position == 400)
        assert(len(dp.data) == 1)
        assert("AAVE/USD" in dp.data)
        df = dp.data["AAVE/USD"]
        assert(isinstance(df, pd.DataFrame))
        assert(df.columns.to_list() == ["timestamp", "datetime", "open", "high", "low", "close", "volume", "ema_short", "ema_long", "super_trend_direction"])
        assert(len(df) == 402)

    def test_get_value_last_in_dataframe(self):
        # context
        input = "./test/data_sim_real_time_data_provider"
        dp = rtdp_simulation.SimRealTimeDataProvider({'input': input, 'chronos': chronos.Chronos()})
        ds = rtdp.DataDescription()
        ds.symbols = ["AAVE/USD"]
        ds.features = ["high", "low"]

        # action
        dp.next(ds)
        dp.next(ds)
        dp.next(ds)
        value = dp.get_value("AAVE/USD")
        
        # expectations
        assert(value == 189.53)

    def test_get_value_ko_bad_symbol(self):
        # context
        input = "./test/data_sim_real_time_data_provider"
        dp = rtdp_simulation.SimRealTimeDataProvider({'input': input, 'chronos': chronos.Chronos()})

        # action
        value = dp.get_value("FOOBAR/USD")
        
        # expectations
        assert(value == -1)

    def test_get_value_ok(self):
        # context
        input = "./test/data_sim_real_time_data_provider"
        dp = rtdp_simulation.SimRealTimeDataProvider({'input': input, 'chronos': chronos.Chronos()})
        ds = rtdp.DataDescription()
        ds.symbols = ["AAVE/USD"]
        ds.features = ["high", "low"]
        dp.next(ds)

        # action
        value = dp.get_value("AAVE/USD")
        
        # expectations
        assert(value == 189.53)

    def test_get_current_datetime_ok(self):
        # context
        input = "./test/data_sim_real_time_data_provider"
        dp = rtdp_simulation.SimRealTimeDataProvider({'input': input, 'chronos': chronos.Chronos()})
        ds = rtdp.DataDescription()
        ds.symbols = ["AAVE/USD"]
        ds.features = ["high", "low"]
        dp.next(ds)

        # action
        current_time = dp.get_current_datetime()
        
        # expectations
        assert(current_time == "2021-01-17 15:00:00")

    def test_next(self):
        # context
        input = "./test/data_sim_real_time_data_provider"
        dp = rtdp_simulation.SimRealTimeDataProvider({'input': input, 'chronos': chronos.Chronos()})
        ds = rtdp.DataDescription()
        ds.symbols = ["AAVE/USD"]
        ds.features = ["high", "low"]

        # action
        df = dp.next(ds)
        
        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(len(df.index) == 1)
        assert(df["high"]["AAVE/USD"] == 191.99)
        assert(df["low"]["AAVE/USD"] == 185.76)
        
    def test_next_next(self):
        # context
        input = "./test/data_sim_real_time_data_provider"
        dp = rtdp_simulation.SimRealTimeDataProvider({'input': input, 'chronos': chronos.Chronos()})
        ds = rtdp.DataDescription()
        ds.symbols = ["AAVE/USD"]
        ds.features = ["high", "low"]

        # action
        df = dp.next(ds)
        df = dp.next(ds)
        
        # expectations
        assert(isinstance(df, pd.DataFrame))
        assert(len(df.index) == 1)
        assert(df["high"]["AAVE/USD"] == 191.99)
        assert(df["low"]["AAVE/USD"] == 185.76)
        value = dp.get_value("AAVE/USD")
        assert(value == 189.53)
        