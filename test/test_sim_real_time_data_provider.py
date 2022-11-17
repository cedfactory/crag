import pytest
import shutil
import os.path
import pandas as pd
from src import rtdp,rtdp_simulation
'''
class TestSimRealTimeDataProvider:

    def test_constructor(self):
        # context
        data_directory = "./test/data_sim_real_time_data_provider"

        # action
        dp = rtdp_simulation.SimRealTimeDataProvider({'data_directory': data_directory})

        # expectations
        assert(dp.data_directory == data_directory)
        assert(dp.current_position == 400)
        assert(len(dp.data) == 1)
        assert("AAVE/USD" in dp.data)
        df = dp.data["AAVE/USD"]
        assert(isinstance(df, pd.DataFrame))
        expected_columns = ['Unnamed: 0', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'ema_5', 'ema_400', 'super_trend_direction']
        assert(df.columns.to_list() == expected_columns)
        assert(len(df) == 402)

    def test_get_value_last_in_dataframe(self):
        # context
        data_directory = "./test/data_sim_real_time_data_provider"
        dp = rtdp_simulation.SimRealTimeDataProvider({'data_directory': data_directory})
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
        data_directory = "./test/data_sim_real_time_data_provider"
        dp = rtdp_simulation.SimRealTimeDataProvider({'data_directory': data_directory})

        # action
        value = dp.get_value("FOOBAR/USD")
        
        # expectations
        assert(value == -1)

    def test_get_value_ok(self):
        # context
        data_directory = "./test/data_sim_real_time_data_provider"
        dp = rtdp_simulation.SimRealTimeDataProvider({'data_directory': data_directory})
        ds = rtdp.DataDescription()
        ds.symbols = ["AAVE/USD"]
        ds.features = ["high", "low"]

        # action
        value = dp.get_value("AAVE/USD")
        
        # expectations
        assert(value == 189.53)

    def test_get_current_datetime_ok(self):
        # context
        data_directory = "./test/data_sim_real_time_data_provider"
        dp = rtdp_simulation.SimRealTimeDataProvider({'data_directory': data_directory})
        ds = rtdp.DataDescription()
        ds.symbols = ["AAVE/USD"]
        ds.features = ["high", "low"]

        # action
        current_time = dp.get_current_datetime()
        
        # expectations
        assert(current_time == "2021-01-17 15:00:00")

    def test_get_current_data(self):
        # context
        data_directory = "./test/data_sim_real_time_data_provider"
        dp = rtdp_simulation.SimRealTimeDataProvider({'data_directory': data_directory})
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

    def test_record(self, mocker):
        # context
        data_directory = "./test/tmp/"
        if os.path.isdir(data_directory):
            shutil.rmtree(data_directory)

        my_rtdp = rtdp_simulation.SimRealTimeDataProvider({"data_directory": data_directory})
        ds = rtdp.DataDescription()
        ds.symbols = ["AAVE/EURS"]

        df = pd.read_csv("./test/data/AAVE_USD.csv", delimiter=',')
        json_df = {'result': {'AAVE_EURS': {'status': 'ok', 'info':df.to_json()}}}
        mocker.patch('src.utils.fdp_request_post', side_effect=[json_df])

        # action
        my_rtdp.record(ds)

        # expectations
        expected_filename = data_directory+'AAVE_EURS.csv'
        assert(os.path.isfile(expected_filename))

        expected_columns = ['Unnamed: 0.1', 'Unnamed: 0', 'timestamp', 'open', 'high', 'low',
       'close', 'volume', 'ema_short', 'ema_long', 'super_trend_direction',
       'TRIX', 'TRIX_PCT', 'TRIX_SIGNAL', 'TRIX_HISTO', 'STOCH_RSI', 'ema12',
       'ema26', 'ema12gtema26', 'ema12gtema26co', 'ema12ltema26',
       'ema12ltema26co', 'macd', 'signal', 'macdgtsignal', 'macdgtsignalco',
       'macdltsignal', 'macdltsignalco', 'sma50', 'sma200', 'goldencross',
       'obv', 'obv_pc', 'ema13', 'elder_ray_bull', 'elder_ray_bear', 'eri_buy',
       'eri_sell', 'AO', 'previous_AO', 'EMA100', 'EMA200', 'WILLR', 'HLC3',
       'VMC_WAVE1', 'VMC_WAVE2', 'MONEY_FLOW', 'ema_short_vmc', 'ema_long_vmc',
       'CHOP', 'n1_VMC_WAVE1', 'n1_VMC_WAVE2']

        df = pd.read_csv(expected_filename, delimiter=',')
        assert(isinstance(df, pd.DataFrame))
        assert(df.columns.to_list() == expected_columns)

        # cleaning
        shutil.rmtree(data_directory)
'''
