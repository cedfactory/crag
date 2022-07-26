import pytest
from src import chronos
import pandas as pd

class TestChronos:

    def test_get_df_range_time(self, mocker):
        # context
        df = pd.read_csv("./test/data/AAVE_USD.csv", delimiter=';')

        df.drop(["Unnamed: 0"], axis=1, inplace=True)
        df['timestamp'] = df['timestamp'].astype('string') # convert object type to string type
        df['timestamp'] = pd.to_datetime(df['timestamp'], format="%d/%m/%Y %H:%M") # format datetime
        df.rename(columns = {"timestamp":"index"}, inplace = True)
        json_df = {'result': {'BTC_USD': {'status': 'ok', 'info':df.to_json()}}}
        mocker.patch('src.utils.fdp_request', side_effect=[json_df])

        # action
        my_chronos = chronos.Chronos("2022-01-01", "2022-01-10", "1h")

        # expectations
        assert(isinstance(my_chronos.df_time, pd.DataFrame))
        assert(my_chronos.df_time["timestamp"].dtype == "datetime64[ns]")
                
