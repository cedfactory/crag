import pytest
from src import chronos
import pandas as pd
from . import utils

class TestChronos:

    def test_get_df_range_time(self, mocker):
        # context
        json_df = utils.get_json_for_get_df_range()
        mocker.patch('src.utils.fdp_request_post', side_effect=[json_df])

        # action
        my_chronos = chronos.Chronos("2022-01-01", "2022-01-10", "1h")

        # expectations
        assert(isinstance(my_chronos.df_time, pd.DataFrame))
        assert(my_chronos.df_time["timestamp"].dtype == "datetime64[ns]")
                
