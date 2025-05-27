import pytest
import os
import pandas as pd
from src import data_frame_store

class TestDataFrameStore:

    def test_save_to_csv(self):
        # context
        dfs = data_frame_store.DataFrameStore("toto")
        data = {'close': [20, 21, 23, 19, 18, 24, 25, 26, 27, 28]}
        dfs.df = pd.DataFrame(data)
        dfs.max_size = 10

        # action
        dfs.save_to_csv()

        # expectations
        expected_filename = "toto_0001.csv.gz"
        assert(os.path.isfile(expected_filename))
        os.remove(expected_filename)
