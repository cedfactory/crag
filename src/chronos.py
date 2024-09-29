from datetime import datetime
import pandas as pd
from . import utils

class Chronos():
    def __init__(self, start_date=None, end_date=None, interval=None):
        self.master_position = 0
        self.start_date = start_date
        self.end_date = end_date
        self.interval = interval

        if start_date != None and end_date != None:
            self.df_time = self.get_df_range_time()
        else:
            self.df_time = None

        self.master_time = self.set_time()

    def increment_time(self):
        self.master_position = self.master_position + 1
        self.master_time = self.set_time()

    def set_time(self):
        if isinstance(self.df_time,pd.DataFrame):
            return self.df_time.iloc[min(self.master_position, len(self.df_time)-1)]['timestamp']
        else:
            return None

    def get_current_position(self):
        return self.master_position

    def get_current_time(self):
        return datetime.now()

    def get_df_range_time(self):
        df_datetime = pd.DataFrame({'timestamp': pd.date_range(start=self.start_date, end=self.end_date, freq=self.interval)})
        return df_datetime

    def get_final_time(self):
        if isinstance(self.df_time,pd.DataFrame):
            return self.df_time['timestamp'][len(self.df_time)-1]
        else:
            return None