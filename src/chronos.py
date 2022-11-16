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

        self.master_time = self.set_time()

    def increment_time(self):
        self.master_position = self.master_position + 1
        self.master_time = self.set_time()

    def set_time(self):
        return self.df_time.iloc[min(self.master_position, len(self.df_time)-1)]['timestamp']

    def get_current_position(self):
        return self.master_position

    def get_current_time(self):
        if self.start_date != None and self.end_date != None:
            return self.master_time
        else:
            return datetime.now()

    def get_df_range_time(self):
        formatted_symbol = "BTC_USDT"
        params = { "service":"history", "exchange":"binance", "symbol":formatted_symbol, "start":self.start_date, "end": self.end_date, "interval": self.interval }
        response_json = utils.fdp_request_post("history", params)

        if response_json["result"][formatted_symbol]["status"] == "ko":
            print("no data for scheduler ")

        df = pd.read_json(response_json["result"][formatted_symbol]["info"])
        df_datetime = pd.DataFrame()
        df_datetime['timestamp'] = pd.to_datetime(df['index'], unit='ms')
        return df_datetime

    def get_final_time(self):
        return self.df_time['timestamp'][len(self.df_time)-1]