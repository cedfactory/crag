import pandas as pd

class Chronos():
    def __init__(self):
        self.master_position = 400
        self.df_time = pd.read_csv('./data/' + 'BTC_USD.csv', delimiter=';', usecols=['timestamp'])
        self.master_time = self.set_time()

    def increment_time(self):
        self.master_position = self.master_position + 1
        self.master_time = self.set_time()

    def set_time(self):
        return self.df_time.iloc[min(self.master_position, len(self.df_time)-1)]['timestamp']

    def get_current_position(self):
        return self.master_position

    def get_current_time(self):
        return self.master_time