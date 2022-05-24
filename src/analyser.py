import time
import pandas as pd
from . import trade

class Analyser:
    def __init__(self, params = None):
        self.start_date = 0
        self.end_date = 0
        self.list_symbols = []
        self.path = './output/'

        self.df_transaction_records = pd.read_csv(self.path + 'sim_broker_history.csv', delimiter=';')
        self.df_wallet_records = pd.read_csv(self.path + 'wallet_tracking_records.csv')

    def run_analyse(self):
        print('analyser')



