import time
import pandas as pd
from . import trade

'''
    Period: [2020-12-31 00:00:00] -> [2022-01-10 00:00:00]
    Initial wallet: 1000.0 $
    
    --- General Information ---
    Final wallet: 979.66 $
    Performance vs US dollar: -2.03 %
    Sharpe Ratio: 0.16
    Worst Drawdown T|D: -19.53% | -26.61%
    Buy and hold performance: 43.82 %
    Performance vs buy and hold: -31.88 %
    Total trades on the period: 46
    Global Win rate: 21.74 %
    Average Profit: 0.13 %
    
    Best trades: +25.23 % the 2021-02-08 17:00:00 -> 2021-02-22 15:00:00
    Worst trades: -9.84 % the 2021-01-29 14:00:00 -> 2021-01-31 10:00:00
    ----- 2020 Cumulative Performances: 0.0% -----
    ----- 2021 Cumulative Performances: -2.03% -----
    ----- 2022 Cumulative Performances: 0.0% -----
'''

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



