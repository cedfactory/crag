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
        self.path = './output/'

        self.df_transaction_records = pd.read_csv(self.path + 'sim_broker_history.csv', delimiter=';')
        self.df_wallet_records = pd.read_csv(self.path + 'wallet_tracking_records.csv')

        self.starting_time = self.df_wallet_records['time'][0]
        self.ending_time = self.df_wallet_records['time'][len(self.df_wallet_records) - 3] # Foced sell need to provide time... to do list
        self.init_asset = self.df_wallet_records['cash'][0]
        self.final_wallet = round(self.df_wallet_records['cash'][len(self.df_wallet_records) - 1], 2)

        self.performance = round(self.df_wallet_records['roi%'][len(self.df_wallet_records) - 1], 2)

        self.df_trades = self.df_transaction_records.copy()
        self.df_trades.drop(self.df_trades[self.df_trades['type'] == 'SOLD'].index, inplace=True)

        self.df_trades['transaction_roi$'] = self.df_trades['net_price'] \
                                             - self.df_trades['buying_fees'] - self.df_trades['selling_fees'] \
                                             - self.df_trades['net_size'] * self.df_trades['buying_price']

        self.df_trades.sort_values(by=['transaction_roi%'], ascending=False, inplace=True)

        self.nb_transaction = len(self.df_trades)
        self.positive_trades = round((self.df_trades['transaction_roi%'] >= 0).sum() * 100 / self.nb_transaction, 2)
        self.negative_trades = round((self.df_trades['transaction_roi%'] <= 0).sum() * 100 / self.nb_transaction, 2)
        self.best_transaction = round(self.df_trades['transaction_roi%'].max(), 2)
        self.worst_transaction = round(self.df_trades['transaction_roi%'].min(), 2)
        self.mean_transaction = round(self.df_trades['transaction_roi%'].mean(), 2)

        self.positive_trades_val = round((self.df_trades['transaction_roi$'] >= 0).sum() * 100 / self.nb_transaction, 2)
        self.negative_trades_val = round((self.df_trades['transaction_roi$'] <= 0).sum() * 100 / self.nb_transaction, 2)
        self.best_transaction_val = round(self.df_trades['transaction_roi$'].max(), 2)
        self.worst_transaction_val = round(self.df_trades['transaction_roi$'].min(), 2)
        self.mean_transaction_val = round(self.df_trades['transaction_roi$'].mean(), 2)


        self.list_symbols = list(set(self.df_transaction_records['symbol'].to_list()))
        self.nb_symbols = len(self.list_symbols)



    def display_analyse(self):
        print('period: [', self.starting_time,'] -> [', self.ending_time,']')
        print('initial wallet: ', self.init_asset,'$')
        print('final wallet: ', self.final_wallet,'$')
        print('performance vs US dollar: ', self.performance,'%')

        print('total trades on the period: ', self.nb_transaction)
        print('global Win rate: ', self.positive_trades,'%')
        # print('negative trades executed: ', self.negative_trades,'%')
        print('best trades executed: ', self.best_transaction,'%      ', self.best_transaction_val, '$')
        print('worst trades executed: ', self.worst_transaction,'%      ', self.worst_transaction_val, '$')
        print('Average Profit: ', self.mean_transaction,'%      ', self.mean_transaction_val,'$')

        print('symbols traded: ', self.list_symbols)
        print('symbols traded nbr: ', self.nb_symbols)



    def run_analyse(self):
        self.display_analyse()



