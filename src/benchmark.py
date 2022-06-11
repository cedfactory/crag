import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from . import trade
import math
import os, fnmatch


class Benchmark:
    def __init__(self, params = None):
        self.path = './benchmark/'
        self.path_symbol_data = './data_processed/'
        self.path_symbol_plot_analyse = './benchmark/plot_analyse/'

        self.df_transaction_records = pd.DataFrame()
        self.df_wallet_records = pd.DataFrame()

        self.list_strategies = []
        list_csv_files = fnmatch.filter(os.listdir(self.path), '*.csv')
        for csv_file in list_csv_files:
            prefixe = csv_file.split(".")[0]
            strategy = prefixe.split("_")[3]
            self.list_strategies.append(strategy)
            if prefixe.split("_")[0] == 'sim':
                df_data = pd.read_csv(self.path + csv_file, delimiter=';')
            elif prefixe.split("_")[0] == 'wallet':
                df_data = pd.read_csv(self.path + csv_file)
            df_data['strategy'] = strategy
            if prefixe.split("_")[0] == 'sim':
                if len(self.df_transaction_records) == 0:
                    self.df_transaction_records = df_data.copy()
                else:
                    self.df_transaction_records = pd.concat([self.df_transaction_records, df_data])
            elif prefixe.split("_")[0] == 'wallet':
                if len(self.df_wallet_records) == 0:
                    self.df_wallet_records = df_data.copy()
                else:
                    self.df_wallet_records = pd.concat([self.df_wallet_records, df_data])

        self.list_strategies = list(set(self.list_strategies))

        # Wallet values
        self.df_wallet = pd.DataFrame()
        for strategy in self.list_strategies:
            df_strategy = self.df_wallet_records[self.df_wallet_records['strategy'] == strategy].copy()
            if len(self.df_wallet) == 0:
                self.df_wallet = pd.DataFrame(columns=self.list_strategies, index=df_strategy['time'].to_list())
            df_strategy.set_index('time', inplace=True)
            self.df_wallet[strategy] = df_strategy['wallet']

        # Dropping last 2 rows using drop
        # DEBUG identified bug in Crag forced_sell_position
        n = 2
        self.df_wallet.drop(self.df_wallet.tail(n).index, inplace=True)

        # Plot Wallet values
        ax = plt.gca()
        for strategy in self.list_strategies:
            self.df_wallet.plot(kind='line', y=strategy, ax=ax)
        plt.savefig(self.path + 'wallet_strategy.png')
        plt.clf()

        # Global Win rate
        self.df_win_rate = pd.DataFrame()
        for strategy in self.list_strategies:
            df_strategy = self.df_transaction_records[self.df_transaction_records['strategy'] == strategy].copy()
            df_strategy = df_strategy[df_strategy['type'] == 'SELL'].copy()
            if len(self.df_win_rate) == 0:
                self.df_win_rate = pd.DataFrame(columns=self.list_strategies, index=['transaction_total', 'global_win_rate'])

            self.df_win_rate[strategy]['transaction_total'] = len(df_strategy)

            # df_strategy['transaction_roi$'] = df_strategy['net_price'] - df_strategy['buying_fees'] - df_strategy['selling_fees'] - df_strategy['net_size'] * df_strategy['buying_price']
            df_strategy['transaction_roi$'] = df_strategy['net_price'] - df_strategy['buying_fees'] - df_strategy['net_size'] * df_strategy['buying_price']

            self.df_win_rate[strategy]['global_win_rate'] = round((df_strategy['transaction_roi$'] >= 0).sum() * 100 / self.df_win_rate[strategy]['transaction_total'], 2)

        df_win_rate = self.df_win_rate.copy()
        df_win_rate.drop(inplace=True, index='transaction_total')
        df_win_rate.reset_index(inplace=True)
        # Plot win rate
        ax = plt.gca()
        df_win_rate.plot(kind='bar', x='index', ax=ax)
        plt.savefig(self.path + 'win_rate.png')
        plt.clf()

        df_transaction_total = self.df_win_rate.copy()
        df_transaction_total.drop(inplace=True, index='global_win_rate')
        df_transaction_total.reset_index(inplace=True)
        # Plot win rate
        ax = plt.gca()
        df_transaction_total.plot(kind='bar', x='index', ax=ax)
        plt.savefig(self.path + 'transaction_total.png')
        plt.clf()

    def run_analyse(self):
        pass



