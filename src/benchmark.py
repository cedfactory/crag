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
        self.path_output = self.path + '/output/'
        self.path_symbol_data = './data_processed/'
        self.path_symbol_plot_analyse = './benchmark/plot_analyse/'

        self.period = 0
        if params:
            self.period = params.get("period", self.period)

        if not os.path.exists(self.path_output):
            os.makedirs(self.path_output)

        self.df_transaction_records = pd.DataFrame()
        self.df_wallet_records = pd.DataFrame()

        self.list_strategies = []
        list_csv_files = fnmatch.filter(os.listdir(self.path), '*.csv')
        for csv_file in list_csv_files:
            prefixe = csv_file.split(".")[0]
            strategy = prefixe.split("_")[5]
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
                    start = self.df_wallet_records['time'][0]
                    start = start.split(" ")[0]
                    end = self.df_wallet_records['time'][len(self.df_wallet_records)-3] # CEDE DEBUG -3
                    end = end.split(" ")[0]
                else:
                    self.df_wallet_records = pd.concat([self.df_wallet_records, df_data])
        # self.path_results = self.path_output + '/' + start + '_to_' + end + '/'

        self.path_results = self.path_output
        if not os.path.exists(self.path_results):
            os.makedirs(self.path_results)
        period = " from_" + start + ' to ' + end

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
        # ax.legend(bbox_to_anchor=(1, 1.02), loc='upper left')
        plt.title('wallet_strategy' + period)
        plt.savefig(self.path_results + 'wallet_strategy.png')
        plt.clf()

        # Global Win rate
        self.df_plot_data = pd.DataFrame()
        for strategy in self.list_strategies:
            df_strategy = self.df_transaction_records[self.df_transaction_records['strategy'] == strategy].copy()
            df_strategy = df_strategy[df_strategy['type'] == 'SELL'].copy()
            if len(self.df_plot_data) == 0:
                self.df_plot_data = pd.DataFrame(columns=self.list_strategies, index=['transaction_total',
                                                                                      'global_win_rate',
                                                                                      'profit$',
                                                                                      'profit%'])

            self.df_plot_data[strategy]['transaction_total'] = len(df_strategy)

            # df_strategy['transaction_roi$'] = df_strategy['net_price'] - df_strategy['buying_fees'] - df_strategy['selling_fees'] - df_strategy['net_size'] * df_strategy['buying_price']
            df_strategy['transaction_roi$'] = df_strategy['net_price'] - df_strategy['buying_fees'] - df_strategy['net_size'] * df_strategy['buying_price']

            self.df_plot_data[strategy]['global_win_rate'] = round((df_strategy['transaction_roi$'] >= 0).sum() * 100 / self.df_plot_data[strategy]['transaction_total'], 2)

            df_strategy = self.df_wallet_records[self.df_wallet_records['strategy'] == strategy].copy()
            self.df_plot_data[strategy]['profit$'] = round(df_strategy['wallet'][len(df_strategy)-1] - df_strategy['wallet'][0], 1)
            self.df_plot_data[strategy]['profit%'] = round(self.df_plot_data[strategy]['profit$'] * 100 / df_strategy['wallet'][0], 1)

        # Plot win rate
        df_win_rate = self.df_plot_data.copy()
        for row_to_drop in df_win_rate.index.to_list():
            if (row_to_drop != 'global_win_rate'):
                df_win_rate.drop(inplace=True, index=row_to_drop)
        df_win_rate.reset_index(inplace=True)

        ax = plt.gca()
        df_win_rate.plot(kind='bar', x='index', ax=ax)
        for i in range(len(ax.containers)):
            ax.bar_label(ax.containers[i])
        plt.title('win_rate' + period)
        plt.savefig(self.path_results + 'win_rate.png')
        plt.clf()

        # Plot win rate
        df_transaction_total = self.df_plot_data.copy()
        for row_to_drop in df_transaction_total.index.to_list():
            if (row_to_drop != 'transaction_total'):
                df_transaction_total.drop(inplace=True, index=row_to_drop)
        df_transaction_total.reset_index(inplace=True)

        ax = plt.gca()
        df_transaction_total.plot(kind='bar', x='index', ax=ax)
        for i in range(len(ax.containers)):
            ax.bar_label(ax.containers[i])
        plt.title('total_transaction_performed' + period)
        plt.savefig(self.path_results + 'transaction_total.png')
        plt.clf()

        # Plot profit $
        df_profit = self.df_plot_data.copy()
        for row_to_drop in df_profit.index.to_list():
            if (row_to_drop != 'profit$'):
                df_profit.drop(inplace=True, index=row_to_drop)
        df_profit.reset_index(inplace=True)

        ax = plt.gca()
        df_profit.plot(kind='bar', x='index', ax=ax)
        for i in range(len(ax.containers)):
            ax.bar_label(ax.containers[i])
        plt.title('profit $' + period)
        plt.savefig(self.path_results + 'profit$.png')
        plt.clf()

        # Plot profit %
        df_profit = self.df_plot_data.copy()
        for row_to_drop in df_profit.index.to_list():
            if (row_to_drop != 'profit%'):
                df_profit.drop(inplace=True, index=row_to_drop)
        df_profit.reset_index(inplace=True)

        ax = plt.gca()
        df_profit.plot(kind='bar', x='index', ax=ax)
        for i in range(len(ax.containers)):
            ax.bar_label(ax.containers[i])
        plt.title('profit %' + period)
        plt.savefig(self.path_results + 'profit%.png')
        plt.clf()

    def run_benchmark(self):
        pass

    def set_benchmark_df_results(self, df):
        print(self.df_plot_data)

        for strategy in self.list_strategies:
            transaction_total = self.df_plot_data.loc['transaction_total', strategy]
            global_win_rate = self.df_plot_data.loc['global_win_rate', strategy]
            profit_dol = self.df_plot_data.loc['profit$', strategy]
            profit_percent = self.df_plot_data.loc['profit%', strategy]

            df.loc[(df['period'] == self.period) & (df['strategy'] == strategy), 'total_transaction'] = transaction_total
            df.loc[(df['period'] == self.period) & (df['strategy'] == strategy), 'profit$'] = profit_dol
            df.loc[(df['period'] == self.period) & (df['strategy'] == strategy), 'profit%'] = profit_percent
            df.loc[(df['period'] == self.period) & (df['strategy'] == strategy), 'win_rate'] = global_win_rate

            ''' CEDE Comment: Alternative methode
            df['total_transaction'] = np.where( (df['period'] == self.period) & (df['strategy'] == strategy),
                                                transaction_total,
                                                df['total_transaction']
                                                )
            '''
        return df

