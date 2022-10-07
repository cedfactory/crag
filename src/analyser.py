import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from . import trade
import math
import os

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
        self.path_symbol_data = './data_processed/'
        self.path_symbol_plot_analyse = './output/plot_analyse/'
        self.path_symbol_plot_symbol = './output/plot_symbol/'

        self.df_analyser_result = pd.DataFrame(columns=self.get_df_result_header())

        self.df_transaction_records = pd.read_csv(self.path + 'sim_broker_history.csv', delimiter=',')
        self.df_wallet_records = pd.read_csv(self.path + 'wallet_tracking_records.csv')

        self.df_trades = self.df_transaction_records.copy()
        self.df_trades.drop(self.df_trades[self.df_trades['type'] == 'SOLD'].index, inplace=True)
        self.df_trades['transaction_roi$'] = self.df_trades['net_price'] \
                                             - self.df_trades['buying_fees'] \
                                             - self.df_trades['net_size'] * self.df_trades['buying_price']
                                            # - self.df_trades['buying_fees'] - self.df_trades['selling_fees']

        self.df_trades['trade_result_pct'] = self.df_trades['transaction_roi$'] / self.df_trades["net_price"]

        # self.df_trades.sort_values(by=['transaction_roi%'], ascending=False, inplace=True)

        self.starting_time = self.df_wallet_records['time'][0]
        self.ending_time = self.df_wallet_records['time'][len(self.df_wallet_records) - 3] # Foced sell need to provide time... to do list
        self.init_asset = self.df_wallet_records['cash'][0]
        self.final_wallet = round(self.df_wallet_records['cash'][len(self.df_wallet_records) - 1], 2)

        self.performance = round(self.df_wallet_records['roi%'][len(self.df_wallet_records) - 1], 2)
        self.vs_usd_pct = round(100*(self.final_wallet - self.init_asset) / self.init_asset, 2)
        self.avg_profit = self.df_trades['trade_result_pct'].mean()

        self.df_wallet_records['wallet_ath'] = self.df_wallet_records['wallet'].cummax()
        self.df_wallet_records['drawdown'] = self.df_wallet_records['wallet_ath'] - self.df_wallet_records['wallet']
        self.df_wallet_records['drawdown_pct'] = self.df_wallet_records['drawdown'] / self.df_wallet_records['wallet_ath']
        self.max_days_drawdown = round(self.df_wallet_records['drawdown_pct'].max()*100, 2)

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

        self.df_symbol_data = self.set_symbol_data()
        self.df_symbol_full_data = self.get_df_symbol_full_data()
        self.df_cross_check = pd.DataFrame()
        self.df_result_cross_check = pd.DataFrame()

        self.top_ranking = True    # Top 5 or threshold
        self.ranking = 10
        self.win_rate_threshold = 40

        self.merge_of_best_lists = []

        self.df_wallet_records['evolution'] = self.df_wallet_records['wallet'].diff()
        self.df_wallet_records['daily_return'] = self.df_wallet_records['evolution'] / self.df_wallet_records['wallet'].shift(1)
        self.sharpe_ratio = round((365 ** 0.5) * (self.df_wallet_records['daily_return'].mean() / self.df_wallet_records['daily_return'].std()), 2)



    def display_analysed_data(self):
        print('period: [', self.starting_time,'] -> [', self.ending_time,']')
        print('initial wallet: ', self.init_asset,'$')
        print('final wallet: ', self.final_wallet,'$')
        print('performance vs US dollar: ', self.performance,'%')
        print('performance vs US dollar: ', self.vs_usd_pct, '%')
        print('total trades on the period: ', self.nb_transaction)
        print('global Win rate: ', self.positive_trades,'%')
        print("worst drawdown days: -{}%".format(self.max_days_drawdown))
        print('best trades executed: ', self.best_transaction,'%      ', self.best_transaction_val, '$')
        print('worst trades executed: ', self.worst_transaction,'%      ', self.worst_transaction_val, '$')
        print('average profit per trades: ', self.mean_transaction,'%      ', self.mean_transaction_val,'$')
        print("average profit: {} %".format(round(self.avg_profit * 100, 2)))
        print('sharpe ratio: ', self.sharpe_ratio)

        print('list symbols traded: ', self.list_symbols)
        print('total symbols traded: ', self.nb_symbols)

        print('best win rate: ', self.list_best_win_rate)
        print('top ranking ', self.ranking,' performer %: ', self.list_ranking_perf_average_percent)
        print('top ranking ', self.ranking,' performer $: ', self.list_ranking_perf_average_dollard)
        print('merged best lists - ', len(self.merge_of_best_lists),' symbols : ', self.merge_of_best_lists )

    def set_data_analysed(self):
        self.list_symbols.append("global")

        for symbol in self.list_symbols:
            df_symbol_trades = self.df_trades.copy()

            if(symbol != "global"):
                df_symbol_trades.drop(df_symbol_trades[df_symbol_trades['symbol'] != symbol].index, inplace=True)

            df_symbol_trades.sort_values(by=['buying_time'], ascending=True, inplace=True)
            df_symbol_trades.reset_index(drop=True, inplace=True)
            first_transaction_buying = df_symbol_trades['buying_time'][0]
            price_first_buy = df_symbol_trades['buying_price'][0]

            df_symbol_trades.sort_values(by=['time'], ascending=False, inplace=True)
            df_symbol_trades.reset_index(drop=True, inplace=True)
            last_transaction_selling = df_symbol_trades['time'][0]
            price_last_sell = df_symbol_trades['symbol_price'][len(df_symbol_trades)-1]

            nb_trades_performed = len(df_symbol_trades)
            win_rate = round((df_symbol_trades['transaction_roi%'] >= 0).sum() * 100 / nb_trades_performed, 2)

            performance_symbol = round(df_symbol_trades['transaction_roi%'].sum(), 2)
            performance_average_symbol = round(df_symbol_trades['transaction_roi%'].mean(), 2)
            best_trade_symbol = round(df_symbol_trades['transaction_roi%'].max(), 2)
            worst_trade_symbol = round(df_symbol_trades['transaction_roi%'].min(), 2)

            performance_value_symbol = round(df_symbol_trades['transaction_roi$'].sum(), 2)
            performance_value_average_symbol = round(df_symbol_trades['transaction_roi$'].mean(), 2)
            best_trade_value_symbol = round(df_symbol_trades['transaction_roi$'].max(), 2)
            worst_trade_value_symbol = round(df_symbol_trades['transaction_roi$'].min(), 2)

            buy_and_hold_pct = (price_last_sell - price_first_buy) / price_first_buy
            buy_and_hold_wallet = self.init_asset + self.init_asset * buy_and_hold_pct
            vs_hold_pct = (self.final_wallet - buy_and_hold_wallet) / buy_and_hold_wallet
            vs_usd_pct = (self.final_wallet - self.init_asset) / self.init_asset

            df_new_line = pd.DataFrame([[symbol,
                                         first_transaction_buying, last_transaction_selling, nb_trades_performed, win_rate,
                                         round(buy_and_hold_pct*100, 2), round(vs_hold_pct*100, 2), round(vs_usd_pct*100, 2),
                                         performance_symbol, performance_average_symbol, best_trade_symbol, worst_trade_symbol,
                                         performance_value_symbol, performance_value_average_symbol, best_trade_value_symbol, worst_trade_value_symbol]],
                                       columns=self.get_df_result_header())

            self.df_analyser_result = pd.concat([self.df_analyser_result, df_new_line])
            self.df_analyser_result.reset_index(inplace=True, drop=True)

        self.list_symbols.remove("global")
        self.df_analyser_result.to_csv(self.path + 'analyser_records.csv')

    def get_df_result_header(self):
        return ["symbol",
                "first_buying", "last_selling", "trades_performed", "win_rate",
                "bnh_perf", "perf_vs_bnh", "perf_vs_usd",
                "performance%", "performance_average%", "best_trade%", "worst_trade%",
                "performance_value$", "performance_value_average$", "best_trade_value$", "worst_trade_value$"]

    def get_best_performer(self):
        df_performer = self.df_analyser_result.copy()

        df_performer.drop(df_performer[df_performer['symbol'] == 'global'].index, inplace=True)

        # get best win rate
        # Top 5 or threshold
        if self.top_ranking:
            df_performer.sort_values(by=['win_rate'], ascending=False, inplace=True)
            self.list_best_win_rate = df_performer['symbol'].to_list()
            self.list_best_win_rate = self.list_best_win_rate[:self.ranking]
        else:
            df_win_rate = df_performer.copy()
            df_win_rate.drop(df_win_rate[df_win_rate['win_rate'] <= self.win_rate_threshold].index, inplace=True)
            df_win_rate.sort_values(by=['win_rate'], ascending=False, inplace=True)
            self.list_best_win_rate = df_win_rate['symbol'].to_list()

        # get top 5
        df_performer.sort_values(by=['performance_average%'], ascending=False, inplace=True)
        self.list_ranking_perf_average_percent = df_performer['symbol'].to_list()
        self.list_ranking_perf_average_percent = self.list_ranking_perf_average_percent[:self.ranking]

        df_performer.sort_values(by=['performance_value$'], ascending=False, inplace=True)
        self.list_ranking_perf_average_dollard = df_performer['symbol'].to_list()
        self.list_ranking_perf_average_dollard = self.list_ranking_perf_average_dollard[:self.ranking]

        self.merge_of_best_lists = self.list_best_win_rate
        self.list_best_win_rate.extend(self.list_ranking_perf_average_percent)
        self.list_best_win_rate.extend(self.list_ranking_perf_average_dollard)
        self.merge_of_best_lists = list(set(self.merge_of_best_lists))

    def plot_analysed_data(self):
        if not os.path.exists(self.path_symbol_plot_analyse):
            os.makedirs(self.path_symbol_plot_analyse)

        ax = plt.gca()
        self.df_wallet_records.plot(kind='line', x='time', y='wallet', ax=ax)
        plt.savefig(self.path_symbol_plot_analyse + 'wallet_record_wallet.png')
        plt.clf()
        ax = plt.gca()
        self.df_wallet_records.plot(kind='line', x='time', y='portfolio', ax=ax)
        plt.savefig(self.path_symbol_plot_analyse + 'wallet_record_portfolio.png')
        plt.clf()
        ax = plt.gca()
        self.df_wallet_records.plot(kind='line', x='time', y='cash', ax=ax)
        plt.savefig(self.path_symbol_plot_analyse + 'wallet_record_cash.png')
        plt.clf()
        ax = plt.gca()
        self.df_wallet_records.plot(kind='line', x='time', y='roi%', ax=ax)
        plt.savefig(self.path_symbol_plot_analyse + 'wallet_record_roi.png')
        plt.clf()
        ax = plt.gca()
        self.df_wallet_records.plot(kind='line', x='time', y='asset%', ax=ax)
        plt.savefig(self.path_symbol_plot_analyse + 'wallet_record_asset_split.png')

        plt.clf()
        ax = plt.gca()
        self.df_transaction_records.plot(kind='line', x='time', y='wallet_roi%', ax=ax)
        plt.savefig(self.path_symbol_plot_analyse + 'transaction_record_wallet_roi.png')

        plt.clf()
        ax = plt.gca()
        self.df_transaction_records.plot(kind='line', x='time', y='remaining_cash', ax=ax)
        plt.savefig(self.path_symbol_plot_analyse + 'transaction_record_remaining_cash.png')

        plt.clf()
        ax = plt.gca()
        self.df_transaction_records.plot(kind='line', x='time', y='portfolio_value', ax=ax)
        plt.savefig(self.path_symbol_plot_analyse + 'transaction_record_portfolio_value.png')

        plt.clf()
        ax = plt.gca()
        self.df_transaction_records.plot(kind='line', x='time', y='wallet_value', ax=ax)
        plt.savefig(self.path_symbol_plot_analyse + 'transaction_record_wallet_value.png')

    def plot_symbol_data(self):
        if not os.path.exists(self.path_symbol_plot_symbol):
            os.makedirs(self.path_symbol_plot_symbol)

        df_symbol_data_normalized = self.df_symbol_data.copy()

        list_symbol = self.df_symbol_data.columns.to_list()
        list_symbol.remove("time")

        for symbol in list_symbol:
            ax = plt.gca()
            max_val = df_symbol_data_normalized[symbol].max()
            min_val = df_symbol_data_normalized[symbol].min()
            # Skearn can be used...
            df_symbol_data_normalized[symbol] = (df_symbol_data_normalized[symbol] - min_val) / (max_val - min_val)
            df_symbol_data_normalized.plot(kind='line', x='time', y=symbol, ax=ax)
            plt.savefig(self.path_symbol_plot_symbol + symbol + '_symbol_data.png')
            plt.clf()

        df_mean_symbol = pd.DataFrame()
        df_mean_symbol['time'] = df_symbol_data_normalized['time']
        df_symbol_data_normalized.drop(columns='time', inplace=True)
        df_mean_symbol['symbol_value_mean'] = df_symbol_data_normalized.mean(axis=1)

        ax = plt.gca()
        df_mean_symbol.plot(kind='line', x='time', y='symbol_value_mean', ax=ax)
        plt.savefig(self.path_symbol_plot_symbol + 'symbol_value_mean' + '_symbol_data.png')
        plt.clf()

        df_mean_symbol['symbol_wallet'] = self.df_wallet_records['wallet'].copy()
        max_val = df_mean_symbol['symbol_wallet'].max()
        min_val = df_mean_symbol['symbol_wallet'].min()
        df_mean_symbol['symbol_wallet_normalized'] = (df_mean_symbol['symbol_wallet'] - min_val) / (max_val - min_val)

        ax = plt.gca()
        df_mean_symbol.plot(kind='line', x='time', y='symbol_value_mean', ax=ax)
        df_mean_symbol.plot(kind='line', x='time', y='symbol_wallet_normalized', ax=ax)
        plt.savefig(self.path_symbol_plot_symbol + 'wallet_vs' + '_symbol_data.png')
        plt.savefig(self.path + 'wallet_vs' + '_symbol_data.png')
        plt.clf()

    def set_symbol_data(self):
        df_symbol_data = pd.DataFrame()
        df_symbol_data['time'] = self.df_wallet_records['time']
        for symbol in self.list_symbols:
            symbol = symbol.replace("/", "_")
            # df = pd.read_csv(self.path_symbol_data + symbol + '.csv', sep=',')
            df = pd.read_csv(self.path_symbol_data + symbol + '.csv')
            df_symbol_data[symbol] = df['close']
        df_symbol_data.dropna(inplace=True)
        return df_symbol_data

    def get_df_symbol_full_data(self):
        df_symbol_full_data = pd.DataFrame()
        for symbol in self.list_symbols:
            symbol_filename = symbol.replace('/', '_')
            df_data = pd.read_csv("./data_processed/" + symbol_filename + ".csv")
            df_data['symbol'] = symbol
            df_symbol_full_data = pd.concat([df_symbol_full_data, df_data])
        return df_symbol_full_data

    def isnan(self, value):
        try:
            return math.isnan(float(value))
        except:
            return False

    def cross_check(self):
        for symbol in self.list_symbols:
            df_symbol_cross_check = pd.DataFrame()
            df_symbol_data = self.df_symbol_full_data.copy()

            df_symbol_data = df_symbol_data[df_symbol_data['symbol'] == symbol]

            # df_symbol_full_data.drop(df_symbol_full_data[df_symbol_full_data['symbol'] == symbol].index, inplace=True)

            df_symbol_trades = self.df_trades.copy()
            df_symbol_trades.drop(df_symbol_trades[df_symbol_trades['symbol'] != symbol].index, inplace=True)

            df_symbol_cross_check['buying_time'] = df_symbol_trades['buying_time']
            df_symbol_cross_check['selling_time'] = df_symbol_trades['time']
            df_symbol_cross_check['buying_price'] = df_symbol_trades['buying_price']
            df_symbol_cross_check['selling_price'] = df_symbol_trades['symbol_price']
            df_symbol_cross_check['size'] = df_symbol_trades['net_size']
            df_symbol_cross_check['net_price'] = df_symbol_trades['net_price']
            df_symbol_cross_check['buying_fees'] = df_symbol_trades['buying_fees']
            df_symbol_cross_check['selling_fees'] = df_symbol_trades['selling_fees']
            df_symbol_cross_check['gross_price'] = df_symbol_trades['gross_price']

            # Cross check
            df_symbol_data.set_index('timestamp', inplace=True)

            # Cross check Buying values
            df_symbol_cross_check.set_index('buying_time',inplace=True)
            df_symbol_cross_check['check_buying_price'] = False
            df_symbol_cross_check['checked_buying_price'] = False
            for buying_time in df_symbol_cross_check.index.to_list():
                df_symbol_cross_check['check_buying_price'][buying_time] = df_symbol_data['close'][buying_time]
            df_symbol_cross_check['checked_buying_price'] = np.where(df_symbol_cross_check['check_buying_price'] == df_symbol_cross_check['buying_price'],
                                                                     True,
                                                                     False)

            # Cross check Selling values
            df_symbol_cross_check.set_index('selling_time', inplace=True)
            df_symbol_cross_check['check_selling_price'] = False
            df_symbol_cross_check['checked_selling_price'] = False
            for selling_time in df_symbol_cross_check.index.to_list():
                try:
                    df_symbol_cross_check['check_selling_price'][selling_time] = df_symbol_data['close'][selling_time]
                except:
                    print("error selling time: ", selling_time, symbol)
            df_symbol_cross_check['checked_selling_price'] = np.where(
                df_symbol_cross_check['check_selling_price'] == df_symbol_cross_check['selling_price'],
                True,
                False)

            # Cross check Net price
            df_symbol_cross_check['buying_*_size'] = df_symbol_cross_check['buying_price'] * df_symbol_cross_check['size']
            df_symbol_cross_check['selling_*_size'] = df_symbol_cross_check['selling_price'] * df_symbol_cross_check['size']

            # df_symbol_cross_check['checked_net_price'] = np.where(
            #     df_symbol_cross_check['net_price'] == df_symbol_cross_check['selling_*_size'],
            #     True,
            #     False)
            df_symbol_cross_check['checked_net_price'] = np.where(
                abs(df_symbol_cross_check['selling_*_size'] - df_symbol_cross_check['net_price']) < 0.0001,
                True,
                False)

            df_symbol_cross_check['buying_fees'] = df_symbol_cross_check['buying_*_size'] * 0.0007
            df_symbol_cross_check['selling_fees'] = df_symbol_cross_check['selling_*_size'] * 0.0007
            df_symbol_cross_check['total_fees'] = df_symbol_cross_check['buying_fees'] + df_symbol_cross_check['selling_fees']


            df_symbol_cross_check['calc_gross_price_all'] =df_symbol_cross_check['buying_fees'] + \
                                                            df_symbol_cross_check['selling_*_size'] + \
                                                            df_symbol_cross_check['selling_fees']

            df_symbol_cross_check['transaction_benefit'] = df_symbol_cross_check['calc_gross_price_all'] - df_symbol_cross_check['buying_*_size']
            df_symbol_cross_check['transaction_benefit_%'] = df_symbol_cross_check['transaction_benefit'] * 100 / df_symbol_cross_check['buying_*_size']

            self.df_cross_check = pd.concat([self.df_cross_check, df_symbol_cross_check])

            df_new_line = pd.DataFrame([[symbol,
                                         len(df_symbol_cross_check),
                                         round((df_symbol_cross_check['transaction_benefit'] >= 0).sum() * 100 / len(df_symbol_cross_check), 2),
                                         df_symbol_cross_check['checked_buying_price'].sum() == len(df_symbol_cross_check),
                                         df_symbol_cross_check['checked_selling_price'].sum() == len(df_symbol_cross_check),
                                         round(df_symbol_cross_check['buying_fees'].sum(), 2),
                                         round(df_symbol_cross_check['selling_fees'].sum(), 2),
                                         round(df_symbol_cross_check['total_fees'].sum(), 2),
                                         round(df_symbol_cross_check['buying_*_size'].sum(), 2),
                                         round(df_symbol_cross_check['selling_*_size'].sum(), 2),
                                         round(df_symbol_cross_check['transaction_benefit'].sum(), 2),
                                         round(df_symbol_cross_check['transaction_benefit_%'].mean(), 2),
                                         round(df_symbol_cross_check['calc_gross_price_all'].sum() * 100 / df_symbol_cross_check['buying_*_size'].sum(), 2),
                                         round(df_symbol_cross_check['transaction_benefit'].sum() * 100 / df_symbol_cross_check['buying_*_size'].sum(), 2)]],

                                       columns=['symbol',
                                                'total_transactions',
                                                'win_rate',
                                                'buying_price_check',
                                                'selling_price_check',
                                                'buying_fees',
                                                'selling_fees',
                                                'total_fees',
                                                'net_buying_price',
                                                'net_selling_price',
                                                'roi_$',
                                                'roi',
                                                'global_roi_%',
                                                'roi_%'])

            self.df_result_cross_check = pd.concat([self.df_result_cross_check, df_new_line])

        self.df_result_cross_check.to_csv(self.path + 'cross_check_results.csv')

    def run_analyse(self):
        self.set_data_analysed()
        self.get_best_performer()
        self.display_analysed_data()
        self.plot_analysed_data()
        self.plot_symbol_data()
        self.cross_check()



