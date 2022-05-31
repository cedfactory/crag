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

        self.df_analyser_result = pd.DataFrame(columns=self.get_df_result_header())

        self.df_transaction_records = pd.read_csv(self.path + 'sim_broker_history.csv', delimiter=';')
        self.df_wallet_records = pd.read_csv(self.path + 'wallet_tracking_records.csv')

        self.df_trades = self.df_transaction_records.copy()
        self.df_trades.drop(self.df_trades[self.df_trades['type'] == 'SOLD'].index, inplace=True)
        self.df_trades['transaction_roi$'] = self.df_trades['net_price'] \
                                             - self.df_trades['buying_fees'] - self.df_trades['selling_fees'] \
                                             - self.df_trades['net_size'] * self.df_trades['buying_price']
        # self.df_trades.sort_values(by=['transaction_roi%'], ascending=False, inplace=True)

        self.starting_time = self.df_wallet_records['time'][0]
        self.ending_time = self.df_wallet_records['time'][len(self.df_wallet_records) - 3] # Foced sell need to provide time... to do list
        self.init_asset = self.df_wallet_records['cash'][0]
        self.final_wallet = round(self.df_wallet_records['cash'][len(self.df_wallet_records) - 1], 2)

        self.performance = round(self.df_wallet_records['roi%'][len(self.df_wallet_records) - 1], 2)

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

        print('best win rate: ', self.list_best_win_rate)
        print('top5 performer %: ', self.list_top5_perf_average_percent)
        print('top5 performer $: ', self.list_top5_perf_average_dollard)

    def set_data_analysed(self):
        self.list_symbols.append("global")

        for symbol in self.list_symbols:
            df_symbol_trades = self.df_trades.copy()

            if(symbol != "global"):
                df_symbol_trades.drop(df_symbol_trades[df_symbol_trades['symbol'] != symbol].index, inplace=True)

            df_symbol_trades.sort_values(by=['buying_time'], ascending=True, inplace=True)
            df_symbol_trades.reset_index(drop=True, inplace=True)
            first_transaction_buying = df_symbol_trades['buying_time'][0]

            df_symbol_trades.sort_values(by=['time'], ascending=False, inplace=True)
            df_symbol_trades.reset_index(drop=True, inplace=True)
            last_transaction_selling = df_symbol_trades['time'][0]

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

            df_new_line = pd.DataFrame([[symbol,
                                         first_transaction_buying, last_transaction_selling, nb_trades_performed, win_rate,
                                         performance_symbol, performance_average_symbol, best_trade_symbol, worst_trade_symbol,
                                         performance_value_symbol, performance_value_average_symbol, best_trade_value_symbol, worst_trade_value_symbol]],
                                       columns=self.get_df_result_header())

            self.df_analyser_result = pd.concat([self.df_analyser_result, df_new_line])
            self.df_analyser_result.reset_index(inplace=True, drop=True)

        self.df_analyser_result.to_csv(self.path + 'analyser_records.csv')

    def get_df_result_header(self):
        return ["symbol",
                "first_buying", "last_selling", "trades_performed", "win_rate",
                "performance%", "performance_average%", "best_trade%", "worst_trade%",
                "performance_value$", "performance_value_average$", "best_trade_value$", "worst_trade_value$"]

    def get_best_performer(self):
        df_performer = self.df_analyser_result.copy()

        df_performer.drop(df_performer[df_performer['symbol'] == 'global'].index, inplace=True)

        # get best win rate
        df_win_rate = df_performer.copy()
        df_win_rate.drop(df_win_rate[df_win_rate['win_rate'] <= 40].index, inplace=True)
        df_win_rate.sort_values(by=['win_rate'], ascending=False, inplace=True)
        self.list_best_win_rate = df_win_rate['symbol'].to_list()

        # get top 5
        df_performer.sort_values(by=['performance_average%'], ascending=False, inplace=True)
        self.list_top5_perf_average_percent = df_performer['symbol'].to_list()
        self.list_top5_perf_average_percent = self.list_top5_perf_average_percent[:5]

        df_performer.sort_values(by=['performance_value$'], ascending=False, inplace=True)
        self.list_top5_perf_average_dollard = df_performer['symbol'].to_list()
        self.list_top5_perf_average_dollard = self.list_top5_perf_average_dollard[:5]



    def run_analyse(self):
        self.set_data_analysed()
        self.get_best_performer()
        self.display_analyse()



