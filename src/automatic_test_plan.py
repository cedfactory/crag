import pandas as pd
import os
from datetime import datetime

class AutomaticTestPlan():
    def __init__(self, params):
        self.path_results = "./automatic_test_results/"
        if not os.path.exists(self.path_results):
            os.makedirs(self.path_results)


        self.test_plan_columns = ['start', 'end', 'split', 'period',
                                  'strategy', 'interval', 'sl', 'tp',
                                  'total_transaction', 'profit$', 'profit%', 'win_rate', 'ath', 'drawdown',
                                  'test_id', 'path_results']
        self.df_output_test_plan = pd.DataFrame(columns=self.test_plan_columns)


        self.list_strategies = 0
        self.list_sl = 0
        self.list_tp = 0
        self.split = 0
        self.interval = 0
        if(params):
            self.str_start_date = params.get("start", self.list_strategies)
            self.str_end_date = params.get("end", self.list_strategies)
            self.list_strategies = params.get("startegies", self.list_strategies)
            self.list_sl = params.get("sl", self.list_sl)
            self.list_tp = params.get("tp", self.list_tp)
            self.split = params.get("split", self.split)
            self.interval = params.get("interval", self.interval)

        self.start_date = datetime.strptime(self.str_start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(self.str_end_date, "%Y-%m-%d")

        delta = (self.end_date - self.start_date)/self.split

        self.list_date_intervals = []
        self.list_date_intervals.append(self.start_date)
        for i in range(1, self.split + 1):
            #self.list_date_intervals.append((self.start_date+i*delta).strftime('%Y%m%d'))
            self.list_date_intervals.append(self.start_date + i * delta)

        self.list_test_id = []
        list_end_date = self.list_date_intervals.copy()
        list_end_date = list_end_date[::-1]
        for start in self.list_date_intervals:
            list_end_date.remove(start)
            for end in list_end_date:
                for strategy in self.list_strategies:
                    for sl in self.list_sl:
                        for tp in self.list_tp:
                            # add empty row to df
                            empty_row = [None] * len(self.df_output_test_plan.columns)
                            self.df_output_test_plan.loc[len(self.df_output_test_plan)] = empty_row
                            self.df_output_test_plan.reset_index(inplace=True, drop=True)

                            self.df_output_test_plan['start'][len(self.df_output_test_plan)-1] = start
                            self.df_output_test_plan['end'][len(self.df_output_test_plan)- 1] = end
                            self.df_output_test_plan['split'][len(self.df_output_test_plan) - 1] = end - start
                            self.df_output_test_plan['interval'][len(self.df_output_test_plan) - 1] = self.interval
                            self.df_output_test_plan['strategy'][len(self.df_output_test_plan)- 1] = strategy
                            self.df_output_test_plan['sl'][len(self.df_output_test_plan) - 1] = sl
                            self.df_output_test_plan['tp'][len(self.df_output_test_plan) - 1] = tp
                            self.df_output_test_plan['period'][len(self.df_output_test_plan) - 1] = str(start)[0:10] + '_' + str(end)[0:10]
                            self.df_output_test_plan['test_id'][len(self.df_output_test_plan) - 1] = str(start)[0:10]+'_'+str(end)[0:10]+'_'+strategy+'_sl'+ str(sl)+'_tp'+str(tp)
                            self.df_output_test_plan['path_results'][len(self.df_output_test_plan) - 1] = self.path_results+str(start)[0:10]+'_'+str(end)[0:10]
                            if not os.path.exists(self.df_output_test_plan['path_results'][len(self.df_output_test_plan) - 1]):
                                os.makedirs(self.df_output_test_plan['path_results'][len(self.df_output_test_plan) - 1])
                            if not os.path.exists(self.df_output_test_plan['path_results'][len(self.df_output_test_plan) - 1] + '/data/'):
                                os.makedirs(self.df_output_test_plan['path_results'][len(self.df_output_test_plan) - 1] + '/data/')
                            if not os.path.exists(self.df_output_test_plan['path_results'][len(self.df_output_test_plan) - 1] + '/output/'):
                                os.makedirs(self.df_output_test_plan['path_results'][len(self.df_output_test_plan) - 1] + '/output/')
                            if not os.path.exists(self.df_output_test_plan['path_results'][len(self.df_output_test_plan) - 1] + '/benchmark/'):
                                os.makedirs(self.df_output_test_plan['path_results'][len(self.df_output_test_plan) - 1] + '/benchmark/')

    def savetofile(self):
        self.df_output_test_plan.to_csv(self.path_results + 'automatictestplan.csv')

def build_automatic_test_plan(params):
    automatictestplan = AutomaticTestPlan(params)
    automatictestplan.savetofile()
    return automatictestplan.df_output_test_plan

