from . import rtdp
import pandas as pd
import os
from . import utils,chronos
from . import features # temporary (before using fdp)

class SimRealTimeDataProvider(rtdp.IRealTimeDataProvider):
    def __init__(self, params = None):
        print("[SimRealTimeDataProvider] initialization...")
        self.input = "./data/"
        self.scheduler = chronos.Chronos()

        if params:
            self.input = params.get("input", self.input)
    
        self.data = {}
        # self.current_position = -1 # CEDE Test Debug
        # self.current_position = 400  # CEDE Test Debug Offset
        self.current_position = self.scheduler.get_current_position()

        if self.input == None or not os.path.exists(self.input):
            print(" 💥 ",self.input," not found")
            return
        print("[SimRealTimeDataProvider] reading data from ", self.input)
        list_dates = []
        files = os.listdir(self.input)
        for file in files:
            print(" ",format(file))
            strs = file.split('.')
            symbol = strs[0].replace("_", "/")
            if symbol in rtdp.default_symbols and strs[1] == "csv":
                df = pd.read_csv(self.input+"/"+file, sep=";")
                df.rename(columns={"Unnamed: 0": "datetime"}, inplace=True)

                df["ema_long"] = df["ema_long"].shift(1)
                df["ema_short"] = df["ema_short"].shift(1)
                df['super_trend_direction'] = df['super_trend_direction'].shift(1)

                # df_buy_sell = pd.read_csv('BTC_buy_sell.csv')
                # df['open_long_limit'] = df_buy_sell['open_long_limit']
                # df['close_long_limit'] = df_buy_sell['close_long_limit']

                #df.set_index('datetime', inplace=True)
                self.data[symbol] = df
                list_dates.extend(self.data[symbol]['timestamp'].to_list())
                list_dates = list(set(list_dates))
        df_timestamp = pd.DataFrame(list_dates, columns=['timestamp'])
        df_timestamp.set_index('timestamp', inplace=True)
        for symbol in self.data:
            self.data[symbol].set_index('timestamp', inplace=True)
            df_backup = self.data[symbol].copy()
            self.data[symbol] = pd.concat([self.data[symbol], df_timestamp], axis=0)
            self.data[symbol].sort_index(ascending=True, inplace=True)
            self.data[symbol] = self.data[symbol].index.drop_duplicates(keep=False)
            if len(self.data[symbol]) == 0:
                self.data[symbol] = df_backup.copy()
            else:
                self.data[symbol] = pd.DataFrame(index=self.data[symbol])
                self.data[symbol] = pd.concat([self.data[symbol], df_backup], axis=0)
                self.data[symbol] = self.data[symbol][~self.data[symbol].index.duplicated(keep='first')]
            self.data[symbol].sort_index(ascending=True, inplace=True)
            self.data[symbol].reset_index(inplace=True)
            self.data[symbol]['datetime'] = self.data[symbol].index
            if not os.path.exists('./data_processed'):
                os.makedirs('./data_processed')
            symbol_file_name = symbol.replace("/", "_")
            self.data[symbol].to_csv('./data_processed/' + symbol_file_name + '.csv')

    '''
    def _is_in_dataframe(self):
        if self.current_position < 0:
            return False
        if not bool(self.data):
            return False
        for symbol in self.data:
            if self.current_position >= len(self.data[symbol].index):
                return False
        return True
    '''

    def _is_in_dataframe(self):
        if self.scheduler.get_current_position() < 0:
            return False
        if not bool(self.data):
            return False
        for symbol in self.data:
            if self.scheduler.get_current_position() >= len(self.data[symbol].index):
                return False
        return True

    '''
    def _is_last_in_dataframe(self):
        for symbol in self.data:
            if self.current_position >= len(self.data[symbol].index):
                return True
        return False
    '''

    def _is_last_in_dataframe(self):
        for symbol in self.data:
            if self.scheduler.get_current_position() >= len(self.data[symbol].index):
                return True
        return False

    def tick(self):
        self.scheduler.increment_time()

    def get_current_data(self, data_description):
        self.current_position = self.scheduler.get_current_position()
        if not self._is_in_dataframe():
            return None

        df_result = pd.DataFrame(columns=['symbol'])
        for symbol in data_description.symbols:
            if symbol in self.data:
                df_symbol = self.data[symbol]
                available_columns = list(df_symbol.columns)
                row = {'symbol':symbol}
                for feature in data_description.features:
                    if feature not in available_columns:
                        return None
                    # row[feature] = [df_symbol[feature].iloc[self.current_position]]
                    row[feature] = [df_symbol[feature].iloc[self.scheduler.get_current_position()]]


                df_row = pd.DataFrame(data=row)
                df_result = pd.concat((df_result, df_row), axis = 0)

        df_result.set_index("symbol", inplace=True)

        return df_result

    def get_value(self, symbol):
        if not self._is_in_dataframe() or not symbol in self.data:
            if self._is_last_in_dataframe() and symbol in self.data:
                df_symbol = self.data[symbol]
                value = df_symbol.iloc[len(self.data[symbol].index)-1]['close']
                return value
            else:
                return -1
        df_symbol = self.data[symbol]
        # value = df_symbol.iloc[self.current_position]['close']
        value = df_symbol.iloc[self.scheduler.get_current_position()]['close']
        return value

    def get_current_datetime(self):
        if not self._is_in_dataframe():
            return None
        return self.scheduler.get_current_time()

    def record(self, data_description, target="./data/"):
        symbols = ','.join(data_description.symbols)
        symbols = symbols.replace('/','_')
        url = "history?exchange=ftx&symbol="+symbols+"&start=01_01_2021"+"&interval=1h"+"&length=9000"
        # url = "history?exchange=ftx&symbol=" + symbols + "&start=01_01_2020" + "&interval=1h" + "&length=400"
        # url = "history?exchange=ftx&symbol=" + symbols + "&start=01_01_2021" + "&interval=1h" + "&end=01_01_2022"
        response_json = utils.fdp_request(url)
        for symbol in data_description.symbols:
            formatted_symbol = symbol.replace('/','_')
            if response_json["result"][formatted_symbol]["status"] == "ko":
                print("no data for ",symbol)
                continue
            df = pd.read_json(response_json["result"][formatted_symbol]["info"])
            df = features.add_features(df, data_description.features)
            if not os.path.exists(target):
                os.makedirs(target)
            df.to_csv(target+'/'+formatted_symbol+".csv", sep=";")

