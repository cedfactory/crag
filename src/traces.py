import pandas as pd
import datetime
import os
import shutil

class TradeTraces():
    def __init__(self):
        self.output_dir = "./traces/"
        self.output_filename = self.output_dir + "trade_traces_export.csv"
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        else:
            shutil.rmtree(self.output_dir)
            os.makedirs(self.output_dir)

        self.df_traces = pd.DataFrame(columns=self.get_header())
        self.last_execution_time = datetime.datetime.now() - datetime.timedelta(hours=2)

    def get_header(self):
        return ["buying_time", "selling_time",
                "symbol", "trade_id",
                "size",
                'buying_symbol_price', 'selling_symbol_price',
                "buying_value", "selling_value",
                "fee",
                "roi$", "roi%"]

    def add_new_entry(self, symbol, trade_id, size, symbol_price, buying_value, buying_fee):
        current_time = datetime.datetime.now()
        # Create a new row as a Series with values
        new_row = pd.Series({'symbol': symbol,
                             'trade_id': trade_id,
                             'buying_time': current_time.strftime("%Y-%m-%d %H:%M:%S"),
                             'size': size,
                             'buying_symbol_price': symbol_price,
                             'buying_value': buying_value,
                             'fee': buying_fee
                             })

        # Use loc to add the new row with NaN values
        self.df_traces = self.df_traces.append(new_row, ignore_index=True)

        # Optionally, you can fill NaN values with a default value (e.g., -1)
        self.df_traces.fillna(-1, inplace=True)

    def set_sell(self, symbol, trade_id, symbol_price, selling_value, selling_fee):
        # Condition to filter rows
        condition = (self.df_traces['symbol'] == symbol) & (self.df_traces['trade_id'] == trade_id)

        if len(self.df_traces) > 0 and len(self.df_traces[condition] > 0):
            current_time = datetime.datetime.now()

            self.df_traces.loc[condition, 'selling_time'] = current_time.strftime("%Y-%m-%d %H:%M:%S")
            self.df_traces.loc[condition, 'selling_symbol_price'] = symbol_price
            self.df_traces.loc[condition, 'selling_value'] = selling_value

            buying_fee = self.df_traces.loc[condition, 'fee'].iloc[0]
            fee = buying_fee + selling_fee
            self.df_traces.loc[condition, 'fee'] = fee

            buying_value = self.df_traces.loc[condition, 'buying_value'].iloc[0]

            self.df_traces.loc[condition, 'roi$'] = selling_value - buying_value
            self.df_traces.loc[condition, 'roi%'] = (selling_value - buying_value) / selling_value

    def export(self):
        current_time = datetime.datetime.now()

        # Check if it's been more than one hour since the last execution
        if (len(self.df_traces) > 0) \
                and ((current_time - self.last_execution_time).total_seconds() >= 3600):  # 3600 seconds in an hour
            try:
                self.df_traces.to_csv(self.output_filename)
                self.last_execution_time = current_time
            except:
                print("************** traces export failed **************")

