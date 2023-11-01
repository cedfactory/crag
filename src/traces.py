import pandas as pd
import datetime
import os
import shutil

class TradeTraces():
    def __init__(self):
        self.output_dir = "./traces"
        self.output_backup_dir = "./traces_backup"

        self.output_filename = os.path.join(self.output_dir, "trade_traces_export.csv")
        self.output_backup_filename = os.path.join(self.output_backup_dir, "trade_traces_export.csv")

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        else:
            shutil.rmtree(self.output_dir)
            os.makedirs(self.output_dir)
        if not os.path.exists(self.output_backup_dir):
            os.makedirs(self.output_backup_dir)

        self.df_traces = pd.DataFrame(columns=self.get_header())
        self.last_execution_time = datetime.datetime.now() - datetime.timedelta(hours=2)

        self.counter = 0

    def get_header(self):
        return ["symbol", 'selling_symbol', "trade_id", "type",
                "buying_time", "selling_time",
                "size",
                'buying_symbol_price', 'selling_symbol_price',
                "buying_value", "selling_value",
                "fee",
                "roi$", "roi%",
                "signal"]

    def add_new_entry(self, symbol, trace_id, clientOid, size, symbol_price, buying_value, buying_fee):
        current_time = datetime.datetime.now()
        # str_current_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
        str_current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        if "OPEN_LONG" in clientOid:
            str_type = "LONG"
        elif "OPEN_SHORT" in clientOid:
            str_type = "SHORT"
        else:
            str_type = "-"

        new_row = [symbol, "", trace_id, str_type,
                   str_current_time, "",
                   size,
                   symbol_price, 0,
                   buying_value, 0,
                   buying_fee,
                   0, 0, ""
                   ]

        self.df_traces.loc[len(self.df_traces)] = new_row

    def set_sell(self, symbol, trace_id, symbol_price, selling_value, selling_fee, str_signal):
        # Condition to filter rows
        # condition = (self.df_traces['symbol'] == symbol) & (self.df_traces['trade_id'] == trace_id)
        condition = (self.df_traces['trade_id'] == trace_id)

        if ((len(self.df_traces) > 0)
                and (len(self.df_traces[condition]) > 0)):
            print("traces set sell start")
            current_time = datetime.datetime.now()

            self.df_traces.loc[condition, 'selling_symbol'] = symbol
            self.df_traces.loc[condition, 'selling_time'] = current_time.strftime("%Y-%m-%d %H:%M:%S")
            self.df_traces.loc[condition, 'selling_symbol_price'] = symbol_price
            self.df_traces.loc[condition, 'selling_value'] = selling_value
            self.df_traces.loc[condition, 'signal'] = str_signal


            buying_fee = self.df_traces.loc[condition, 'fee'].iloc[0]

            print("buying_fee: ", buying_fee, " - ", type(buying_fee))
            print("selling_fee: ", selling_fee, " - ", type(selling_fee))

            if not isinstance(buying_fee, float):
                if isinstance(buying_fee, str):
                    print("DEBUG FEE  buying_fee IS NOT FLOAT")
                    buying_fee = float(buying_fee)
            if not isinstance(selling_fee, float):
                if isinstance(selling_fee, str):
                    print("DEBUG FEE selling_fee IS NOT FLOAT - could not convert string to float: ''")
                    try:
                        selling_fee = float(selling_fee)
                    except:
                        selling_fee = 0.0

            print("DEBUG - buying_fee: ", buying_fee, " - ", type(buying_fee))
            print("DEBUG - selling_fee: ", selling_fee, " - ", type(selling_fee))

            fee = buying_fee + selling_fee

            print("DEBUG - fee: ", fee, " - ", type(fee))

            self.df_traces.loc[condition, 'fee'] = fee

            buying_value = self.df_traces.loc[condition, 'buying_value'].iloc[0]

            print("DEBUG - buying_value: ", buying_value, " - ", type(buying_value))
            print("DEBUG - selling_value: ", selling_value, " - ", type(selling_value))

            if not isinstance(buying_value, float):
                if isinstance(buying_value, str):
                    print("DEBUG buying_value IS NOT FLOAT")
                    buying_value = float(buying_value)
            if not isinstance(selling_value, float):
                if isinstance(selling_value, str):
                    print("DEBUG selling_value IS NOT FLOAT")
                    selling_value = float(selling_value)

            print("DEBUG - buying_value: ", buying_value, " - ", type(buying_value))
            print("DEBUG - selling_value: ", selling_value, " - ", type(selling_value))

            if self.df_traces.loc[condition, 'type'].values[0] == "SHORT":
                multi = -1
            else:
                multi = 1
            if selling_value == buying_value:
                self.df_traces.loc[condition, 'roi$'] = 0
                self.df_traces.loc[condition, 'roi%'] = 0
            else:
                self.df_traces.loc[condition, 'roi$'] = multi * (selling_value - buying_value)
                self.df_traces.loc[condition, 'roi%'] = multi * (selling_value - buying_value) / selling_value

            print("DEBUG - traces set sell ok")
        else:
            print("TRACES ERROR - trade.id NOT FOUND ", len(self.df_traces), " - ", len(self.df_traces[condition] > 0))

    def export(self):
        current_time = datetime.datetime.now()

        # Check if it's been more than one hour since the last execution
        if (len(self.df_traces) > 0) \
                and ((current_time - self.last_execution_time).total_seconds() >= 3600):  # 3600 seconds in an hour
            try:
                self.df_traces.to_csv(self.output_filename, sep=";")
                self.last_execution_time = current_time
            except:
                print("************** traces export failed **************")

            # Check if the source file exists
            if os.path.exists(self.output_filename):
                # Get the current date and time
                current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

                # Create a new file name with the date and time appended
                destination_file_path = os.path.splitext(self.output_backup_filename)[0] \
                                        + '_' + current_datetime \
                                        + os.path.splitext(self.output_backup_filename)[1]

                # Use shutil.copy() to copy the file
                shutil.copy(self.output_filename, destination_file_path)


