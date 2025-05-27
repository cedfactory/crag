import pandas as pd
import datetime
import os
import gzip

class DataFrameStore:
    def __init__(self, filename):
        # Define the expected columns that must be set manually.
        self.required_columns = [
            "symbol",
            "price",
            "close",
            "zerolag_ma_buy_adj",
            "zerolag_ma_sell_adj",
            "trend_indicator",
            "high_tf_close",
            "s_highest_window",
            "s_lowest_window",
            "trend_signal",
            "below_ma",
            "trend_up",
            "signal_buy",
            "sell_adj",
            "signal_sell",
            "fdp_source_trend",
            "fdp_source_zeroma",
            "released_dt",
            "index_ws"
        ]
        # Full list of columns including the auto-generated time column.
        self.columns = self.required_columns + ["time"]
        self.columns = self.required_columns + ["datetime"]
        # Create an empty DataFrame with these columns.
        self.df = pd.DataFrame(columns=self.columns)
        # Temporary storage for a row that's being built.
        self.temp_row = {}
        # Store the CSV filename passed during initialization.
        self.filename = filename
        self.current_id = 1
        self.max_size = 10000

    def set_data(self, **kwargs):
        """
        Set values for the row.
        This method accepts keyword arguments that correspond to valid user-supplied columns.
        When all required columns have been set, the row is automatically added to the DataFrame.
        """
        for key, value in kwargs.items():
            if key in self.required_columns:
                self.temp_row[key] = value
            else:
                raise ValueError(f"Invalid column: {key}")

        # Check if the current temporary row is full.
        if self.row_is_full():
            self.add_row()

    def row_is_full(self):
        """
        Check if the temporary row has values for all the required columns.
        """
        return all(col in self.temp_row for col in self.required_columns)

    def add_row(self):
        """
        Add the current temporary row to the DataFrame and reset the temporary row.
        Automatically adds the current time (formatted as YYYY-MM-DD HH:MM:SS) to the 'time' column.
        """
        # Automatically set the time if not provided.
        if "time" not in self.temp_row:
            self.temp_row["time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if "datetime" not in self.temp_row:
            now = datetime.datetime.now().replace(second=0, microsecond=0)
            self.temp_row["datetime"] = now.strftime("%Y-%m-%d %H:%M:%S")

        # Append the new row to the DataFrame.
        new_row = pd.DataFrame([self.temp_row])
        self.df = pd.concat([self.df, new_row], ignore_index=True)

        # Clear the temporary row after appending.
        self.temp_row = {}

    def _get_filename(self):
        return "{}_{:04d}.csv".format(self.filename, self.current_id)

    def _get_filesize(self, filename):
        if filename != "" and os.path.isfile(filename):
            return os.stat(filename).st_size
        return 0

    def save_to_csv(self):
        """
        Save the current DataFrame to a CSV file using the filename provided at initialization.
        """
        #self.df = self.df.tail(14000) # CEDE 10 days
        current_filename = self._get_filename()
        self.df.to_csv(current_filename, index=False)

        if self._get_filesize(current_filename) > self.max_size:
            # gzip current log file
            with open(current_filename, "rb") as f_in, gzip.open(current_filename+".gz", "wb") as f_out:
                f_out.writelines(f_in)
            try:
                os.remove(current_filename)
            except Exception as e:
                print("!!! can't remove", current_filename)

            self.df = pd.DataFrame(columns=self.columns)

            self.current_id += 1

        # print(f"DataFrame saved to {self.filename}")
