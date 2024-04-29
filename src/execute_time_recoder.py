from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import utils
import os
import psutil

from concurrent.futures import ThreadPoolExecutor

class ExecuteTimeRecorder():
    def __init__(self, iter):
        self.lst_recorder_columns = ["csci", "section", "cycle", "position", "start", "end", "duration"]
        self.df_time_recorder = pd.DataFrame(columns=self.lst_recorder_columns)
        self.lst_system_recorder_columns = ["cycle", "RAM_used_percent", "RAM_used_GB", "CPU_usage_percent"]
        self.df_system_recorder = pd.DataFrame(columns=self.lst_system_recorder_columns)
        self.df_grid = pd.DataFrame()
        self.dump_directory = "./dump_timer"
        self.dump_time_directory = self.dump_directory + "/timer"
        self.dump_system_directory = self.dump_directory + "/system"
        self.dump_grid_directory = self.dump_directory + "/grid"
        lst_dir = [self.dump_directory, self.dump_time_directory, self.dump_system_directory, self.dump_grid_directory]
        for dir in lst_dir:
            utils.create_dir(dir)
            if iter == 0:
                utils.empty_files(dir, pattern=".png")
                utils.empty_files(dir, pattern=".csv")
            elif iter == -1:
                utils.empty_files(dir, pattern=".png")
        del lst_dir

        self.plot_df_timer = False
        self.plot_df_system = False
        self.plot_df_grid = False



    def set_master_cycle(self, cycle):
        self.master_cycle = utils.format_integer(cycle)

    def reset_time_recoder(self):
        del self.df_time_recorder
        self.df_time_recorder = pd.DataFrame(columns=self.lst_recorder_columns)
        del self.df_system_recorder
        self.df_system_recorder = pd.DataFrame(columns=self.lst_system_recorder_columns)
        self.df_grid
        self.df_grid = pd.DataFrame()

    def set_system_record(self, cycle):
        RAM_used_percent = psutil.cpu_percent(4)
        RAM_used_GB = psutil.virtual_memory()[3] / 1000000000
        cpu_usage = psutil.cpu_percent(4)
        # total_memory, used_memory, free_memory = map(int, os.popen('free -t -m').readlines()[-1].split()[1:])
        # Total_RAM_used_with_os = round((used_memory/total_memory) * 100, 2)
        # load1, load5, load15 = psutil.getloadavg()
        # avg_cpu_usage = (load15/os.cpu_count()) * 100

        new_row = {
            "cycle": cycle,
            "RAM_used_percent": RAM_used_percent,
            "RAM_used_GB": RAM_used_GB,
            "CPU_usage_percent": cpu_usage
        }

        self.df_system_recorder = pd.concat([self.df_system_recorder, pd.DataFrame([new_row])], ignore_index=True)
        del new_row

    def set_start_time(self, csci, section, position, cycle):
        current_start_timestamp = int(datetime.now().timestamp())
        new_row = {
            "csci": csci,
            "section": section,
            "cycle": cycle,
            "position": position,
            "start": current_start_timestamp,
            "end": None,
            "duration": None
        }
        self.df_time_recorder = pd.concat([self.df_time_recorder, pd.DataFrame([new_row])], ignore_index=True)
        del new_row


    def set_time_to_zero(self, csci, section, position, cycle):
        new_row = {
            "csci": csci,
            "section": section,
            "cycle": cycle,
            "position": position,
            "start": 0,
            "end": 0,
            "duration": 0
        }
        self.df_time_recorder = pd.concat([self.df_time_recorder, pd.DataFrame([new_row])], ignore_index=True)
        del new_row

    def set_system_record_to_zero(self, cycle):
        new_row = {
            "cycle": cycle,
            "RAM_used_percent": 0,
            "RAM_used_GB": 0,
            "CPU_usage_percent": 0
        }
        self.df_time_recorder = pd.concat([self.df_time_recorder, pd.DataFrame([new_row])], ignore_index=True)
        del new_row

    def set_grid_infos(self, df_grid):
        if len(self.df_grid) == 0:
            self.df_grid = df_grid.copy()
            self.df_grid.rename(columns={'values': '0'}, inplace=True)
        else:
            # Check if the new column is different from the last column
            if (self.df_grid.iloc[:, -1] != df_grid["values"]).any():
                # Join all columns at once using pd.concat(axis=1)
                last_column_name = str(int(self.df_grid.columns[-1]) + 1)
                df_grid.rename(columns={'values': last_column_name}, inplace=True)
                new_columns = df_grid[last_column_name]
                self.df_grid = pd.concat([self.df_grid, new_columns], axis=1)

        # print(self.df_grid.to_string())
        del df_grid

    def set_end_time(self, csci, section, position, cycle):
        current_end_timestamp = datetime.now().timestamp()
        # Find the row to update based on csci, section, and cycle
        mask = (self.df_time_recorder["csci"] == csci) & \
               (self.df_time_recorder["section"] == section) & \
               (self.df_time_recorder["cycle"] == cycle) & \
               (self.df_time_recorder["position"] == position)
        # Calculate duration
        self.df_time_recorder.loc[mask, "end"] = current_end_timestamp
        self.df_time_recorder.loc[mask, "duration"] = self.df_time_recorder.loc[mask, "end"] - self.df_time_recorder.loc[mask, "start"]

    def close_timer_thread(self):
        if False:
            with ThreadPoolExecutor(max_workers=3) as executor:
                # Submit each function to the executor
                futures = []
                futures.append(executor.submit(self.close_timer))
                futures.append(executor.submit(self.close_system))
                futures.append(executor.submit(self.close_grid))

                # Wait for all futures to complete
                for future in futures:
                    future.result()
        else:
            self.close_timer()
            print("....self.close_timer() ending")
            self.close_system()
            print("....self.close_system() ending")
            self.close_grid()
            print("....self.close_grid() ending")

    def close_timer(self):
        # Save DataFrame to CSV
        self.df_time_recorder.to_csv(self.dump_time_directory + "/" + self.master_cycle + "_df_time_recorder.csv")
        if self.plot_df_timer:
            self.trigger_plot_df_time()

    def plot_all_close_timer(self):
        csv_files = [file for file in os.listdir(self.dump_time_directory) if file.endswith('.csv')]
        for filename in csv_files:
            self.df_time_recorder = pd.read_csv(os.path.join(self.dump_time_directory, filename))
            self.master_cycle = filename.split("-")[0]
            self.trigger_plot_df_time()

    def trigger_plot_df_time(self):
        # Drop columns
        self.df_time_recorder.drop(columns=['start', 'end'], inplace=True)
        # Get unique CSCI values
        unique_csci = self.df_time_recorder["csci"].unique()
        for csci in unique_csci:
            csci_mask = (self.df_time_recorder["csci"] == csci)
            df_filtered_csci = self.df_time_recorder.loc[csci_mask].copy()
            # Get unique section values
            unique_section = df_filtered_csci["section"].unique()
            for section in unique_section:
                section_mask = (df_filtered_csci["section"] == section)
                df_filtered_section = df_filtered_csci.loc[section_mask].copy()
                # Get unique position values
                unique_position = df_filtered_section["position"].unique()
                for position in unique_position:
                    position_mask = (df_filtered_section["position"] == position)
                    df_filtered_position = df_filtered_section.loc[position_mask].copy()
                    # Save the plot for this cycle
                    self.save_cycle_plot(df_filtered_position, csci, section, position)

    def close_system(self):
        self.df_system_recorder.to_csv(self.dump_system_directory + "/" + self.master_cycle + "_df_system_recorder.csv")
        if self.plot_df_timer:
            self.trigger_plot_df_system()

    def plot_all_close_system(self):
        csv_files = [file for file in os.listdir(self.dump_system_directory) if file.endswith('.csv')]
        for filename in csv_files:
            self.df_system_recorder = pd.read_csv(os.path.join(self.dump_system_directory, filename))
            self.master_cycle = filename.split("-")[0]
            self.trigger_plot_df_system()

    def trigger_plot_df_system(self):
        lst_section = self.df_system_recorder.columns.to_list()
        lst_section.remove("cycle")
        for section in lst_section:
            self.save_system_cycle_plot(self.df_system_recorder, section)

    def close_grid(self):
        self.df_grid = utils.concat_csv_files_with_df(self.dump_grid_directory, self.df_grid)
        utils.empty_files(self.dump_grid_directory, pattern=".png")
        utils.empty_files(self.dump_grid_directory, pattern=".csv")
        self.df_grid.to_csv(self.dump_grid_directory + "/" + self.master_cycle + "_df_grid_recorder.csv")

        if self.plot_df_grid:
            self.trigger_plot_df_grid()

    def plot_all_close_grid(self):
        csv_files = [file for file in os.listdir(self.dump_grid_directory) if file.endswith('.csv')]
        for filename in csv_files:
            self.df_grid = pd.read_csv(os.path.join(self.dump_grid_directory, filename))
            self.master_cycle = filename.split("-")[0]
            self.trigger_plot_df_grid()

    def trigger_plot_df_grid(self):
        self.df_grid = utils.drop_duplicate_grid_columns(self.df_grid)
        self.save_grid_cycle_plot()

    def save_cycle_plot(self, df_filtered_cycle, csci, section, position):
        plt.figure(figsize=(30, 10))
        plt.bar(df_filtered_cycle["cycle"], df_filtered_cycle["duration"])
        plt.xlabel("Cycle")
        plt.ylabel("Duration")
        plt.title(f"{csci}   -   {section}   -   {position}")
        plt.grid(True)
        plt.savefig(f"{self.dump_time_directory}/{self.master_cycle}-{csci}-{section}-{position}.png")
        plt.close()

    def save_system_cycle_plot(self, df_filtered_cycle, section):
        plt.figure(figsize=(30, 10))
        plt.bar(df_filtered_cycle["cycle"], df_filtered_cycle[section])
        plt.xlabel("Cycle")
        plt.ylabel(section)
        plt.title(f"{section}")
        plt.grid(True)
        plt.savefig(f"{self.dump_system_directory}/{self.master_cycle}-{section}.png")
        plt.close()

    def save_grid_cycle_plot(self):
        # Plotting
        color_map = {'close_long_on_hold': 'silver',
                     'close_long_pending': 'gold',
                     'close_long_engaged': 'red',
                     'open_long_on_hold': 'grey',
                     'open_long_pending': 'green',
                     'open_long_engaged': 'blue'
                     }

        plt.figure(figsize=(50, 15))
        for position, row in self.df_grid.iterrows():
            for state, value in row.items():  # Use items() instead of iteritems()
                color = color_map[value]
                plt.scatter(state, position, c=color)  # Switched state and position

        plt.xlabel('Y')  # Label switched to match the new axis
        plt.ylabel('X')  # Label switched to match the new axis
        plt.title('Scatter Plot')
        plt.grid(True)
        plt.savefig(f"{self.dump_grid_directory}/{self.master_cycle}-grid.png")
        plt.close()
