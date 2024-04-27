from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from . import utils
import os
import psutil

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
        del lst_dir


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
                new_columns = df_grid["values"]
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

    def close_timer(self):
        self.df_time_recorder.to_csv(self.dump_time_directory + "/" + self.master_cycle + "_df_time_recorder.csv")
        self.df_time_recorder.drop(columns=['start', 'end'], inplace=True)
        lst_csci = self.df_time_recorder["csci"].to_list()
        lst_csci = list(set(lst_csci))
        for csci in lst_csci:
            mask = (self.df_time_recorder["csci"] == csci)
            df_filtered_csci = self.df_time_recorder[mask].copy()
            lst_section = list(set(self.df_time_recorder["section"].to_list()))
            for section in lst_section:
                mask = (self.df_time_recorder["section"] == section)
                df_filtered_section = df_filtered_csci[mask].copy()
                lst_position = list(set(df_filtered_section["position"].to_list()))
                for position in lst_position:
                    mask = (self.df_time_recorder["position"] == position)
                    df_filtered_position = df_filtered_section[mask].copy()
                    # Save the plot for this cycle
                    self.save_cycle_plot(df_filtered_position, csci, section, position)
        self.close_system()

    def close_system(self):
        self.df_system_recorder.to_csv(self.dump_system_directory + "/" + self.master_cycle + "_df_system_recorder.csv")
        lst_section = self.df_system_recorder.columns.to_list()
        lst_section.remove("cycle")
        for section in lst_section:
            self.save_system_cycle_plot(self.df_system_recorder, section)
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
        self.df_grid = utils.concat_csv_files(self.dump_grid_directory, self.df_grid)
        utils.delete_files_by_pattern(self.dump_grid_directory, '.csv')
        utils.delete_files_by_pattern(self.dump_grid_directory, '.png')
        self.df_grid.to_csv(self.dump_grid_directory + "/" + self.df_grid + "_df_grid_recorder.csv")

        # Plotting
        color_map = {'close_long_on_hold': 'grey',
                     'close_long_pending': 'orange',
                     'close_long_engaged': 'red',
                     'open_long_on_hold': 'grey',
                     'open_long_pending': 'yellow',
                     'open_long_engaged': 'blue'
                     }

        plt.figure(figsize=(30, 15))
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
