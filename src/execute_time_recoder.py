from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import utils
import os
from pathlib import Path
import shutil
import psutil
import numpy as np

from concurrent.futures import ThreadPoolExecutor

class ExecuteTimeRecorder():
    def __init__(self, iter, directory):
        self.lst_recorder_columns = ["csci", "section", "cycle", "position", "start", "end", "duration"]
        self.df_time_recorder = pd.DataFrame(columns=self.lst_recorder_columns)
        self.lst_system_recorder_columns = ["cycle", "RAM_used_percent", "RAM_used_GB", "CPU_usage_percent"]
        self.df_system_recorder = pd.DataFrame(columns=self.lst_system_recorder_columns)
        self.df_grid = pd.DataFrame()
        self.dct_grid = {
            "long": None,
            "short": None
        }

        self.set_input_dir(iter, directory)
        self.archive_id = 0

        self.plot_df_timer = False
        self.plot_df_system = False
        self.plot_df_grid = False

    def check_log(self):
        dump_time_path = Path(self.dump_time_directory)
        size = sum(f.stat().st_size for f in dump_time_path.glob('**/*') if f.is_file())
        if size > 100000000:  # 100Mo
            files = [f for f in os.listdir(self.dump_time_directory) if
                     os.path.isfile(os.path.join(self.dump_time_directory, f))]
            archive_name = "dump_timer_{:04d}".format(self.archive_id)
            self.archive_id += 1
            shutil.make_archive(os.path.join(self.dump_directory, archive_name), "zip", self.dump_time_directory)
            for file in files:
                os.remove(os.path.join(self.dump_time_directory, file))

        self.scenario_directory = None

    def set_input_dir(self, iter, directory):
        self.dump_directory = directory
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

    def set_grid_infos(self, df_grid, side):
        self.df_grid = self.dct_grid[side]
        if self.df_grid is None:
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

        self.dct_grid[side] = self.df_grid

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

        self.check_log()

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
        # self.df_grid = utils.concat_csv_files_with_df(self.dump_grid_directory, self.df_grid)
        # utils.empty_files(self.dump_grid_directory, pattern=".png")
        # utils.empty_files(self.dump_grid_directory, pattern=".csv")
        keys_list = list(self.dct_grid.keys())
        for side in keys_list:
            if not (self.dct_grid[side] is None):
                self.dct_grid[side].to_csv(self.dump_grid_directory + "/" + self.master_cycle + "_" + side + "_df_grid_recorder.csv")

        if self.plot_df_grid:
            self.trigger_plot_df_grid()

    def set_scenario_directory(self, directory):
        self.scenario_directory = directory

    def close_grid_scenario(self):
        # self.df_grid = utils.concat_csv_files_with_df(self.dump_grid_directory, self.df_grid)
        # utils.empty_files(self.dump_grid_directory, pattern=".png")
        # utils.empty_files(self.dump_grid_directory, pattern=".csv")
        keys_list = list(self.dct_grid.keys())
        for side in keys_list:
            if not (self.dct_grid[side] is None):
                self.dct_grid[side].to_csv(self.scenario_directory + "/" + self.master_cycle + "_" + side + "_df_grid_recorder.csv")

        if self.plot_df_grid:
            self.trigger_plot_df_grid()

    def plot_all_close_grid(self):
        pattern_csv = ".csv"
        self.df_grid = utils.concat_csv_files_with_df(self.dump_grid_directory, pattern_csv)
        self.trigger_plot_df_grid()

    def plot_all_close_grid_scenario(self):
        pattern_csv = "df_grid_recorder.csv"
        self.df_grid = utils.concat_csv_files_with_df(self.scenario_directory, pattern_csv)
        self.trigger_plot_df_grid()

    def trigger_plot_df_grid(self):
        self.df_grid = utils.drop_duplicate_grid_columns(self.df_grid)
        chunk_size = 200
        num_columns = len(self.df_grid.columns)
        if num_columns <= chunk_size:
            lst_df = [self.df_grid]
        else:
            num_chunks = num_columns // chunk_size + (1 if num_columns % chunk_size != 0 else 0)
            lst_df = [self.df_grid.iloc[:, i * chunk_size:(i + 1) * chunk_size] for i in range(num_chunks)]
        cpt = 0
        for df in lst_df:
            self.df_grid = df
            self.master_cycle = "000000" + str(cpt)
            self.save_grid_cycle_plot()
            cpt += 1

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
    """
    def save_grid_cycle_plot(self):
        # Plotting
        if self.df_grid.apply(lambda x: x.str.contains('_long_')).any().any():
            color_map = {'close_long_on_hold': 'silver',
                         'close_long_pending': 'gold',
                         'close_long_engaged': 'red',
                         'open_long_on_hold': 'grey',
                         'open_long_pending': 'orange',
                         'open_long_engaged': 'blue'
                         }
        else:
            color_map = {'close_short_on_hold': 'silver',
                         'close_short_pending': 'gold',
                         'close_short_engaged': 'red',
                         'open_short_on_hold': 'grey',
                         'open_short_pending': 'orange',
                         'open_short_engaged': 'blue'
                         }

        positions = self.df_grid.index.values  # Get positions
        states = self.df_grid.columns.values  # Get states
        colors = np.vectorize(color_map.get)(self.df_grid.values)  # Map values to colors

        plt.figure(figsize=(30, 15))
        for i in range(len(positions)):
            try:
                plt.scatter(states, np.full(len(states), positions[i]), c=colors[i])
            except:
                print("titi")

        plt.xlabel('Y')  # Label switched to match the new axis
        plt.ylabel('X')  # Label switched to match the new axis
        plt.title('Scatter Plot')
        plt.grid(True)
        if self.scenario_directory is None:
            plt.savefig(f"{self.dump_grid_directory}/{self.master_cycle}-grid.png")
        else:
            plt.savefig(f"{self.scenario_directory}/{self.master_cycle}-grid.png")
        plt.close()
    """

    def save_grid_cycle_plot(self):
        # Determine the color map based on whether '_long_' is in any of the DataFrame values
        if self.df_grid.apply(lambda x: x.str.contains('_long_')).any().any():
            color_map = {
                'close_long_on_hold': 'silver',
                'close_long_pending': 'gold',
                'close_long_engaged': 'red',
                'open_long_on_hold': 'grey',
                'open_long_pending': 'orange',
                'open_long_engaged': 'blue'
            }
        else:
            color_map = {
                'close_short_on_hold': 'silver',
                'close_short_pending': 'gold',
                'close_short_engaged': 'red',
                'open_short_on_hold': 'grey',
                'open_short_pending': 'orange',
                'open_short_engaged': 'blue'
            }

        positions = self.df_grid.index.values  # Get positions
        states = self.df_grid.columns.values  # Get states

        # Map DataFrame values to colors, use 'black' as default color for unmapped values
        colors = self.df_grid.applymap(lambda x: color_map.get(x, 'black')).values

        plt.figure(figsize=(30, 15))

        for i, position in enumerate(positions):
            state_colors = colors[i]
            plt.scatter(states, np.full(len(states), position), c=state_colors, s=100, edgecolors='w')

        plt.xlabel('States')  # Changed for clarity
        plt.ylabel('Positions')  # Changed for clarity
        plt.title('Scatter Plot of Grid Cycles')
        plt.grid(True)

        # Save the plot
        file_path = f"{self.scenario_directory or self.dump_grid_directory}/{self.master_cycle}-grid.png"
        plt.savefig(file_path)
        plt.close()
