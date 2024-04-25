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
        self.dump_directory = "./dump_timer"
        self.dump_time_directory = self.dump_directory + "/timer"
        self.dump_system_directory = self.dump_directory + "/system"
        utils.create_dir(self.dump_directory)
        utils.create_dir(self.dump_time_directory)
        utils.create_dir(self.dump_system_directory)
        if iter == 0:
            utils.empty_files(self.dump_directory, pattern=".png")
            utils.empty_files(self.dump_directory, pattern=".csv")
            utils.empty_files(self.dump_time_directory, pattern=".png")
            utils.empty_files(self.dump_time_directory, pattern=".csv")
            utils.empty_files(self.dump_system_directory, pattern=".png")
            utils.empty_files(self.dump_system_directory, pattern=".csv")

    def set_master_cycle(self, cycle):
        self.master_cycle = utils.format_integer(cycle)

    def reset_time_recoder(self):
        del self.df_time_recorder
        self.df_time_recorder = pd.DataFrame(columns=self.lst_recorder_columns)
        del self.df_system_recorder
        self.df_system_recorder = pd.DataFrame(columns=self.lst_system_recorder_columns)

    def set_system_record(self, cycle):
        RAM_used_percent = psutil.cpu_percent(4)
        RAM_used_GB = psutil.virtual_memory()[3] / 1000000000

        # total_memory, used_memory, free_memory = map(int, os.popen('free -t -m').readlines()[-1].split()[1:])
        # Total_RAM_used_with_os = round((used_memory/total_memory) * 100, 2)

        cpu_usage = psutil.cpu_percent(4)

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

