from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import utils

class ExecuteTimeRecorder():
    def __init__(self):
        self.lst_recorder_columns = ["csci", "section", "cycle", "position", "start", "end", "duration"]
        self.df_time_recorder = pd.DataFrame(columns=self.lst_recorder_columns)
        self.dump_directory = "./dump_timer"
        utils.create_dir(self.dump_directory)
        utils.empty_files(self.dump_directory, pattern=".png")
        utils.empty_files(self.dump_directory, pattern=".csv")

    def set_master_cycle(self, cycle):
        self.master_cycle = utils.format_integer(cycle)

    def reset_time_recoder(self):
        del self.df_time_recorder
        self.df_time_recorder = pd.DataFrame(columns=self.lst_recorder_columns)

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
        self.df_time_recorder.to_csv(self.dump_directory + "/" + self.master_cycle + "df_time_recorder.csv")
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

    def save_cycle_plot(self, df_filtered_cycle, csci, section, position):
        plt.figure(figsize=(30, 10))
        plt.bar(df_filtered_cycle["cycle"], df_filtered_cycle["duration"])
        plt.xlabel("Cycle")
        plt.ylabel("Duration")
        plt.title(f"{csci}   -   {section}   -   {position}")
        plt.grid(True)
        plt.savefig(f"{self.dump_directory}/{self.master_cycle}-{csci}-{section}-{position}.png")
        plt.close()

