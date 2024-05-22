import execute_time_recoder
import os
import shutil

# dir = "./dump20240502/dump_timer"
dir_long = "./log_recorder/dump_timer_long"
dir_short = "./log_recorder/dump_timer_short"
dir = "./log_recorder/"
# dir = "./dump_timer_miniPC"

if False:
    # Create the target directories if they don't exist
    os.makedirs(dir_long, exist_ok=True)
    os.makedirs(dir_short, exist_ok=True)

    # Loop through all files in the source directory
    for filename in os.listdir(dir):
        # Construct full file path
        file_path = os.path.join(dir, filename)
        # Check if it's a file
        if os.path.isfile(file_path):
            # Move files containing '_long_' to dir_long
            if '_long_' in filename:
                shutil.move(file_path, dir_long)
            # Move files containing '_short_' to dir_short
            elif '_short_' in filename:
                shutil.move(file_path, dir_short)
    exit(0)

execute_timer = execute_time_recoder.ExecuteTimeRecorder(-1, directory=dir_long)
execute_timer.plot_all_close_grid()
execute_timer.plot_all_close_timer()
execute_timer.plot_all_close_system()
