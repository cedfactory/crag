import execute_time_recoder
import os
import shutil

# dir = "./dump20240502/dump_timer"
dir_long = "./log_recorder_002/dump_timer_long"
dir_short = "./log_recorder_002/dump_timer_short"
dir = "./log_recorder_002/"
# dir = "./dump_timer_miniPC"

lst_dir = [dir_long, dir_short]

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

    for directory in lst_dir:
        # Define the target directory
        target_directory = directory + './grid'

        # Create the target directory if it doesn't exist
        if not os.path.exists(target_directory):
            os.makedirs(target_directory)

        # Get the list of all files in the current directory
        files_in_current_directory = os.listdir(directory)

        # Iterate through all the files
        for file_name in files_in_current_directory:
            # Check if the file has a .csv extension
            if file_name.endswith('.csv'):
                # Construct the full file path
                full_file_name = os.path.join(directory, file_name)
                # Move the file to the target directory
                shutil.move(full_file_name, target_directory)

    exit(0)

for direrctory in lst_dir:
    execute_timer = execute_time_recoder.ExecuteTimeRecorder(-1, directory=direrctory)
    execute_timer.plot_all_close_grid()
    execute_timer.plot_all_close_timer()
    execute_timer.plot_all_close_system()
