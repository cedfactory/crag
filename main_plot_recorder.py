import execute_time_recoder

execute_timer = execute_time_recoder.ExecuteTimeRecorder(-1, directory="./dump_timer_miniPC")
execute_timer.plot_all_close_grid()
execute_timer.plot_all_close_timer()
execute_timer.plot_all_close_system()
