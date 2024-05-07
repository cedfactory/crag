import execute_time_recoder
dir = "./dump20240502/dump_timer"
# dir = "./dump_timer_miniPC"
execute_timer = execute_time_recoder.ExecuteTimeRecorder(-1, directory=dir)
execute_timer.plot_all_close_grid()
execute_timer.plot_all_close_timer()
execute_timer.plot_all_close_system()
