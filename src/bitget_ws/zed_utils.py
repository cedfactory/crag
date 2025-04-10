import os, platform, psutil, subprocess
from src.toolbox import settings_helper, os_helper
from src import crag_helper


def zed_start(configuration_file):
    configuration = crag_helper.load_configuration_file(configuration_file)
    if not configuration:
        print("💥 A problem occurred while loading {}".format(configuration_file))
        return

    account_id = configuration["broker"].get("account", "")
    account = settings_helper.get_account_info(account_id)
    api_key = account.get("api_key", "")
    api_secret = account.get("api_secret", "")
    api_password = account.get("api_password", "")

    params_strategy = configuration["strategy"]
    my_strategy = crag_helper.get_strategy(params_strategy)
    if not my_strategy:
        return None

    lst_data_description = crag_helper.get_strategy_lst_data_description(my_strategy)

    g_os_platform = platform.system()
    log_file = "zed.log"
    if g_os_platform == "Windows":
        command = "start /B python -u zed.py server {} > {}".format(configuration_file, log_file)
        print("command : ", command)
        os.system(command)
    elif g_os_platform == "Linux":
        # command = "nohup python zed.py > {} &".format(log_file)
        command = "nohup python zed.py server {} > {} &".format(configuration_file, log_file)
        print("command : ", command)
        subprocess.Popen(command, shell=True)


def zed_stop():
    all_processes = os_helper.get_python_processes()
    processes = [process for process in all_processes if 'zed.py' in process['command']]

    for process in processes:
        print(process['pid'])
        print(process)
        try:
            p = psutil.Process(process["pid"])
            p.terminate()
            p.wait()
            print("zed's dead'")
        except Exception as e:
            pass
