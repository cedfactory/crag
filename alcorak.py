import subprocess
import os, sys, psutil
from src import crag_helper, broker_bitget_api
from src.toolbox import settings_helper, os_helper
from rich import print
import platform

g_os_platform = platform.system()
g_python_executable = ""

def reset_account(configuration, start=True):
    params_alcorak = configuration["alcorak"]
    if start:
        reset_account_parameter = "reset_account_start"
        reset_account_ignore_parameter = "reset_account_start_ignore"
    else:
        reset_account_parameter = "reset_account_stop"
        reset_account_ignore_parameter = "reset_account_stop_ignore"

    reset_account = params_alcorak.get(reset_account_parameter, False)
    if isinstance(reset_account, str):
        reset_account = (reset_account.lower() == "true")  # convert string to boolean
    if reset_account:
        # get the broker
        my_broker = broker_bitget_api.BrokerBitGetApi(configuration["broker"])

        # manage ignore option
        reset_account_ignore = params_alcorak.get(reset_account_ignore_parameter, "")
        lst_reset_account_ignore = reset_account_ignore.split(",")
        open_orders = "open_orders" not in lst_reset_account_ignore
        triggers = "triggers" not in lst_reset_account_ignore
        positions = "positions" not in lst_reset_account_ignore

        # reset account
        my_broker.execute_reset_account(open_orders=open_orders, triggers=triggers, positions=positions)

def start_zed(configuration):
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

    log_file = "zed.log"
    if g_os_platform == "Windows":
        command = "start /B python -u zed.py > {}".format(log_file)
        print("command : ", command)
        os.system(command)
    elif g_os_platform == "Linux":
        # command = "nohup python zed.py > {} &".format(log_file)
        command = "nohup python zed.py > {} &".format(log_file)
        print("command : ", command)
        subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)


def stop_zed():
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

def start_strategy(strategy_configuration_file):

    print("Launching {}...".format(strategy_configuration_file))
    configuration = crag_helper.load_configuration_file(strategy_configuration_file)
    if not configuration:
        print("💥 A problem occurred while loading {}".format(strategy_configuration_file))
        return

    if "crag" not in configuration and "id" not in configuration["crag"]:
        print("Can't find id for crag")
        return

    # inject safety_step_iterations_max
    safety_step_iterations_max = 3000
    configuration["crag"]["safety_step_iterations_max"] = safety_step_iterations_max

    reset_account(configuration, True)

    command = []
    if g_os_platform == "Windows":
        command = ['cmd.exe', '/c', g_python_executable, "main.py", "--live", strategy_configuration_file]
    elif g_os_platform == "Linux":
        command = [g_python_executable, "main.py", "--live", strategy_configuration_file]

    result = subprocess.run(command, stdout=subprocess.PIPE)
    print(result)

    backup_file = "./output/" + configuration['crag']['id'] + "_crag_backup.pickle"
    while result.returncode == 1 and os.path.exists(backup_file):
        print("size of {} : {} bytes".format(backup_file, os.path.getsize(backup_file)))
        result = subprocess.run(
            [g_python_executable, "main.py", "--reboot", backup_file],
            stdout=subprocess.PIPE)
        print(result)

        # filtrer les logs de ced

    print("[alcorak] result.returncode : ", result.returncode)
    print("[alcorak] backup_file : ", backup_file)
    print("[alcorak] backup exists : ", os.path.exists(backup_file))

    reset_account(configuration, False)


_usage_str = '''
Options:
    --start <StrategyConfigFile>
'''

def _usage():
    print(_usage_str)

if __name__ == '__main__':
    print('''
       ______   __                                          __       
      /      \ |  \                                        |  \      
     |  $$$$$$\| $$  _______   ______    ______    ______  | $$   __ 
     | $$__| $$| $$ /       \ /      \  /      \  |      \ | $$  /  \\
     | $$    $$| $$|  $$$$$$$|  $$$$$$\|  $$$$$$\  \$$$$$$\| $$_/  $$
     | $$$$$$$$| $$| $$      | $$  | $$| $$   \$$ /      $$| $$   $$ 
     | $$  | $$| $$| $$_____ | $$__/ $$| $$      |  $$$$$$$| $$$$$$\ 
     | $$  | $$| $$ \$$     \ \$$    $$| $$       \$$    $$| $$  \$$\\
      \$$   \$$ \$$  \$$$$$$$  \$$$$$$  \$$        \$$$$$$$ \$$   \$$
     ''')
    print("Platform :", g_os_platform)
    g_python_executable = sys.executable
    print("Python executable :", g_python_executable)

    if len(sys.argv) > 1 and (sys.argv[1] == "--start"):
        strategy_configuration_file = sys.argv[2]
        start_strategy(strategy_configuration_file)
        print("launching {}...".format(strategy_configuration_file))

    else:
        _usage()
