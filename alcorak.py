import subprocess
import os
import sys
from src import crag_helper,broker_bitget_api
from rich import print
import platform

g_os_platform = platform.system()
g_python_executable = ""

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

    params_broker = configuration["broker"]
    reset_account_stop = params_broker.get("reset_account_stop", False)
    if isinstance(reset_account_stop, str):
        reset_account_stop = reset_account_stop == "True"  # convert string to boolean
    if reset_account_stop:
        # don't call reset_account when instantiating the broker
        params_broker["reset_account"] = False
        params_broker["reset_account_start"] = False

        # get the broker
        my_broker = broker_bitget_api.BrokerBitGetApi(params_broker)

        # manage ignore option
        reset_account_stop_ignore = params_broker.get("reset_account_stop_ignore", "")
        reset_account_stop_ignore = reset_account_stop_ignore.split(",")
        open_orders = "open_orders" in reset_account_stop_ignore
        triggers = "triggers" in reset_account_stop_ignore
        positions = "positions" in reset_account_stop_ignore

        # reset account
        my_broker.execute_reset_account(open_orders=open_orders, triggers=triggers, positions=positions)

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
