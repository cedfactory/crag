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

        # filtrer le slogs de ced


    params_broker = configuration["broker"]
    my_broker = broker_bitget_api.BrokerBitGetApi(params_broker)
    my_broker.execute_reset_account()
    my_broker.cancel_all_orders(["XRP"])


def stop_strategy(strategy_config_file):
    print("Stopping {}... TODO !!!".format(strategy_config_file))


_usage_str = '''
Options:
    --start <StrategyConfigFile>
    --stop <StrategyConfigFile>
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
    elif len(sys.argv) > 1 and (sys.argv[1] == "--stop"):
        strategy_configuration_file = sys.argv[2]
        stop_strategy(strategy_configuration_file)


    else:
        _usage()
