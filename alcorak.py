import os, sys, subprocess, platform
sys.path.append(os.path.abspath("src"))

from rich import print
from src import crag_helper, broker_bitget_api
from src.bitget_ws import zed_utils

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


def start_strategy(strategy_configuration_file):

    print("Launching {}...".format(strategy_configuration_file))
    configuration = crag_helper.load_configuration_file(strategy_configuration_file)
    if not configuration:
        print("💥 A problem occurred while loading {}".format(strategy_configuration_file))
        return

    if "crag" not in configuration and "id" not in configuration["crag"]:
        print("Can't find id for crag")
        return

    zed_utils.zed_start(strategy_configuration_file)

    params_strategy = configuration["strategy"]
    my_strategy = crag_helper.get_strategy(params_strategy)
    if not my_strategy:
        print("Can't find strategy with the following parameters :", params_strategy)
        return

    if my_strategy.get_strategy_type() == "CONTINUE":
        # inject safety_step_iterations_max
        safety_step_iterations_max = 3000
        configuration["crag"]["safety_step_iterations_max"] = safety_step_iterations_max

    reset_account(configuration, True)

    command = []
    if g_os_platform == "Windows":
        command = ['cmd.exe', '/c', g_python_executable, "main.py", "--live", strategy_configuration_file, ">", "crag.log"]
    elif g_os_platform == "Linux":
        #command = [g_python_executable, "main.py", "--live", strategy_configuration_file, ">", "crag.log"]
        command = "{} main.py --live {} > crag.log".format(g_python_executable, strategy_configuration_file)

    print("command : ", command)
    result = subprocess.run(command, shell=True)
    print(result)

    backup_file = "./output/" + configuration['crag']['id'] + "_crag_backup.pickle"
    while result.returncode == 1 and os.path.exists(backup_file):
        zed_utils.zed_stop()
        zed_utils.zed_start(strategy_configuration_file)

        print("size of {} : {} bytes".format(backup_file, os.path.getsize(backup_file)))

        if g_os_platform == "Windows":
            command = ['cmd.exe', '/c', g_python_executable, "main.py", "--reboot", backup_file, ">", "crag.log"]
        elif g_os_platform == "Linux":
            #command = [g_python_executable, "main.py", "--reboot", backup_file, ">", "crag.log"]
            command = "{} main.py --reboot {} > crag.log".format(g_python_executable, backup_file)

        result = subprocess.run(command, shell=True)
        print(result)

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
    print("/// Alcorak ///")
    print("Platform :", g_os_platform)
    g_python_executable = sys.executable
    print("Python executable :", g_python_executable)

    if len(sys.argv) > 1 and (sys.argv[1] == "--start"):
        strategy_configuration_file = sys.argv[2]
        start_strategy(strategy_configuration_file)
        print("launching {}...".format(strategy_configuration_file))

    else:
        _usage()
