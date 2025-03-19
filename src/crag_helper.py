import pandas as pd

from src import broker_simulation
from src import rtstr,strategies
from src import broker_bitget_api
from src import logger
from src import utils

import xml.etree.cElementTree as ET
import os
import glob
import shutil
import pickle
import dill


def get_configuration_files_list(config_files_df_lst):
    config_path = './conf'
    configuration_df_file = os.path.join(config_path, config_files_df_lst)
    df_config_files = pd.read_csv(configuration_df_file)
    lst_config_files = df_config_files['configuration_files'].tolist()
    lst_files = []
    for conf_file in lst_config_files:
        path_conf_file = os.path.join(config_path, conf_file)
        if os.path.exists(path_conf_file):
            lst_files.append(conf_file)
        else:
            print(conf_file, ' not in conf directory')
    return lst_files

def benchmark_results(configuration_file):
    config_path = './conf'
    output_path_crag = './output'
    output_path_benchmark = './validation/benchmark'
    configuration_file = os.path.join(config_path, configuration_file)
    tree = ET.parse(configuration_file)
    root = tree.getroot()
    if root.tag != "configuration":
        print("!!! tag {} encountered. expecting configuration".format(root.tag))
        return

    if not os.path.exists(output_path_crag):
        return

    strategy_node = root.find("strategy")
    strategy_name = strategy_node.get("name", None)

    output_path_strategy = os.path.join(output_path_benchmark, strategy_name)
    os.makedirs(output_path_strategy, exist_ok=True)

    broker_node = root.find("broker")
    broker_name = broker_node.get("name", None)
    params_node = list(broker_node.iter('params'))
    params_broker = {"name": broker_name}
    if len(params_node) == 1:
        for name, value in params_node[0].attrib.items():
            params_broker[name] = value
    output_dir = os.path.join(output_path_strategy, params_broker['start_date'] + '_' + params_broker['end_date'] + '_' + params_broker['intervals'])
    os.makedirs(output_dir, exist_ok=True)

    source_dir = output_path_crag
    dest_dir = output_dir
    csv_files = glob.glob(source_dir + './*.csv')
    for file in csv_files:
        shutil.move(file, dest_dir)
    png_files = glob.glob(source_dir + './*.png')
    for file in png_files:
        shutil.move(file, dest_dir)

def load_configuration_file(configuration_file, config_path = './conf'):
    configuration_file = os.path.join(config_path, configuration_file)
    if not os.path.isfile(configuration_file):
        print("!!! {} not found".format(configuration_file))
        return {}
    tree = ET.parse(configuration_file)
    root = tree.getroot()
    if root.tag != "configuration":
        print("!!! tag {} encountered. expecting configuration".format(root.tag))
        return {}

    strategy_node = root.find("strategy")
    strategy_id = strategy_node.get("id", "")
    strategy_name = strategy_node.get("name", None)
    params_node = list(strategy_node.iter('params'))
    params_strategy = {"id": strategy_id, "name": strategy_name}
    if len(params_node) == 1:
        for name, value in params_node[0].attrib.items():
            params_strategy[name] = value

    broker_node = root.find("broker")
    broker_name = broker_node.get("name", None)
    params_node = list(broker_node.iter('params'))
    params_broker = {"name": broker_name}
    if len(params_node) == 1:
        for name, value in params_node[0].attrib.items():
            params_broker[name] = value

    fdp_node = broker_node.find("fdp")
    fdp_source_nodes = fdp_node.findall(".//source")
    fdp_source_list = [
        {attr: source.get(attr) for attr in source.attrib}
        for source in fdp_source_nodes
    ]
    params_broker["fdp"] = fdp_source_list

    crag_node = root.find("crag")
    params_crag = {
        "id": crag_node.get("id", ""),
        "interval": int(crag_node.get("interval", 10)),
        "bot_id": crag_node.get("botId", ""),
        "loggers": crag_node.get("loggers", "")
    }

    params_alcorak = {}
    alcorak_node = root.find("alcorak")
    if alcorak_node:
        params_alcorak = {
            "reset_account_start": alcorak_node.get("reset_account_start", ""),
            "reset_account_start_ignore": alcorak_node.get("reset_account_start_ignore", ""),
            "reset_account_stop": alcorak_node.get("reset_account_stop", ""),
            "reset_account_stop_ignore": alcorak_node.get("reset_account_stop_ignore", "")
        }

    return {"broker": params_broker,
            "strategy": params_strategy,
            "crag": params_crag,
            "alcorak": params_alcorak,
            }

def get_crag_params_from_configuration(configuration):
    params_crag = configuration["crag"]
    crag_id = params_crag.get("id", "")
    crag_interval = params_crag.get("interval", "")
    str_loggers = params_crag.get("loggers", "")
    loggers = logger.get_loggers(str_loggers)

    params_strategy = configuration["strategy"]
    strategy_name = params_strategy.get("name", "")
    available_strategies = rtstr.RealTimeStrategy.get_strategies_list()
    lst_data_description = []
    if strategy_name in available_strategies:
        my_strategy = rtstr.RealTimeStrategy.get_strategy_from_name(strategy_name, params_strategy)
        if my_strategy.get_strategy_type() == "INTERVAL":
            lst_data_description = my_strategy.get_data_description(["1m", "5m", "15m", "30m", "1h"])
            lst_data_description = utils.reduce_data_description(lst_data_description)
    else:
        print("💥 unknown strategy ({})".format(strategy_name))
        print("available strategies : ", available_strategies)
        return None

    my_broker = None
    params_broker = configuration["broker"]
    params_broker["data_description"] = lst_data_description
    broker_name = params_broker.get("name", "")
    if broker_name == "simulator" or broker_name == "simulation" or broker_name == "simu":
        my_broker = broker_simulation.SimBroker(params_broker)
    elif broker_name == "mock":
        my_broker = None
    else:
        my_broker = broker_bitget_api.BrokerBitGetApi(params_broker)
    if broker_name != "mock" and (not my_broker or not my_broker.ready()):
        return None

    bot_id = params_crag.get("bot_id", None)
    crag_discord_bot = logger._initialize_crag_discord_bot(bot_id)

    return {"broker": my_broker, "rtstr": my_strategy, "id": crag_id, "interval": crag_interval, "logger": crag_discord_bot, "loggers": loggers}

def initialization_from_pickle(picklefilename):
    bot = None
    if os.path.exists(picklefilename):
        with open(picklefilename, 'rb') as file:
            try:
                bot = pickle.load(file)
                # bot = dill.load(file)
            except Exception as exception:
                print(exception)
    return bot

