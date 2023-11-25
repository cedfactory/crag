import pandas as pd

from src import broker_simulation
from src import rtstr,rtstr_dummy_test,rtstr_envelope,rtstr_envelopestochrsi,rtstr_dummy_test_tp,rtstr_bollinger_trend,rtstr_bollinger_trend_long,rtstr_tv_recommendation_mid,rtstr_super_reversal,rtstr_volatility_test_live,rtstr_trix,rtstr_cryptobot,rtstr_sltp_only,rtstr_bigwill,rtstr_VMC
from src import broker_bitget_api
from src import logger
from src import utils
from src.toolbox import settings_helper

import xml.etree.cElementTree as ET
from dotenv import load_dotenv
import os
import glob
import shutil
import pickle

def _initialize_crag_discord_bot(botId=""):
    if botId == None or botId == "":
        return None

    if botId != None and botId != "":
        bot_info = settings_helper.get_discord_bot_info(botId)
        token = bot_info.get("token", None)
        channel_id = bot_info.get("channel", None)
        webhook = bot_info.get("webhook", None)
        return logger.LoggerDiscordBot(params={"token":token, "channel_id":channel_id, "webhook":webhook})

    load_dotenv()
    token = os.getenv("CRAG_DISCORD_BOT_TOKEN")
    channel_id = os.getenv("CRAG_DISCORD_BOT_CHANNEL")
    webhook = os.getenv("CRAG_DISCORD_BOT_WEBHOOK")
    return logger.LoggerDiscordBot(params={"token":token, "channel_id":channel_id, "webhook":webhook})

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

    crag_node = root.find("crag")
    params_crag = {
        "id": crag_node.get("id", ""),
        "interval": int(crag_node.get("interval", 10)),
        "bot_id": crag_node.get("botId", "")
    }

    return {'broker': params_broker, 'strategy': params_strategy, "crag": params_crag}

def get_crag_params_from_configuration(configuration):
    params_broker = configuration["broker"]
    params_strategy = configuration["strategy"]
    params_crag = configuration["crag"]

    crag_id = params_crag.get("id", "")
    crag_interval = params_crag.get("interval", "")
    bot_id = params_crag.get("bot_id", None)
    crag_discord_bot = _initialize_crag_discord_bot(bot_id)
    params_strategy["logger"] = crag_discord_bot
    available_strategies = rtstr.RealTimeStrategy.get_strategies_list()
    strategy_name = params_strategy.get("name", "")
    if strategy_name in available_strategies:
        my_strategy = rtstr.RealTimeStrategy.get_strategy_from_name(strategy_name, params_strategy)
    else:
        print("💥 unknown strategy ({})".format(strategy_name))
        print("available strategies : ", available_strategies)
        return None

    my_broker = None
    broker_name = params_broker.get("name", "")
    if broker_name == "simulator" or broker_name == "simulation" or broker_name == "simu":
        my_broker = broker_simulation.SimBroker(params_broker)
    else:
        my_broker = broker_bitget_api.BrokerBitGetApi(params_broker)
    if not my_broker or not my_broker.ready():
        return None

    return {"broker": my_broker, "rtstr": my_strategy, "id": crag_id, "interval": crag_interval, "logger": crag_discord_bot}

def initialization_from_pickle(picklefilename):
    with open(picklefilename, 'rb') as file:
        bot = pickle.load(file)
    return bot

