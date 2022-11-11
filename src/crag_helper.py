from src import rtdp,rtdp_simulation
from src import broker_simulation,broker_ftx
from src import crag
from src import rtstr,rtstr_grid_trading, rtstr_super_reversal,rtstr_trix,rtstr_cryptobot,rtstr_bigwill,rtstr_VMC
from src import logger
import xml.etree.cElementTree as ET
from dotenv import load_dotenv
import os
import pickle

def _initialize_crag_discord_bot():
    load_dotenv()
    token = os.getenv("CRAG_DISCORD_BOT_TOKEN")
    channel_id = os.getenv("CRAG_DISCORD_BOT_CHANNEL")
    webhook = os.getenv("CRAG_DISCORD_BOT_WEBHOOK")
    return logger.LoggerDiscordBot(params={"token":token, "channel_id":channel_id, "webhook":webhook})

def initialization_from_configuration_file(configuration_file):
    tree = ET.parse(configuration_file)
    root = tree.getroot()
    if root.tag != "configuration":
        print("!!! tag {} encountered. expecting configuration".format(root.tag))
        return

    strategy_node = root.find("strategy")
    strategy_id = strategy_node.get("id", "")
    strategy_name = strategy_node.get("name", None)
    params_node = list(strategy_node.iter('params'))
    params_strategy = {}
    params_strategy["id"] = strategy_id
    if len(params_node) == 1:
        for name, value in params_node[0].attrib.items():
            params_strategy[name] = value

    broker_node = root.find("broker")
    broker_name = broker_node.get("name", None)
    params_node = list(broker_node.iter('params'))
    params_broker = {}
    if len(params_node) == 1:
        for name, value in params_node[0].attrib.items():
            params_broker[name] = value

    crag_node = root.find("crag")
    crag_interval = crag_node.get("interval", 10)
    crag_interval = int(crag_interval)
    crag_id = crag_node.get("id", "")

    crag_discord_bot = _initialize_crag_discord_bot()
    params_strategy["logger"] = crag_discord_bot
    available_strategies = rtstr.RealTimeStrategy.get_strategies_list()
    if strategy_name in available_strategies:
        my_strategy = rtstr.RealTimeStrategy.get_strategy_from_name(strategy_name, params_strategy)
    else:
        print("💥 unknown strategy ({})".format(strategy_name))
        print("available strategies : ", available_strategies)
        return None

    my_broker = None
    if broker_name == "ftx":
        my_broker = broker_ftx.BrokerFTX(params_broker)
    elif broker_name == "simulator":
        my_broker = broker_simulation.SimBroker(params_broker)

    params = {'broker':my_broker, 'rtstr':my_strategy, "id": crag_id, 'interval':crag_interval, 'logger':crag_discord_bot}
    bot = crag.Crag(params)
    return bot

def initialization_from_pickle(picklefilename):
    with open(picklefilename, 'rb') as file:
        bot = pickle.load(file)
    return bot

