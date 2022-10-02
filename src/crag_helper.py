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
    strategy_name = strategy_node.get("name", None)

    broker_node = root.find("broker")
    broker_name = broker_node.get("name", None)
    account_name = broker_node.get("account", None)
    broker_simulation = broker_node.get("simulation", False)
    if broker_simulation == "1":
        broker_simulation = True
    elif broker_simulation == "0":
        broker_simulation = False

    crag_node = root.find("crag")
    crag_interval = crag_node.get("interval", 10)
    crag_interval = int(crag_interval)

    crag_discord_bot = _initialize_crag_discord_bot()
    params = {"logger":crag_discord_bot}
    available_strategies = rtstr.RealTimeStrategy.get_strategies_list()
    if strategy_name in available_strategies:
        my_strategy = rtstr.RealTimeStrategy.get_strategy_from_name(strategy_name, params)
    else:
        print("💥 unknown strategy ({})".format(strategy_name))
        print("available strategies : ", available_strategies)
        return None

    my_broker = None
    if broker_name == "ftx":
        my_broker = broker_ftx.BrokerFTX({'account':account_name, 'simulation':broker_simulation})

    params = {'broker':my_broker, 'rtstr':my_strategy, 'interval':crag_interval, 'logger':crag_discord_bot}
    bot = crag.Crag(params)
    return bot

def initialization_from_pickle(picklefilename):
    with open(picklefilename, 'rb') as file:
        bot = pickle.load(file)
    return bot

