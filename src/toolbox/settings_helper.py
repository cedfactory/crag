import os
import xml.etree.cElementTree as ET

def _get_settings_file(settings_path='./conf'):
    return os.path.join(settings_path, "settings.xml")

def _get_settings_node(nodename, settings_path='./conf'):
    settings_file = _get_settings_file(settings_path)
    if not os.path.isfile(settings_file):
        print("!!! {} not found".format(settings_file))
        return {}
    tree = ET.parse(settings_file)
    root = tree.getroot()
    if root.tag != "settings":
        print("!!! tag {} encountered. \"settings\" expected".format(root.tag))
        return {}

    info = {}
    node = root.find(nodename)
    return node

def get_monitor_info(id, settings_path='./conf'):
    info = {}
    monitoring_node = _get_settings_node("monitoring", settings_path)
    monitors = list(monitoring_node)
    for monitor_node in monitors:
        if monitor_node.tag != "monitor" or "id" not in monitor_node.attrib or monitor_node.attrib["id"] != id:
            continue
        for name, value in monitor_node.attrib.items():
            info[name] = value

    return info

def get_mailbot_info(mailbotId, settings_path='./conf'):
    info = {}
    mailbots_node = _get_settings_node("mailbots", settings_path)
    mailbots = list(mailbots_node)
    for mailbot in mailbots:
        if mailbot.tag != "mailbot" or "id" not in mailbot.attrib or mailbot.attrib["id"] != mailbotId:
            continue

        for name, value in mailbot.attrib.items():
            info[name] = value

    return info
