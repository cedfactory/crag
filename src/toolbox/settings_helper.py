import os
import xml.etree.cElementTree as ET

def _get_settings_file():
    config_path = './conf'
    return os.path.join(config_path, "settings.xml")

def get_monitor_info(id):
    settings_file = _get_settings_file()
    if not os.path.isfile(settings_file):
        print("!!! {} not found".format(settings_file))
        return {}
    tree = ET.parse(settings_file)
    root = tree.getroot()
    if root.tag != "settings":
        print("!!! tag {} encountered. \"settings\" expected".format(root.tag))
        return {}

    info = {}
    monitoring_node = root.find("monitoring")
    monitors = list(monitoring_node)
    for monitor_node in monitors:
        if monitor_node.tag != "monitor" or "id" not in monitor_node.attrib or monitor_node.attrib["id"] != id:
            continue

        for name, value in monitor_node.attrib.items():
            info[name] = value

    return info

