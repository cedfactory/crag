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

def get_discord_bot_info(botId, settings_path="./conf"):
    info = {}
    discordbots_node = _get_settings_node("discordbots", settings_path)
    discordbots = list(discordbots_node)
    for discordbot in discordbots:
        if discordbot.tag != "discordbot" or "id" not in discordbot.attrib or discordbot.attrib["id"] != botId:
            continue

        for name, value in discordbot.attrib.items():
            info[name] = value
        break

    return info

def get_telegram_bot_info(botId, settings_path="./conf"):
    info = {}
    telegrambots_node = _get_settings_node("telegrambots", settings_path)
    telegrambots = list(telegrambots_node)
    for telegrambot in telegrambots:
        if telegrambot.tag != "telegrambot" or "id" not in telegrambot.attrib or telegrambot.attrib["id"] != botId:
            continue

        for name, value in telegrambot.attrib.items():
            info[name] = value
        break

    return info

def get_fdp_url_info(fdpId, settings_path="./conf"):
    info = {}
    fdps_node = _get_settings_node("fdp_urls", settings_path)
    fdps = list(fdps_node)
    for fdp in fdps:
        if fdp.tag != "fdp_url" or "id" not in fdp.attrib or fdp.attrib["id"] != fdpId:
            continue

        for name, value in fdp.attrib.items():
            info[name] = value
        break

    return info

def get_ftp_account_info(accountId, settings_path="./conf"):
    info = {}
    ftp_accounts_node = _get_settings_node("ftp_accounts", settings_path)
    ftp_accounts = list(ftp_accounts_node)
    for ftp_account in ftp_accounts:
        if ftp_account.tag != "ftp_account" or "id" not in ftp_account.attrib or ftp_account.attrib["id"] != accountId:
            continue

        for name, value in ftp_account.attrib.items():
            info[name] = value
        break

    return info

def get_accounts_info(settings_path="./conf"):
    accounts_node = _get_settings_node("accounts", settings_path)
    accounts_node = list(accounts_node)
    accounts = {}
    for account_node in accounts_node:
        if account_node.tag != "account":
            continue

        account = {}
        for name, value in account_node.attrib.items():
            account[name] = value
        if "id" in account:
            accounts[account["id"]] = account

    return accounts

def get_account_info(accountId, settings_path="./conf"):
    info = {}
    accounts_node = _get_settings_node("accounts", settings_path)
    accounts = list(accounts_node)
    for account in accounts:
        if account.tag != "account" or "id" not in account.attrib or account.attrib["id"] != accountId:
            continue

        for name, value in account.attrib.items():
            info[name] = value
        break

    return info
