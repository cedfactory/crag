import xml.etree.cElementTree as ET
import os

def import_accounts(filename="accounts.xml"):
    accounts_path = "./conf"
    accounts_filename = os.path.join(accounts_path, filename)
    if not os.path.isfile(accounts_filename):
        print("!!! {} not found".format(accounts_filename))
        return {}

    tree = ET.parse(accounts_filename)
    root = tree.getroot()
    if root.tag != "accounts":
        print("!!! tag {} encountered. expecting accounts".format(root.tag))
        return {}

    accounts = {}
    accounts_nodes = list(root)
    for account_node in accounts_nodes:
        if account_node.tag != "account":
            continue

        account = {}
        for name, value in account_node.attrib.items():
            account[name] = value
        if "id" in account:
            accounts[account["id"]] = account

    return accounts

def get_account_info(accountId, filename="accounts.xml"):
    accounts = import_accounts(filename)
    if accountId in accounts:
        return accounts[accountId]
    return {}
