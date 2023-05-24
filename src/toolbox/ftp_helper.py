import os
import xml.etree.cElementTree as ET
import ftplib

def import_ftp_accounts(filename="accounts_ftp.xml"):
    path = "./conf"
    accounts_filename = os.path.join(path, filename)
    if not os.path.isfile(accounts_filename):
        print("!!! {} not found".format(accounts_filename))
        return {}

    tree = ET.parse(accounts_filename)
    root = tree.getroot()
    if root.tag != "accounts_ftp":
        print("!!! tag {} encountered. expecting accounts_ftp".format(root.tag))
        return {}

    accounts = {}
    accounts_nodes = list(root)
    for account_node in accounts_nodes:
        if account_node.tag != "account_ftp":
            continue

        account = {}
        for name, value in account_node.attrib.items():
            account[name] = value
        if "id" in account:
            accounts[account["id"]] = account

    return accounts

def get_ftp_info(accountId, filename="accounts_ftp.xml"):
    accounts = import_ftp_accounts(filename)
    if accountId in accounts:
        return accounts[accountId]
    return {}

def get_ftp_session(accountId):
    account = get_ftp_info(accountId)
    url = account.get("url", None)
    port = account.get("port", 21)
    if isinstance(port, str):
        port = int(port)
    user = account.get("user", None)
    password = account.get("password", None)
    if not url or not user or not password:
        return None

    session = ftplib.FTP(url)
    session.login(user, password)
    return session

def pull_file(accountId, remotepath, remotefilename, localfilename):
    ret = False
    session = get_ftp_session(accountId)
    if not session:
        return ret

    try:
        session.cwd(remotepath)
        session.retrbinary("RETR " + remotefilename, open(localfilename, 'wb').write)
        ret = True
    except:
        print("error while pull_file")

    session.quit()

    return ret

def push_file(accountId, localfilename, remotefilename, format="text"):
    ret = False
    session = get_ftp_session(accountId)
    if not session:
        return ret

    try:
        if format == "text":
            session.storlines("STOR " + remotefilename, open(localfilename, 'rb'))
        elif format == "binary":
            session.storbinary("STOR " + remotefilename, open(localfilename, 'rb'))
        ret = True
    except:
        print("error while push_file")

    session.quit()

    return ret
