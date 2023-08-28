import ftplib
from . import settings_helper

def get_ftp_session(accountId):
    account = settings_helper.get_ftp_account_info(accountId)
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
