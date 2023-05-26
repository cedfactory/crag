import pandas as pd
import os, sys
import time
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from src import accounts,broker_bitget_api
from src.toolbox import pdf_helper, mail_helper, ftp_helper

g_use_ftp = True
def export_graph(title, df, filename):
    dates = [datetime.fromtimestamp(ts) for ts in df["timestamp"]]
    datenums = mdates.date2num(dates)
    y = df["usdt_equity"]

    fig = plt.subplots()

    ax = plt.gca()
    xfmt = mdates.DateFormatter('%d-%m-%Y')
    ax.xaxis.set_major_formatter(xfmt)

    margin = 100
    y_min = min(y) - margin
    if y_min < 0:
        y_min = 0
    y_max = max(y) + margin
    ax.set_ylim([y_min, y_max])

    ax.plot(datenums, y)

    plt.title(title)
    plt.ylabel("USDT equity")
    plt.fill_between(datenums, y, alpha=0.3)
    plt.xticks(rotation=25)
    plt.subplots_adjust(bottom=0.2)

    plt.savefig(filename)
    #plt.show()

def export_all():
    rootpath = "./conf/"
    accounts_info = accounts.import_accounts()
    for key, value in accounts_info.items():
        account_id = value.get("id", "")
        filename = rootpath + "history_" + account_id + ".csv"
        df = pd.read_csv(filename, delimiter=',')
        pngfilename = rootpath + "history_" + account_id + ".png"
        export_graph(account_id, df, pngfilename)

        pdffilename = rootpath + "history_" + account_id + ".pdf"
        pdf_helper.export_report(pdffilename, df, pngfilename)

def update_history():
    print("updating history...")
    rootpath = "./conf/"
    accounts_info = accounts.import_accounts()
    ct = datetime.now()
    ts = ct.timestamp()

    for key, value in accounts_info.items():
        my_broker = None
        broker_name = value.get("broker", "")
        account_id = value.get("id", "")
        if broker_name == "bitget":
            my_broker = broker_bitget_api.BrokerBitGetApi({"account": account_id, "reset_account": False})

        if my_broker:
            usdt_equity = my_broker.get_usdt_equity()

            local_filename = rootpath + "history_" + account_id + ".csv"
            if g_use_ftp:
                remote_path = "./customers/history/"
                remote_filename = "history_" + account_id + ".csv"
                ftp_helper.pull_file("default", remote_path, remote_filename, local_filename)

            if not os.path.isfile(local_filename):
                data = [[ts, usdt_equity]]
                df = pd.DataFrame(data, columns=["timestamp", "usdt_equity"])
                df.reset_index()
                df.set_index("timestamp", inplace=True)
                df.to_csv(local_filename)
            else:
                df = pd.read_csv(local_filename, delimiter=',')
                new_row = {"timestamp": ts, "usdt_equity": usdt_equity}
                df = df.append(new_row, ignore_index=True)
                df.reset_index()
                df.set_index("timestamp", inplace=True)
                df.to_csv(local_filename)

            if g_use_ftp:
                remote_path = "./customers/history/"
                remote_filename = "history_" + account_id + ".csv"
                ftp_helper.push_file("default", local_filename, remote_path+remote_filename)

def loop(freq):
    while True:
        print("UPDATE")
        start = datetime.now()
        end = start + timedelta(seconds=freq)

        update_history()

        current_end = datetime.now()
        sleeping_time = datetime.timestamp(end) - datetime.timestamp(current_end)
        if sleeping_time > 0:
            time.sleep(sleeping_time)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        if sys.argv[1] == "--export":
            export_all()
        elif sys.argv[1] == "--mail":
            mail_helper.send_mail("receiver@foobar.com", "Subject", "message")
        else:
            freq = int(sys.argv[1])
            loop(freq)
    else:
        update_history()


