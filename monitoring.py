import pandas as pd
import os, sys
import time
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from src import accounts,broker_bitget_api
from src.toolbox import pdf_helper, mail_helper, ftp_helper

g_use_ftp = True
def export_graph(title, df, column_name, filename):
    if title == "Sum":
        dates = df["timestamp"]
    else:
        dates = [datetime.fromtimestamp(ts) for ts in df["timestamp"]]
    datenums = mdates.date2num(dates)
    y = df[column_name]

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
    plt.ylabel(column_name)
    plt.fill_between(datenums, y, alpha=0.3)
    plt.xticks(rotation=25)
    plt.subplots_adjust(bottom=0.2)

    plt.savefig(filename)
    #plt.show()

def export_all():
    rootpath = "./conf/"
    accounts_info = accounts.import_accounts()

    report = pdf_helper.PdfDocument("report", "logo.png")

    df_sum = pd.DataFrame([], columns=["timestamp", "usdt_equity"])
    df_sum.set_index("timestamp", inplace=True)

    accounts_export_info = []

    for key, value in accounts_info.items():
        account_id = value.get("id", "")
        filename = rootpath + "history_" + account_id + ".csv"
        df = pd.read_csv(filename, delimiter=',')

        # page with a figure
        pngfilename = rootpath + "history_" + account_id + ".png"
        export_graph(account_id, df, "usdt_equity", pngfilename)

        pngfilename_btcusd = rootpath + "history_" + account_id + "_btcusd.png"
        export_graph(account_id, df.dropna(), "btcusd", pngfilename_btcusd)

        df["timestamp"] = [datetime.fromtimestamp(x).replace(minute=0, second=0, microsecond=0) for x in df["timestamp"]]
        df.drop_duplicates(subset=["timestamp"], inplace=True)
        #report.add_page(account_id, [pngfilename, pngfilename_btcusd, df])

        account_export_info = {
            "account_id": account_id,
            "usdt_equity": pngfilename,
            "df": df
        }
        accounts_export_info.append(account_export_info)

        df = df.drop(["btcusd"], axis=1)
        df.set_index("timestamp", inplace=True)
        df1 = df.resample("6H").interpolate()
        df_sum = df_sum.groupby('timestamp').sum().add(df1.groupby('timestamp').sum(), fill_value=0)

    pngfilename_sum = rootpath + "history_sum.png"
    df_sum = df_sum.iloc[1:] # temporary hack
    df_sum.reset_index(inplace=True)
    df_sum["usdt_equity"] = pd.to_numeric(df_sum["usdt_equity"])
    export_graph("Sum", df_sum, "usdt_equity", pngfilename_sum)
    report.add_page("Sum", [pngfilename_sum])

    # export each account info
    for account_export_info in accounts_export_info:
        account_id = account_export_info["account_id"]
        pngfilename_usdt_equity = account_export_info["usdt_equity"]
        df = account_export_info["df"]
        report.add_page(account_id, [pngfilename_usdt_equity, df])

    # write the pdf file
    pdffilename = rootpath + "history_report.pdf"
    report.save(pdffilename)

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
            btcusd = my_broker.get_value("BTC")

            local_filename = rootpath + "history_" + account_id + ".csv"
            if g_use_ftp:
                remote_path = "./customers/history/"
                remote_filename = "history_" + account_id + ".csv"
                ftp_helper.pull_file("default", remote_path, remote_filename, local_filename)

            if not os.path.isfile(local_filename):
                data = [[ts, usdt_equity]]
                df = pd.DataFrame(data, columns=["timestamp", "usdt_equity", "btcusd"])
                df.reset_index()
                df.set_index("timestamp", inplace=True)
                df.to_csv(local_filename)
            else:
                try:
                    df = pd.read_csv(local_filename, delimiter=',')
                    new_row = {"timestamp": ts, "usdt_equity": usdt_equity, "btcusd": btcusd}
                    df = df.append(new_row, ignore_index=True)
                except:
                    data = [[ts, usdt_equity, btcusd]]
                    df = pd.DataFrame(data, columns=["timestamp", "usdt_equity", "btcusd"])
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


