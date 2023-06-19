import numpy as np
import pandas as pd
import os, sys
import time
from datetime import datetime, timedelta

from src import accounts,broker_bitget_api
from src.toolbox import pdf_helper, mail_helper, ftp_helper, graph_helper

g_use_ftp = True

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

        if g_use_ftp:
            remote_path = "./customers/history/"
            remote_filename = "history_" + account_id + ".csv"
            ftp_helper.pull_file("default", remote_path, remote_filename, filename)

        df = pd.read_csv(filename, delimiter=',')

        # page with a figure
        pngfilename = rootpath + "history_" + account_id + ".png"
        graph_helper.export_graph(pngfilename, account_id, df, ["usdt_equity"])

        timestamp_start = df.iloc[0]["timestamp"]
        timestamp_end = df.iloc[-1]["timestamp"]
        delta_seconds = timestamp_end - timestamp_start
        delta_days = delta_seconds / (60*60*24)
        pngfilename_btcusd = rootpath + "history_" + account_id + "_btcusd.png"
        graph_helper.export_btcusd(pngfilename_btcusd, delta_days)

        pngfilename_usdt_equity_btcusd_normalized = rootpath + "history_" + account_id + "_usdt_equity_btcusd_normalized.png"
        if df.at[0, "usdt_equity"]:
            df["usdt_equity_normalized"] = 1000 * df["usdt_equity"] / df.at[0, "usdt_equity"]
        else:
            df["usdt_equity_normalized"] = df["usdt_equity"]
        df_btcusd = graph_helper.export_btcusd(pngfilename_usdt_equity_btcusd_normalized, delta_days)
        df_btcusd["close_normalized"] = 1000 * df_btcusd["close"] / df_btcusd.iloc[0]["close"]
        df_btcusd["timestamp"] = df_btcusd.index
        graph_helper.export_graph(pngfilename_usdt_equity_btcusd_normalized, "Normalized", df, ["usdt_equity_normalized"], df_btcusd, ["close_normalized"])

        # Not used ?
        df["timestamp"] = [datetime.fromtimestamp(x).replace(minute=0, second=0, microsecond=0) for x in df["timestamp"]]
        df.drop_duplicates(subset=["timestamp"], inplace=True)

        account_export_info = {
            "account_id": account_id,
            "usdt_equity": pngfilename,
            "btcusd": pngfilename_btcusd,
            "usdt_equity_btcusd_normalized": pngfilename_usdt_equity_btcusd_normalized,
            "df": df
        }
        accounts_export_info.append(account_export_info)

        #df = df.drop(["btcusd"], axis=1)
        df.set_index("timestamp", inplace=True)
        df1 = df.resample("6H").interpolate()
        df_sum = df_sum.groupby('timestamp').sum().add(df1.groupby('timestamp').sum(), fill_value=0)

    # sum : usdt_equity
    pngfilename_sum = rootpath + "history_sum.png"
    df_sum = df_sum.iloc[1:] # temporary hack
    df_sum.reset_index(inplace=True)
    df_sum["usdt_equity"] = pd.to_numeric(df_sum["usdt_equity"])

    if g_use_ftp:
        remote_path = "./customers/history/"
        remote_filename = "transferts.csv"
        ftp_helper.pull_file("default", remote_path, remote_filename, "./conf/transferts.csv")

    df_transferts = pd.read_csv("./conf/transferts.csv", keep_default_na=False)
    df_transferts = df_transferts[(df_transferts.account_src == "") | (df_transferts.account_dst == "")]
    df_transferts.sort_values(by=["timestamp"], inplace=True)
    df_transferts["placed"] = np.where(df_transferts.account_src == "", df_transferts.amount, -df_transferts.amount)
    df_transferts["placed_cumsum"] = df_transferts["placed"].cumsum()
    df_transferts = df_transferts[["timestamp", "placed_cumsum"]]

    # add a row
    last_timestamp = datetime.timestamp(df_sum.tail(1)["timestamp"].iloc[0])
    last_placed_cumsum = df_transferts.tail(1)["placed_cumsum"].iloc[0]
    row = pd.DataFrame({"timestamp": last_timestamp, "placed_cumsum": last_placed_cumsum}, index=[1])
    df_transferts = pd.concat([df_transferts, row])

    graph_helper.export_graph(pngfilename_sum, "Global Investment", df_sum, ["usdt_equity"], df_transferts, ["placed_cumsum"])

    df_sum["usdt_equity_normalized"] = 1000 * df_sum["usdt_equity"] / df_sum.at[0, "usdt_equity"]
    pngfilename_sum_normalized = rootpath + "history_sum_normalized.png"

    # sum : btcusd
    date_start = df_sum.iloc[0]["timestamp"]
    date_end = df_sum.iloc[-1]["timestamp"]
    delta = date_end - date_start
    delta_days = delta.days
    pngfilename_sum_btcusd = rootpath + "history_sum_btcusd.png"
    df_btcusd = graph_helper.export_btcusd(pngfilename_sum_btcusd, delta_days)
    df_btcusd["close_normalized"] = 1000 * df_btcusd["close"] / df_btcusd.iloc[0]["close"]
    df_btcusd["timestamp"] = df_btcusd.index
    graph_helper.export_graph(pngfilename_sum_normalized, "Normalized Investment", df_sum, ["usdt_equity_normalized"], df_btcusd, ["close_normalized"])
    report.add_page("Sum", [pngfilename_sum, pngfilename_sum_normalized, pngfilename_sum_btcusd])

    # export each account info
    for account_export_info in accounts_export_info:
        account_id = account_export_info["account_id"]
        pngfilename_usdt_equity = account_export_info["usdt_equity"]
        pngfilename_btcusd = account_export_info["btcusd"]
        usdt_equity_btcusd_normalized = account_export_info["usdt_equity_btcusd_normalized"]
        df = account_export_info["df"]
        report.add_page(account_id, [pngfilename_usdt_equity, usdt_equity_btcusd_normalized, pngfilename_btcusd])

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


