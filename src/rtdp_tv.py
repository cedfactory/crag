import pandas as pd
import urllib
import urllib.request
import json
import os
import time
import csv
from datetime import datetime
from dotenv import load_dotenv

from . import rtdp

class RTDPTradingView(rtdp.RealTimeDataProvider):
    def __init__(self, params = None):
        super().__init__(params)

        load_dotenv()
        self.fdp_url = os.getenv("FDP_URL")

        self.recommendations = ["STRONG_BUY", "BUY"]
        self.intervals = ["1m", "5m", "15m","30m","1h","2h","4h"]
        self.infile = None
        if params:
            self.recommendations = params.get("recommendations", self.recommendations)
            self.intervals = params.get("intervals", self.intervals)
            self.infile = params.get("infile", self.infile)

        # read input file
        if self.infile != None:
            self.data = []
            with open(self.infile) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=';')
                line_count = 0
                for row in csv_reader:
                    if line_count == 0:
                        print(f'Column names are {", ".join(row)}')
                    else:
                        self.data.append(row[1])
                    line_count += 1
                print('Processed {} lines'.format(line_count-1))
            self.current_position = -1

    def _fetch_data(self):
        recommendations = ','.join(self.recommendations)
        intervals = ','.join(self.intervals)
        url = self.fdp_url+'portfolio?screener=crypto&exchange=ftx&recommendations='+recommendations+'&intervals='+intervals
        request = urllib.request.Request(url)
        request.add_header("User-Agent", "cheese")
        response = urllib.request.urlopen(request).read()
        return response

    def next(self):
        if self.infile != None:
            self.current_position = self.current_position + 1
            if self.current_position >= len(self.data):
                print("no more data")
                self.current_data = None
            else:
                self.current_data = self.data[self.current_position]
        else:
            response = self._fetch_data()
            data_json = json.loads(response)
            if data_json["status"] != "ok":
                print(data_json["reason"])
                self.current_data = None
            else:
                self.current_data = data_json["result"]["symbols"]

        if self.current_data is not None:
            self.current_data = pd.read_json(self.current_data)
        return self.current_data
    
    def get_current_data(self):
        return self.current_data

    def record(self, n_records, outfile="RTDPTradingView_record.csv"):
        f = open(outfile, "w")
        f.write("Date;Symbols\n")
        f.close()
        while n_records > 0:
            response = self._fetch_data()
            data_json = json.loads(response)
            if data_json["status"] == "ko":
                continue

            df_portfolio = pd.read_json(data_json["result"]["symbols"])
            print(df_portfolio)
            print(df_portfolio.columns.to_list())
            selection = df_portfolio['symbol'].to_list()
            print(selection)

            now = datetime.now()
            now_string = now.strftime("%d/%m/%Y %H:%M:%S")

            f = open(outfile, "a")
            f.write("{};{}\n".format(now_string, data_json["result"]["symbols"]))
            f.close()

            n_records = n_records - 1
            time.sleep(6)
