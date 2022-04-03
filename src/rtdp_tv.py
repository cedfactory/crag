import pandas as pd
import json
import time
import csv
from datetime import datetime

from . import rtdp,utils

class RTDPTradingView(rtdp.RealTimeDataProvider):
    def __init__(self, params = None):
        super().__init__(params)

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
                        print("[RTDPTradingView] Column names are {}".format(", ".join(row)))
                    else:
                        data = json.loads(row[1])
                        self.data.append(data)
                    line_count += 1
                print('[RTDPTradingView] Processed {} lines'.format(line_count-1))
            self.current_position = -1

    def _fetch_data(self):
        # get the portfolio
        recommendations = ','.join(self.recommendations)
        intervals = ','.join(self.intervals)
        url = 'portfolio?screener=crypto&exchange=ftx&recommendations='+recommendations+'&intervals='+intervals
        response_json = utils.fdp_request(url)
        if response_json["status"] != "ok":
            print(response_json["reason"])
            return None
        portfolio_json = response_json["result"]["symbols"]

        # get info for symbols in the portfolio
        df_portfolio = pd.read_json(portfolio_json)
        symbols = df_portfolio["symbol"].to_list()
        str_symbols = ','.join(symbols)
        str_symbols = str_symbols.replace("/", "_")
        
        url = 'symbol?screener=crypto&exchange=ftx&symbols='+str_symbols
        response_json = utils.fdp_request(url)
        if response_json["status"] != "ok":
            print(response_json["reason"])
            return None
        symbols_json = response_json["result"]

        result = {"portfolio":portfolio_json, "symbols":symbols_json}
        return result

    def next(self):
        if self.infile != None:
            self.current_position = self.current_position + 1
            if self.current_position >= len(self.data):
                print("no more data")
                self.current_data = None
            else:
                self.current_data = self.data[self.current_position]
        else:
            self.current_data = self._fetch_data()

        return self.current_data
    
    def get_current_data(self):
        return self.current_data

    def record(self, n_records, interval, outfile="RTDPTradingView_record.csv"):
        f = open(outfile, "w")
        f.write("Date;Data\n")
        f.close()
        while n_records > 0:
            json_data = self._fetch_data()
            if json_data is None:
                continue

            now = datetime.now()
            now_string = now.strftime("%d/%m/%Y %H:%M:%S")

            f = open(outfile, "a")
            str_data = json.dumps(json_data)
            f.write("{};{}\n".format(now_string, str_data))
            f.close()

            n_records = n_records - 1
            time.sleep(interval)
