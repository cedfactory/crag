import pandas as pd
import json
import os
import time
import csv
from datetime import datetime

# exports
import matplotlib.pyplot as plt
from reportlab.lib import utils as reportlab_utils
from reportlab.lib.pagesizes import A4,inch
from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER
from reportlab.platypus import SimpleDocTemplate
from reportlab.platypus import Paragraph,Table,TableStyle,Image,PageBreak
from reportlab.lib.styles import getSampleStyleSheet,ParagraphStyle
from reportlab.graphics.shapes import Drawing,Line
from reportlab.lib.units import cm

def get_image(path, width=70*cm):
    img = reportlab_utils.ImageReader(path)
    iw, ih = img.getSize()
    aspect = ih / float(iw)
    return Image(path, width=width, height=(width * aspect))

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

        self.symbols = []
        
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
        self.symbols = self.symbols + list(set(symbols) - set(self.symbols))
        str_symbols = ','.join(self.symbols)
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

    def export(self, filename):
        # prices evolution
        prices = {}
        for data in self.data:
            json_portfolio = data["portfolio"]
                
            # get info for symbols in the portfolio
            df_portfolio = pd.read_json(json_portfolio)
            symbols = df_portfolio["symbol"].to_list()
            print(symbols)
            for symbol in symbols:
                if symbol not in prices.keys():
                    prices[symbol] = []
                symbol_renamed = symbol.replace('/', '_')
                if symbol_renamed not in data["symbols"]:
                    print("[rtdp_tv::export] symbol {} not found".format(symbol))
                    continue
                symbol_price = float(data["symbols"][symbol_renamed]["info"]["info"]["price"])
                print("   {} {:.2f}".format(symbol, symbol_price))
                prices[symbol].append(symbol_price)

        for symbol in prices:
            print("{} : {}".format(symbol, prices[symbol]))
            fig = plt.figure(figsize=(10, 10))
            fig.add_subplot(111)
            plt.plot(range(0,len(prices[symbol])), prices[symbol])
            plt.title(symbol)
            fig.savefig(symbol.replace('/', '_')+".png")

        doc = SimpleDocTemplate(filename, pagesize=A4, rightMargin=0, leftMargin=0, topMargin=0.3 * cm, bottomMargin=0)
        styles = getSampleStyleSheet()
        
        elements = []

        # header

        style_title = ParagraphStyle(name='right', parent=styles['Heading1'], alignment=TA_CENTER)

        elements.append(Paragraph("Crag report", style_title))

        img = get_image('crag.png', width=cm)
        img.hAlign = 'CENTER'
        elements.append(img)

        d = Drawing(590, 1)
        line = Line(0, 0, 583, 0)
        line.strokeWidth = 2
        d.add(line)
        elements.append(d)


        images_to_delete = []
        for symbol in prices:
            elements.append(Paragraph(symbol, styles['Heading2']))
            pngfilename = symbol.replace('/', '_')+".png"
            images_to_delete.append(pngfilename)
            elements.append(get_image(pngfilename, width=7.*cm))
            elements.append(PageBreak()) # break

        doc.build(elements)

        for image_to_delete in images_to_delete:
            os.remove(image_to_delete)
            