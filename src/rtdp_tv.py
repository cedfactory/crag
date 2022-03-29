import pandas as pd
import urllib
import urllib.request
import json
import os
from dotenv import load_dotenv

from . import rtdp

class RTDPTradingView(rtdp.RealTimeDataProvider):
    def __init__(self, params = None):
        super().__init__(params)

        load_dotenv()
        self.fdp_url = os.getenv("FDP_URL")

    def _fetch_data(self):
        request = urllib.request.Request(self.fdp_url+'portfolio')
        request.add_header("User-Agent", "cheese")
        response = urllib.request.urlopen(request).read()
        return response

    def next(self):
        response = self._fetch_data()
        data_json = json.loads(response)
        if data_json["status"] != "ok":
            print(data_json["reason"])
            return []

        df_portfolio = pd.read_json(data_json["result"]["symbols"])
        selection = df_portfolio['symbol'].to_list()

        return selection
