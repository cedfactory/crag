from . import utils
import pandas as pd
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from rich import inspect,print

from . import utils,chronos

default_symbols = []
default_features = ["open", "close", "high", "low", "volume"]

class DataDescription():
    def __init__(self):
        self.symbols = default_symbols
        self.features = default_features
        self.interval = "1d"

class IRealTimeDataProvider(metaclass = ABCMeta):
    def __init__(self, params = None):
        pass

    @abstractmethod
    def tick(self):
        pass

    @abstractmethod
    def get_current_data(self, data_description):
        return None

    @abstractmethod
    def get_value(self, symbol):
        pass

    @abstractmethod
    def get_current_datetime(self, format = None):
        pass

    @abstractmethod
    def check_data_description(self, data_description):
        pass

class RealTimeDataProvider(IRealTimeDataProvider):
    def __init__(self, params = None):
        self.start = None
        self.end = None
        self.intervals = None

        if params:
            self.start = params.get("start_date", self.start)
            self.end = params.get("end_date", self.end)
            self.intervals = params.get("intervals", self.intervals)

        if self.start != None and self.end != None:
            self.scheduler = chronos.Chronos(self.start, self.end, self.intervals)
        else:
            self.scheduler = chronos.Chronos()
        pass

    def tick(self):
        if self.start != None and self.end != None:
            self.scheduler.increment_time()
            pass
        else:
            pass

    def get_current_data(self, data_description):

        symbols = ','.join(data_description.symbols)
        symbols = symbols.replace('/','_')

        # adapt the interval according to the frequency given to crag (todo : the interval should be provided by the strategy itself)
        interval = "1d"
        if data_description.interval <= 60 * 60:
            interval = "1h"
        if data_description.interval <= 60:
            interval = "1m"

        # MODIF CEDE: TEST
        sim_current_date = self.get_current_datetime()
        sim_current_date = sim_current_date.replace(second=0, microsecond=0)
        sim_current_date = None
        # params = { "service":"history_last", "exchange":"binance", "symbol":symbols, "interval": interval, "start": str(sim_current_date),"indicators": data_description.fdp_features}
        params = {"service": "history_last", "exchange": "bitget", "symbol": symbols, "interval": interval, "start": str(sim_current_date), "indicators": data_description.fdp_features}
        # params = {"service": "history_last", "exchange": "binance", "symbol": symbols, "interval": interval, "indicators": data_description.fdp_features}
        # https://fdp-ifxcxetwza-od.a.run.app/history?exchange=bitget&symbol=BTC&start=2023-01-01&indicators=close
        #
        response_json = utils.fdp_request_post("history_last", params)

        data = {feature: [] for feature in data_description.features}
        data["symbol"] = []
        
        if response_json["status"] == "ok":
            for symbol in data_description.symbols:
                formatted_symbol = symbol.replace('/','_')
                if response_json["result"][formatted_symbol]["status"] == "ko":
                    print("[RealTimeDataProvider:get_current_data] !!!! no data for ", symbol)
                    continue
                df = pd.read_json(response_json["result"][formatted_symbol]["info"])
                columns = list(df.columns)
                data["symbol"].append(symbol)
                for feature in data_description.features:
                    if feature not in columns:
                        return None
                    data[feature].append(df[feature].iloc[-1])

        df_result = pd.DataFrame(data)
        df_result.set_index("symbol", inplace=True)
        return df_result

    def get_value(self, symbol):
        if self.start != None and self.end != None:
            ds = DataDescription()
            ds.symbols = [
                symbol,
            ]
            ds.features = {"close": None,
                           }
            if self.intervals == "1d":
                ds.interval = 60 * 60 * 24
            elif self.intervals == "1h":
                ds.interval = 60 * 60
            elif self.intervals == "1m":
                ds.interval = 60
            df_price = self.get_current_data(ds)
            return df_price.loc[df_price.index == symbol, 'close'].iloc[0]
        else:
            return None

    def get_current_datetime(self, format = None):
        current_datetime = self.scheduler.get_current_time()
        if isinstance(current_datetime, datetime) and format != None:
            current_datetime = current_datetime.strftime(format)
        return current_datetime

    def check_data_description(self, data_description):
        pass

    def get_final_datetime(self):
        if self.start != None and self.end != None:
            return datetime.strptime(self.end, "%Y-%m-%d")
        else:
            return None

