# from . import utils
import pandas as pd
import io
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from rich import inspect, print

from . import utils, chronos

from concurrent.futures import ThreadPoolExecutor, as_completed

default_symbols = []
default_features = ["open", "close", "high", "low", "volume"]

class DataDescription():
    def __init__(self):
        self.strategy_id = ""
        self.strategy_name = ""
        self.symbols = default_symbols
        self.fdp_features = default_features
        self.interval = "1d"
        self.candle_stick = "released"

class IRealTimeDataProvider(metaclass = ABCMeta):
    def __init__(self, params = None):
        pass

    @abstractmethod
    def tick(self):
        pass

    @abstractmethod
    def get_lst_current_data(self, data_description, fdp_url_id):
        return None

    @abstractmethod
    def get_fdp_ws_status(self, fdp_url_id):
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

        if params:
            self.start = params.get("start_date", self.start)
            self.end = params.get("end_date", self.end)

        self.scheduler = chronos.Chronos()
        pass

    def tick(self):
        if self.start != None and self.end != None:
            self.scheduler.increment_time()
            pass
        else:
            pass

    def get_fdp_ws_status(self, fdp_url_id):
        params = {
            "service": "ws_status"
        }
        response_json = utils.fdp_request_post("status", params, fdp_url_id)

        if response_json["status"] == "ko":
            return None
        return response_json["result"]

    def get_lst_current_data(self, lst_data_description, fdp_url_id):
        lst_ds_result = []

        def process_data_description(data_description):
            symbols = ','.join(data_description.symbols)
            symbols = symbols.replace('/', '_')

            interval = data_description.str_interval

            params = {
                "service": "last",
                "exchange": "bitget",
                "symbol": symbols,
                "interval": interval,
                "candle_stick": data_description.candle_stick,
                "start": None,
                "indicators": data_description.fdp_features
            }

            response_json = utils.fdp_request_post("last", params, fdp_url_id)

            data = {feature: [] for feature in data_description.features}
            data["symbol"] = []

            if response_json["status"] == "ok":
                for symbol in data_description.symbols:
                    formatted_symbol = symbol.replace('/', '_')
                    if response_json["result"][formatted_symbol]["status"] == "ko":
                        print("[RealTimeDataProvider:get_current_data] !!!! no data for ", symbol)
                        continue
                    # df = pd.read_json(response_json["result"][formatted_symbol]["info"])
                    json_str = response_json["result"][formatted_symbol]["info"]
                    df = pd.read_json(io.StringIO(json_str))
                    columns = list(df.columns)
                    data["symbol"].append(symbol)
                    for feature in data_description.features:
                        if feature not in columns:
                            print("FDP MISSING FEATURE COLUMN")
                            return None
                        if len(df[feature]) > 0:
                            data[feature].append(df[feature].iloc[-1])
                        else:
                            print("FDP MISSING FEATURE VALUE")
                            return None

                df_result = pd.DataFrame(data)
                df_result.set_index("symbol", inplace=True)
                data_description.current_data = df_result.copy()
                return data_description
            else:
                print("Response status not OK")
                return None

        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(process_data_description, dd): dd for dd in lst_data_description}
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    lst_ds_result.append(result)
                else:
                    print(f"Failed to process data description: {futures[future]}")

        return lst_ds_result

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

    def get_value(self):
        exit(1234)