from abc import ABCMeta, abstractmethod
from src.toolbox import settings_helper
from src import logger
import pandas as pd
from os import path
import ast
from datetime import datetime

def create_df_open_positions():
    df_open_positions = pd.DataFrame(columns=["symbol", "holdSide", "leverage", "marginCoin",
                                              "available", "total", "usdtEquity",
                                              "marketPrice", "averageOpenPrice",
                                              "achievedProfits", "unrealizedPL", "liquidationPrice"])
    return df_open_positions

def create_df_open_orders():
    df_open_orders = pd.DataFrame(columns=["symbol", "price", "side", "size", "leverage",
                                           "marginCoin", "clientOid", "orderId",
                                           "gridId"]) # gridId to to remove...
    return df_open_orders

def create_df_prices():
    df_prices = pd.DataFrame(columns = ["symbols", "values"])
    return df_prices

class Broker(metaclass = ABCMeta):
    
    def __init__(self, params=None):
        self.loggers = [] #[ logger.LoggerConsole() ]
        self.df_symbols = None
        self.cash = 10
        self.fdp_url_id = "localhost:5000"
        self.reset_account_start = False
        if params:
            loggers = params.get("loggers", self.loggers)
            if isinstance(loggers, str):
                self.loggers = logger.get_loggers(loggers)
            elif isinstance(loggers, list):
                self.loggers = loggers

            self.cash = params.get("cash", self.cash)
            self.fdp_url_id = params.get("fdp_url_id", self.fdp_url_id)
            self.reset_account_start = params.get("reset_account_start", self.reset_account_start)
            if isinstance(self.reset_account_start, str):
                self.reset_account_start = self.reset_account_start.lower() == "true"

            symbols = params.get("symbols", None)
            if symbols and isinstance(symbols, str) and path.exists("./symbols/"+symbols):
               self.df_symbols = pd.read_csv("./symbols/"+symbols)
            elif isinstance(symbols, dict):
                self.df_symbols = pd.DataFrame(symbols)

        self.cash_borrowed = 10
        self.rtdp = None
        self.account = None
        if params:
            account_id = params.get("account", "")
            self.account = settings_helper.get_account_info(account_id)
            if not self.account:
                self.log("⚠ account {} not found".format(account_id))

    def resume_strategy(self):
        return not self.reset_account_start

    def is_reset_account(self):
        return self.reset_account_start

    def ready(self):
        return False

    def get_leverage_long(self, symbol):
        if isinstance(self.df_symbols, pd.DataFrame) and symbol in self.df_symbols["symbol"].values:
            return self.df_symbols.loc[self.df_symbols["symbol"] == symbol, "leverage_long"].values[0]
        return None

    def get_leverage_short(self, symbol):
        if isinstance(self.df_symbols, pd.DataFrame) and symbol in self.df_symbols["symbol"].values:
            return self.df_symbols.loc[self.df_symbols["symbol"] == symbol, "leverage_short"].values[0]
        return None

    def log(self, msg, header="", attachments=None):
        if attachments is None:
            attachments = []
        for iter_logger in self.loggers:
            if iter_logger.__class__.__name__ != "LoggerDiscordBot":
                iter_logger.log(msg, header=header, author=type(self).__name__, attachments=attachments)

    def log_discord(self, msg, header="", attachments=None):
        if attachments is None:
            attachments = []
        for iter_logger in self.loggers:
            if iter_logger.__class__.__name__ == "LoggerDiscordBot":
                iter_logger.log(msg, header=header, author=type(self).__name__, attachments=attachments)

    def log_info(self):
        return ""

    def tick(self):
        if self.rtdp:
            return self.rtdp.tick()

    def check_data_description(self, data_description):
        if self.rtdp:
            self.rtdp.check_data_description(data_description)

    def get_current_data(self, data_description):
        if self.rtdp:
            return self.rtdp.get_current_data(data_description, self.fdp_url_id)
        return None

    def get_lst_current_data(self, data_description):
        if self.rtdp:
            return self.rtdp.get_lst_current_data(data_description, self.fdp_url_id)
        return None

    def get_fdp_ws_status(self):
        if self.rtdp:
            return self.rtdp.get_fdp_ws_status(self.fdp_url_id)
        return None

    def get_cash(self):
        return self.cash

    def get_cash_borrowed(self):
        return self.cash_borrowed

    def get_balance(self):
        return None

    def get_portfolio_value(self):
        return self.cash

    def get_current_datetime(self, format=None):
        current_datetime = datetime.now()
        if isinstance(current_datetime, datetime) and format != None:
            current_datetime = current_datetime.strftime(format)
        return current_datetime

    def get_final_datetime(self):
        if self.rtdp:
            return self.rtdp.get_final_datetime()
        return None

    def get_info(self):
        return None, None, None

    def broker_resumed(self):
        return False

    def get_usdt_equity(self):
        return None

    def get_current_state(self, lst_symbols):
        current_state = {
            "open_orders": create_df_open_orders(),
            "open_positions": create_df_open_positions(),
            "prices": create_df_prices()
        }
        return current_state

    def get_minimum_size(self, symbol):
        return 0

    def get_df_minimum_size(self, lst_symbols):
        lst = [self.get_minimum_size(symbol) for symbol in lst_symbols]
        df = pd.DataFrame({'symbol': lst_symbols, 'minBuyingSize': lst,'buyingSize':lst})
        return df

    def get_price_place_endstep(self, lst_symbol):
        return []

    def normalize_grid_df_buying_size_size(self, df_buying_size):
        return df_buying_size

    def execute_reset_account(self):
        pass

    def execute_trades(self, lst_orders):
        pass

    def reset_current_postion(self, current_state):
        pass

    @abstractmethod
    def get_value(self, symbol):
        pass

    def get_trading_range(self, symbol):
        return 0, 0

    @abstractmethod
    def get_commission(self, symbol):
        pass

    @abstractmethod
    def export_history(self, target):
        pass

    def log_info_trade(self):
        return ""

    def clear_log_info_trade(self):
        pass

    def save_reboot_data(self, df):
        pass

    def get_global_unrealizedPL(self):
        return 0

    @abstractmethod
    def _get_symbol(self, coin):
        pass

    @abstractmethod
    def _get_coin(self, symbol):
        pass

    def requests_cache_clear(self):
        pass

    def requests_cache_set(self, key, value):
        pass

    def requests_cache_get(self, key):
        return None

    def enable_cache(self):
        pass

    def disable_cache(self):
        pass

    def get_cache_status(self):
        return None

    def set_execute_time_recorder(self, execute_timer):
        pass
