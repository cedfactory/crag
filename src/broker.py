from abc import ABCMeta, abstractmethod
from src.toolbox import settings_helper
from src import logger
import pandas as pd
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
    
    def __init__(self, params = None):
        self.zero_print = False
        self.loggers = [ logger.LoggerConsole() ]
        self.cash = 10
        self.fdp_url_id = "localhost:5000"
        self.reset_account = True  # Reset account default behavior
        self.reset_account_orders = True  # Reset account orders default behavior
        if params:
            self.loggers = params.get("loggers", self.loggers)
            self.cash = params.get("cash", self.cash)
            self.fdp_url_id = params.get("fdp_url_id", self.fdp_url_id)
            self.reset_account = params.get("reset_account", self.reset_account)
            if isinstance(self.reset_account, str):
                try:
                    self.reset_account = ast.literal_eval(self.reset_account)
                except BaseException as err:
                    self.reset_account = True
            self.reset_account_orders = params.get("reset_account_orders", self.reset_account)
            if isinstance(self.reset_account_orders, str):
                try:
                    self.reset_account_orders = ast.literal_eval(self.reset_account_orders)
                except BaseException as err:
                    self.reset_account_orders = True
        self.cash_borrowed = 10
        self.rtdp = None
        self.account = None
        if params:
            account_id = params.get("account", "")
            self.account = settings_helper.get_account_info(account_id)
            if not self.account:
                self.log("⚠ account {} not found".format(account_id))

    def resume_strategy(self):
        return not self.reset_account

    def is_reset_account(self):
        return self.reset_account

    def ready(self):
        return False

    def log(self, msg, header="", attachments=[]):
        if self.zero_print:
            return
        for iter_logger in self.loggers:
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

    def get_cash(self):
        return self.cash

    def get_cash_borrowed(self):
        return self.cash_borrowed

    def get_balance(self):
        return None

    def get_portfolio_value(self):
        # todo : to implement
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
        return 1000.

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

    def execute_orders(self, lst_orders):
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
    def execute_trade(self, trade):
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