from abc import ABCMeta, abstractmethod
from src.toolbox import settings_helper
import ast

class Broker(metaclass = ABCMeta):
    
    def __init__(self, params = None):
        self.cash = 1
        self.fdp_url_id = "localhost:5000"
        self.reset_account = True  # Reset account default behavior
        self.reset_account_orders = True  # Reset account orders default behavior
        if params:
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
        self.cash_borrowed = 0
        self.rtdp = None
        self.account = None
        if params:
            account_id = params.get("account", "")
            self.account = settings_helper.get_account_info(account_id)
            if not self.account:
                print("[Broker] : ⚠ account {} not found".format(account_id))

    def resume_strategy(self):
        return not self.reset_account

    def is_reset_account(self):
        return self.reset_account

    def ready(self):
        return False

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
        if self.rtdp:
            return self.rtdp.get_current_datetime(format)
        return None

    def get_final_datetime(self):
        if self.rtdp:
            return self.rtdp.get_final_datetime()
        return None

    def get_info(self):
        return None, None, None

    def broker_resumed(self):
        return False

    def get_usdt_equity(self):
        return 0.

    def get_current_state(self, lst_symbols):
        current_state = {
            "open_orders": None,
            "open_positions": None,
            "prices": None
        }
        return None

    def get_df_minimum_size(self, lst_symbols):
        return None

    def get_price_place_endstep(self, lst_symbol):
        return []

    def execute_reset_account(self):
        pass

    def execute_orders(self, lst_orders):
        pass

    def reset_current_postion(self, current_state):
        pass

    @abstractmethod
    def get_value(self, symbol):
        pass

    @abstractmethod
    def get_commission(self, symbol):
        pass

    @abstractmethod
    def execute_trade(self, trade):
        pass

    @abstractmethod
    def export_history(self, target):
        pass

    @abstractmethod
    def _get_symbol(self, coin):
        pass

    @abstractmethod
    def _get_coin(self, symbol):
        pass