from . import broker
from datetime import datetime
import json
from .bitget import exceptions
from .bitget.spot_v2 import account_api as spotAccountV2

class BrokerBitGetSpot(broker.Broker):
    def __init__(self, params = None):
        super().__init__(params)
        self.name = ""
        self.exchange_name = "bitget"
        self.failure = 0
        self.success = 0

        api_key = self.account.get("api_key", "")
        api_secret = self.account.get("api_secret", "")
        api_password = self.account.get("api_password", "")

        self.spotAccountV2Api = None
        if api_key != "" and api_secret != "" and api_password != "":
            self.spotAccountV2Api = spotAccountV2.AccountApi(api_key, api_secret, api_password, use_server_time=False, first=False)

    def _get_coin(self, symbol):
        return None

    def _get_symbol(self, coin):
        return None

    def export_history(self, target):
        return None

    def get_commission(self, symbol):
        return None

    def _authentification(self):
        return self.spotAccountV2Api

    def authentication_required(fn):
        """decoration for methods that require authentification"""
        def wrapped(self, *args, **kwargs):
            if not self._authentification():
                self.log("You must be authenticated to use this method {}".format(fn))
                return None
            else:
                return fn(self, *args, **kwargs)
        return wrapped

    def log_api_failure(self, function, e, n_attempts=0):
        self.failure += 1
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        self.log("💥 !!!!! Failure on: " + function)
        self.log("current time = " + str(current_time) + "  - attempt: " + str(n_attempts))
        self.log("failure: " + str(self.failure) + " - success: " + str(self.success) + " - percentage failure: " + str(self.failure / (self.success + self.failure) * 100))

        if hasattr(e, "message"):
            message = e.message
            self.log("message: " + message)

        if hasattr(e, "response") and hasattr(e.response, "content"):
            content = e.response.content.decode('utf-8')
            dict_content = json.loads(content)
            dict_content_as_str = ' - '.join(f'{key}: {value}' for key, value in dict_content.items())

            self.log("content: " + json.dumps(dict_content_as_str))

    def get_value(self, symbol):
        value = None
        try:
            response = self.spotAccountV2Api.assets(params={"coin": symbol})
            if "msg" in response and response["msg"] == "success":
                if "data" in response and len(response["data"]) == 1:
                    value = float(response["data"][0]["available"])
        except (exceptions.BitgetAPIException, Exception) as e:
            self.log_api_failure("positionApi.all_position", e)

        return value

    def get_usdt_equity(self):
        value = None
        try:
            response = self.spotAccountV2Api.assets(params={"coin": "USDT"})
            if "msg" in response and response["msg"] == "success":
                if "data" in response and len(response["data"]) == 1:
                    value = float(response["data"][0]["available"])
        except (exceptions.BitgetAPIException, Exception) as e:
            self.log_api_failure("positionApi.all_position", e)

        return value
