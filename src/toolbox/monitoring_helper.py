from abc import ABCMeta, abstractmethod
import urllib
import urllib.parse
import urllib.request
import json
from . import settings_helper

class IMonitoring(metaclass=ABCMeta):

    def __init__(self, id):
        pass

    @abstractmethod
    def is_ready(self):
        return False

    @abstractmethod
    def send_alive_notification(self, timestamp, account_id, strategy_id):
        pass

#
# SQLMonitoring
#
class SQLMonitoring(IMonitoring):
    def __init__(self, id):
        self.ready = True
        info = settings_helper.get_monitor_info(id)
        self.url_base = info.get("url_base", "")
        self.user = info.get("user", "")
        if self.url_base == "" or self.user == "":
            self.ready = False

    def is_ready(self):
        return self.ready

    def _request_get(self, url):
        n_attempts = 3
        response_json = {}
        while n_attempts > 0:
            try:
                request = urllib.request.Request(url)
                request.add_header("User-Agent", "cheese")
                response = urllib.request.urlopen(request).read()
                response_json = json.loads(response)
                break
            except:
                reason = "exception when requesting GET {}".format(url)
                response_json = {"status": "ko", "info": reason}
                n_attempts = n_attempts - 1
        return response_json

    def send_alive_notification(self, timestamp, account_id, strategy_id):
        url = self.url_base
        url += "user={}&".format(self.user)
        url += "timestamp={}&".format(timestamp)
        url += "account_id={}&".format(account_id)
        url += "strategy_id={}".format(strategy_id)
        response_json = self._request_get(url)
        return response_json

