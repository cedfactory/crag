#!/usr/bin/python
import json
import math
import threading
import time
import traceback
from threading import Timer
from zlib import crc32
import hmac
import base64

import websocket
from typing import Optional

GET = "GET"
REQUEST_PATH = '/user/verify'

CONTRACT_WS_URL = 'wss://ws.bitget.com/mix/v1/stream'

WS_OP_LOGIN = 'login'
WS_OP_SUBSCRIBE = "subscribe"
WS_OP_UNSUBSCRIBE = "unsubscribe"

# Websocket planType
WS_PLAN_TYPES = ['pl', 'tp', 'sl', 'ptp', 'psl']

# Websocket channels type
WS_PRIVATE_ACCOUNT_CHANNEL = "account"
WS_PRIVATE_ORDERS_CHANNEL = "orders"
WS_PRIVATE_POSITIONS_CHANNEL = "positions"
WS_PRIVATE_PLAN_ORDERS_CHANNEL = "ordersAlgo"
WS_CHANNEL_INSTTYPE = "UMCBL"
WS_CHANNEL_INSTID = "default"

def handle(message):
    print(message)

def handle_error(message):
    print("Error :", message)

def create_sign(message, secret_key):
    mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
    d = mac.digest()
    return str(base64.b64encode(d), 'utf8')


def pre_hash(timestamp, method, request_path):
    return str(timestamp) + str.upper(method) + str(request_path)


class BitgetWsClient:
    def __init__(self,
                 api_key: Optional[str] = None,
                 api_secret: Optional[str] = None,
                 passphrase: Optional[str] = None,
                 ws_url: Optional[str] = None,
                 verbose: Optional[str] = False,
                 ):
        self.__api_key = api_key
        self.__api_secret_key = api_secret
        self.__passphrase = passphrase
        self.__connection = False
        self.__login_status = False
        self.__reconnect_status = False
        self.__all_suribe = set()
        self.__listener = handle
        self.__error_listener = handle_error
        self.__scribe_map = {}
        self.__allbooks_map = {}
        self.__ws_client = None
        self.__explicit_close = False
        self.__timer_thread = None

        if ws_url is None:
            self.STREAM_URL = CONTRACT_WS_URL
        else:
            self.STREAM_URL = ws_url
        self.verbose = verbose

    def log_info(self, msg):
        if self.verbose:
            print("Info :", msg)

    def log_warning(self, msg):
        if self.verbose:
            print("Warning :", msg)

    def log_debug(self, msg):
        if self.verbose:
            print("Debug :", msg)

    def log_error(self, msg):
        if self.verbose:
            print("Error :", msg)

    def build(self):
        self.__ws_client = self.__init_client()
        __thread = threading.Thread(target=self.connect)
        __thread.start()

        while not self.has_connect():
            self.log_info("start connecting...%s" % self.STREAM_URL)
            time.sleep(1)

        if self.__api_key is not None and self.__api_secret_key is not None and self.__passphrase is not None:
            self.__login()

        self.__keep_connected(25)

        return self

    def close(self):
        self.__explicit_close = True
        if self.__timer_thread:
            self.__timer_thread.cancel()
        self.__login_status = False
        self.__connection = False
        self.__ws_client.close()

    def listener(self, listener):
        self.__listener = listener
        return self

    def error_listener(self, error_listener):
        self.__error_listener = error_listener
        return self

    def has_connect(self):
        return self.__connection

    def __init_client(self):
        try:
            return websocket.WebSocketApp(self.STREAM_URL,
                                          on_open=self.__on_open,
                                          on_message=self.__on_message,
                                          on_error=self.__on_error,
                                          on_close=self.__on_close)

        except Exception as ex:
            self.log_error(ex)

    def __login(self):
        timestamp = int(round(time.time()))
        sign = create_sign(pre_hash(timestamp, GET, REQUEST_PATH), self.__api_secret_key)
        ws_login_req = WsLoginReq(self.__api_key, self.__passphrase, str(timestamp), sign)
        self.send_message(WS_OP_LOGIN, [ws_login_req])
        self.log_info("logging in......")
        while not self.__login_status:
            time.sleep(1)

    def connect(self):
        try:
            self.__ws_client.run_forever(ping_timeout=10)
        except Exception as ex:
            self.log_error(ex)

    def __keep_connected(self, interval):
        if self.__explicit_close:
            return
        try:
            self.__timer_thread = None
            self.__timer_thread = Timer(interval, self.__keep_connected, (interval,))
            self.__timer_thread.start()
            self.__ws_client.send("ping")
        except Exception as ex:
            self.log_error(ex)

    def send_message(self, op, args):
        message = json.dumps(BaseWsReq(op, args), default=lambda o: o.__dict__)
        self.log_debug(message)
        self.__ws_client.send(message)

    def subscribe(self, channels, listener=None):

        if listener:
            for chanel in channels:
                chanel.inst_type = str(chanel.inst_type).lower()
                self.__scribe_map[chanel] = listener

        for channel in channels:
            self.__all_suribe.add(channel)

        self.send_message(WS_OP_SUBSCRIBE, channels)

    def unsubscribe(self, channels):
        try:
            for channel in channels:
                if channel in self.__scribe_map:
                    del self.__scribe_map[channel]

            for channel in channels:
                if channel in self.__all_suribe:
                    self.__all_suribe.remove(channel)

            self.send_message(WS_OP_UNSUBSCRIBE, channels)
        except Exception as e:
            self.log_error(e)
            pass

    def __on_open(self, ws):
        self.log_info('connection is success....')
        self.__connection = True
        self.__reconnect_status = False

    def __on_message(self, ws, message):

        if message == 'pong':
            #self.log_info("Keep connected: %s" % message)
            return
        json_obj = json.loads(message)
        if "code" in json_obj and json_obj.get("code") != 0:
            if self.__error_listener:
                self.__error_listener(message)
                return

        if "event" in json_obj and json_obj.get("event") == "login":
            self.log_debug("login msg: %s" % message)
            self.__login_status = True
            return
        listenner = None
        if "data" in json_obj:
            if not self.__check_sum(json_obj):
                return

            listenner = self.get_listener(json_obj)

        if listenner:
            listenner(message)
            return

        self.__listener(message)

    def __dict_books_info(self, a_dict):
        return BooksInfo(a_dict['asks'], a_dict['bids'], a_dict['checksum'])

    def __dict_to_subscribe_req(self, a_dict):
        return SubscribeReq(a_dict['instType'], a_dict['channel'], a_dict['instId'])

    def get_listener(self, json_obj):
        try:
            if json_obj.get('arg'):
                json_str = str(json_obj.get('arg')).replace("\'", "\"")
                subscribe_req = json.loads(json_str, object_hook=self.__dict_to_subscribe_req)
                return self.__scribe_map.get(subscribe_req)
        except Exception as e:
            self.log_error("%s %s " % (json_obj.get('arg'), e))
            pass

    def __on_error(self, ws, msg):
        self.log_error(msg)
        self.__close()
        if not self.__reconnect_status:
            self.__re_connect()

    def __on_close(self, ws, close_status_code, close_msg):
        self.log_info("ws is closeing ......close_status:{},close_msg:{}".format(close_status_code, close_msg))
        self.__close()
        if not self.__explicit_close and not self.__reconnect_status:
            self.__re_connect()

    def __re_connect(self):
        self.__reconnect_status = True
        self.log_info("start reconnection ...")
        self.build()
        for channel in self.__all_suribe:
            self.subscribe([channel])
        pass

    def __close(self):
        self.__login_status = False
        self.__connection = False
        self.__ws_client.close()

    def __check_sum(self, json_obj):
        # noinspection PyBroadException
        try:
            if "arg" not in json_obj or "action" not in json_obj:
                return True
            arg = str(json_obj.get('arg')).replace("\'", "\"")
            action = str(json_obj.get('action')).replace("\'", "\"")
            data = str(json_obj.get('data')).replace("\'", "\"")

            subscribe_req = json.loads(arg, object_hook=self.__dict_to_subscribe_req)

            if subscribe_req.channel != "books":
                return True

            books_info = json.loads(data, object_hook=self.__dict_books_info)[0]

            if action == "snapshot":
                self.__allbooks_map[subscribe_req] = books_info
                return True
            if action == "update":
                all_books = self.__allbooks_map[subscribe_req]
                if all_books is None:
                    return False

                all_books = all_books.merge(books_info)
                check_sum = all_books.check_sum(books_info.checksum)
                if not check_sum:
                    self.unsubscribe([subscribe_req])
                    self.subscribe([subscribe_req])
                    return False
                self.__allbooks_map[subscribe_req] = all_books
        except Exception as e:
            msg = traceback.format_exc()
            self.log_error("%s %s" % (msg, e))

        return True


class BooksInfo:
    def __init__(self, asks, bids, checksum):
        self.asks = asks
        self.bids = bids
        self.checksum = checksum

    def merge(self, book_info):
        self.asks = self.innerMerge(self.asks, book_info.asks, False)
        self.bids = self.innerMerge(self.bids, book_info.bids, True)
        return self

    def innerMerge(self, all_list, update_list, is_reverse):
        price_and_value = {}
        for v in all_list:
            price_and_value[v[0]] = v

        for v in update_list:
            if v[1] == "0":
                del price_and_value[v[0]]
                continue
            price_and_value[v[0]] = v

        keys = sorted(price_and_value.keys(), reverse=is_reverse)

        result = []

        for i in keys:
            result.append(price_and_value[i])

        return result

    def check_sum(self, new_check_sum):
        crc32str = ''
        for x in range(25):
            if self.bids[x] is not None:
                crc32str = crc32str + self.bids[x][0] + ":" + self.bids[x][1] + ":"

            if self.asks[x] is not None:
                crc32str = crc32str + self.asks[x][0] + ":" + self.asks[x][1] + ":"

        crc32str = crc32str[0:len(crc32str) - 1]
        #self.log_debug(crc32str)
        merge_num = crc32(bytes(crc32str, encoding="utf8"))
        # print("start checknum mergeVal:" + str(merge_num) + ",checkVal:" + str(new_check_sum) + ",checkSin:" + str(self.__signed_int(merge_num)))
        return self.__signed_int(merge_num) == new_check_sum

    def __signed_int(self, checknum):
        int_max = math.pow(2, 31) - 1
        if checknum > int_max:
            return checknum - int_max * 2 - 2
        return checknum


class SubscribeReq:

    def __init__(self, inst_type, channel, instId):
        self.inst_type = inst_type
        self.channel = channel
        self.inst_id = instId

    def __eq__(self, other) -> bool:
        return self.__dict__ == other.__dict__

    def __hash__(self) -> int:
        return hash(self.inst_type + self.channel + self.inst_id)


class BaseWsReq:

    def __init__(self, op, args):
        self.op = op
        self.args = args


class WsLoginReq:

    def __init__(self, api_key, passphrase, timestamp, sign):
        self.api_key = api_key
        self.passphrase = passphrase
        self.timestamp = timestamp
        self.sign = sign
