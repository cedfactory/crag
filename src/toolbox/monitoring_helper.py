from abc import ABCMeta, abstractmethod
import urllib
import urllib.parse
import urllib.request
import json
from datetime import datetime
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
# RabbitMQ monitoring
#
import pika
import threading
import uuid

accounts_info = {}

def print_accounts_info():
    print(accounts_info)
    return
    for account_info in accounts_info:
        print("timestamp : ", account_info["timestamp"])
        print("account_id : ", account_info["account_id"])
        print("strategy_id : ", account_info["strategy_id"])

def launch_rabbitmq_server():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))
        channel = connection.channel()

        # define basic_consume
        channel.queue_declare(queue='StrategyMonitoring')

        def callback(ch, method, properties, bbody):
            print(" [publish received] ", bbody)
            body = bbody.decode()
            body = json.loads(body)

            if body["id"] == "alive":
                account_id = body["account_id"]
                accounts_info[account_id] = body

        channel.basic_consume(queue='StrategyMonitoring', on_message_callback=callback, auto_ack=True)

        # define rpc queue
        # https://www.rabbitmq.com/tutorials/tutorial-six-python.html
        channel.queue_declare(queue='StrategyMonitoringRequest')

        def on_request(ch, method, props, bbody):
            account_id = bbody.decode()

            print(" [request received] ", account_id)
            response = ""
            if account_id in accounts_info:
                response = accounts_info[account_id]["strategy_id"]

            ch.basic_publish(exchange='',
                             routing_key=props.reply_to,
                             properties=pika.BasicProperties(correlation_id=props.correlation_id),
                             body=str(response))
            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='StrategyMonitoringRequest', on_message_callback=on_request)

        # channel.start_consuming()
        thread = threading.Thread(name='t', target=channel.start_consuming, args=())
        #thread.setDaemon(True)
        thread.start()

    except:
        print("Problem encountered while configuring the rabbitmq receiver")

class RabbitMQMonitoring(IMonitoring):
    def __init__(self, id):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def GetStrategyOnAccount(self, account_id):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='StrategyMonitoringRequest',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=account_id)
        self.connection.process_data_events(time_limit=None)
        return self.response

    def send_alive_notification(self, timestamp, account_id, strategy_id):
        data = {"id": "alive",
                "timestamp": timestamp,
                "account_id": account_id,
                "strategy_id": strategy_id,
                "interval": 1000}
        body = json.dumps(data)
        body = body.encode()

        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('127.0.0.1'))
            channel = connection.channel()
            channel.queue_declare(queue='StrategyMonitoring')
            channel.basic_publish(exchange='', routing_key='StrategyMonitoring', body=body)
            connection.close()
        except:
            print("Problem encountered while sending a notification")

    def publish_strategy_stop(strategy_id):
        current = datetime.now()
        timestamp = datetime.timestamp(current)
        data = {"timestamp": timestamp,
                "id": "command",
                "strategy_id": strategy_id,
                "command": "stop"
                }
        body = json.dumps(data)
        body = body.encode()

        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('127.0.0.1'))
            channel = connection.channel()
            channel.queue_declare(queue='StrategyMonitoring')
            channel.basic_publish(exchange='', routing_key='StrategyMonitoring', body=body)
            connection.close()
        except:
            print("Problem encountered while sending a notification")

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

