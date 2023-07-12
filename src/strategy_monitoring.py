import pika
import threading
import json
from datetime import datetime
import uuid

accounts_info = {}

def print_accounts_info():
    print(accounts_info)
    return
    for account_info in accounts_info:
        print("timestamp : ", account_info["timestamp"])
        print("account_id : ", account_info["account_id"])
        print("strategy_id : ", account_info["strategy_id"])
def launch():
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


def publish_alive_strategy(account_id, strategy_id):
    current = datetime.now()
    timestamp = datetime.timestamp(current)
    data = {"id": "alive",
            "timestamp": timestamp,
            "account_id": account_id,
            "strategy_id": strategy_id,
            "interval": 1000}
    body = json.dumps(data)
    body = body.encode()

    connection = pika.BlockingConnection(pika.ConnectionParameters('127.0.0.1'))
    channel = connection.channel()
    channel.queue_declare(queue='StrategyMonitoring')
    channel.basic_publish(exchange='', routing_key='StrategyMonitoring', body=body)
    connection.close()

class StrategyMonitoringClient(object):
    def __init__(self):
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
