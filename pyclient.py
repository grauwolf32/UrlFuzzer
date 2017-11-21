import pika
import threading
import settings
import json
import time

from urlparse import urlparse
from amqp_conn import Receiver, Sender
from client import Client

cred = pika.PlainCredentials(settings.LOCAL_USER, settings.LOCAL_PASSWD)
conn = pika.BlockingConnection(pika.ConnectionParameters(credentials=cred,host=settings.LOCAL_IP))

def dump_result(parse_result):
    task_result  = dict()
    task_result["scheme"] = parse_result.scheme
    task_result["netloc"] = parse_result.netloc
    task_result["path"] = parse_result.path
    task_result["params"] = parse_result.params
    task_result["query"] = parse_result.query
    task_result["fragment"] = parse_result.fragment

    if parse_result.username:
        task_result["username"] = parse_result.username
    else:
        task_result["username"] = ""

    if parse_result.password:
        task_result["password"] = parse_result.password
    else:
        task_result["password"] = ""

    if parse_result.hostname:
        task_result["hostname"] = parse_result.hostname
    else:
        task_result["hostname"] = ""

    if parse_result.port:
        task_result["port"] = parse_result.port
    else:
        task_result["port"] = ""
    return task_result


class PyClient(Client):
    def __init__(self, conn,client_name,queue):
        self.conn = conn
        self.sender = Sender(conn,settings.LOCAL_EXCHANGE)
        self.receiver = Receiver(conn,settings.LOCAL_EXCHANGE, queue)
        
        Super(Client, self).__init__(receiver, listener, client_name)
        self.receiver.add_listener(self.process_result,["task"])

    def process(self, receiver, method, body):
        message = json.loads(body)
        
        task_id = message["task_id"]
        task_data = message["task_data"]

	try:
            result = do_task(task)
        except:
            error = "1"

        task_result = dict()
        task_result["error"] = error
        task_result["task_id"] = task_id
        task_result["lang"] = self.client

        if error == "1":
            result = ""

        task_result["result"] = result
        self.sender.send_message(routing_key="result",message=json.dumps(task_result))
        receiver.ch.basic_ack(delivery_tag = delivery_tag)

        
    def do_task(self,task):
        parse_result = urlparse(task)
        task_result  = dump_result(parse_result)
        return task_result

def main():
    client = PyClient(conn,"python","python_queue")
    client.start()

if __name__== "__main__":
    main()
