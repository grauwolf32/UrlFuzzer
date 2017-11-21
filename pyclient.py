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

class PyClient(Client):
    def __init__(self, conn,client_name,queue):
        self.conn = conn
        self.sender = Sender(conn,settings.LOCAL_EXCHANGE)
        self.receiver = Receiver(conn,settings.LOCAL_EXCHANGE, queue)
        
        Super(Client, self).__init__(receiver, listener, client_name)
        self.receiver.add_listener(self.process_result,["task"])

    def process_task(self, receiver, method, body):
        message = json.loads(body)
        
        task_id = message["task_id"]
        task_data = message["task_data"]

	try:
            task_result = process_data(task)

        except:
            task_result = "error"

        response = { 
                "client_id" : self.client_id,
                "task_id" : task_id,
		"task_result" : task_result,
		}

        self.sender.send_message(routing_key="task_result", message=json.dumps(response))
        receiver.ch.basic_ack(delivery_tag = delivery_tag)

        
    def process_data(self, task_data):
        parse_result = urlparse(task)
        process_result  = dict()
        
        attributes  = ["scheme","netloc","path","params","query","fragment"]
        attributes += ["username","password","hostname","port"]

        for attribute in attributes:
            try:
                tmp = getattr(parse_result, attribute)
                if tmp: 
                    process_result[attribute] = tmp
                else:
                    process_result[attribute] = ""

            except AttributeError:
                process_result[attribute] = ""

        return process_result
