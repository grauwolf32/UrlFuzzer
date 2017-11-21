import pika
import threading
import settings
import random
import json
import time

from amqp_conn import Receiver, Sender
from client_manager import ClientManager
            
class Server():
    def __init__(self,conn):
        self.conn = conn
        self.sender = Sender(conn, settings.LOCAL_EXCHANGE)
        self.receiver = Receiver(conn, settings.LOCAL_EXCHANGE, settings.LOCAL_TASK_QUEUE)
        self.receiver.add_listener(self.on_result,["task_result"])
        self.receiver.start()

        self.client_manager = ClientManager(self.receiver, self.sender,keep_alive=100)
        self.task_queue = list()
    
    def on_result(self, receiver, method, body):
        print body

    def add_task(self, task):
        self.task_queue.append(task)
        if len(self.task_queue) > settings.MAX_QUEUE_LEN):
            send_tasks(self.task_queue)
            self.task_queue = list()
    
    def send_tasks(self, tasks):
        self.sender.send_message(routing_key="task",message=json.dumps(tasks))
      
