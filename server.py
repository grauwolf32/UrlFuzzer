import pika
import threading
import settings
import random
import json
import time

from amqp_conn import Receiver, Sender, Connection
from client_manager import ClientManager
            
class Server():
    def __init__(self, binder, local_queue):
        self.binder = binder
        self.sender = Sender(binder)
        self.receiver = Receiver(binder, local_queue)
        self.receiver.add_listener(self.on_result,["task_result"])

        self.client_manager = ClientManager(binder, local_queue, self.sender,keep_alive=100)
        self.task_queue = list()
        self.receiver.start()
    
    def on_result(self, receiver, method, body):
        print body

    def add_task(self, task):
        self.task_queue.append(task)
        if len(self.task_queue) > settings.MAX_QUEUE_LEN:
            send_tasks(self.task_queue)
            self.task_queue = list()
    
    def send_tasks(self, tasks):
        self.sender.send_message(routing_key="task",message=json.dumps(tasks))
      
