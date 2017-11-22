import pika
import threading
import json
import time
import settings 

from amqp_conn import *
from client_manager import ClientManager
from client import Client

class ClientsNotReady(Exception):
    pass

class Processor(Client):
    def __init__(self, 	remote_binder, local_binder, remote_task_queue, local_queue, clients):

        self.remote_binder = remote_binder
        self.local_binder = local_binder

        self.remote_server_queue = remote_server_queue
        self.remote_task_queue = remote_task_queue
        self.local_queue = local_queue

        self.remote_receiver = Receiver(remote_binder, queue = remote_task_queue, shared_conn=True)
        self.remote_receiver.add_listener(self.dispatch_task,[settings.PROCESSOR_TASK_REMOTE_RK])
        
        self.local_sender = Sender(local_binder)
        self.local_receiver = Receiver(local_binder, queue = local_queue, shared_conn=True)
        self.local_receiver.add_listener(self.dispatch_result,["task_result"])

        self.client_manager = ClientManager(local_binder, queue=local_queue)
        self.client_manager.await_clients(clients)

        self.pending_tasks = dict() # Set of clients for task_id
        self.task_queue    = dict() # Task data
        self.task_results  = list()  

        super(Processor,self).__init__(binder=remote_binder, queue = remote_server_queue, client_name = settings.PROCESSOR_NAME)
        super(Processor,self).connect(timeout=settings.PROCESSOR_CONNECT_TIMEOUT) # Connect to remote client manager    
   
        self.local_receiver.start()
        self.remote_receiver.start()


    def kill(self):
        super(Client, self).kill()

        if self.local_receiver.ch.is_open:
            self.local_receiver.stop_consuming()

        self.local_receiver.join()

    def dispatch_result(self, method, body):
        message = json.loads(body)
        task_id = message["task_id"]
        client_id = int(message["client_id"])

        connected_clients = self.connection_manager.connected_clients.keys()
        connected_clients = set(connected_clients)

        if client_id not in connected_clients:
            print "Unknown client! Client id: {0}".format(client_id)
            return
  
        active_clients = self.connection_manager.active_clients

        if client_id not in active_clients:
            print "Got message from inactive client! Client id: {0}".format(client_id)
            return

        if task_id in pending_tasks[client_id]:
            task_results.append(message["task_result"])
            pending_tasks[task_id].remove(client_id) 

            if len(pending_tasks[task_id]) == 0:
                del pending_tasks[task_id] 
                del task_queue[task_id]

        else:
            print '''Got task that is not in pending tasks!\n Task id {0}, client id: {1}'''.format(task_id, client_id)

    def dispatch_task(self, receiver, method, body):
        message = json.loads(body)
        for task in message:
            task_id = task["task_id"]
            task_data = task["task_data"]

            self.task_queue[task_id] = task_data
            self.local_sender.send_message(routing_key = "task",
					   exchange = settings.LOCAL_EXCHANGE,
					   	message = json.dumps(task))

            self.pending_tasks[task_id] = self.connection_manager.active_clients

        return

    def report_manager(self):
        if len(self.task_results) > 0:
            self.sender.send_message(routing_key = "task_result",
					exchange = REMOTE_EXCHANGE,
					message = json.dumps(self.task_results))
        self.task_results = list()

# killall -9 python 


        

