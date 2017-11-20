import pika
import threading
import json
import time
import signal
import settings 

from amqp_conn import Receiver, Sender
from client_manager import ClientManager
from client import Client

cc_cred = pika.PlainCredentials(settings.CC_USER, settings.CC_PASSWD)
cc_conn = pika.BlockingConnection(pika.ConnectionParameters(credentials=cc_cred,host=settings.CC_IP))

local_cred = pika.PlainCredentials(settings.LOCAL_USER, settings.LOCAL_PASSWD)
local_conn = pika.BlockingConnection(pika.ConnectionParameters(credentials=local_cred,host=settings.LOCAL_IP))

class ClientsNotReady(Exception):
    pass

class Processor(Client):
    def __init__(self, cc_conn, local_conn, clients):
        self.cc_conn = cc_conn
        self.local_conn = local_conn
        
        sender = Sender(self.cc_conn,settings.CC_EXCHANGE)
        receiver = Receiver(conn = self.cc_conn, exch = settings.CC_EXCHANGE, queue = settings.CC_QUEUE)
        super(Client,self).__init__(receiver, sender, client_name="processor")
        self.receiver.add_listener(self.dispatch_task,["task"])
       
        self.local_sender = Sender(local_conn,settings.LOCAL_EXCHANGE)
        self.local_receiver = Receiver(conn = local_conn, exch = settings.LOCAL_EXCHANGE, queue = settings.LOCAL_QUEUE)
        self.local_receiver.add_listener(self.dispatch_result,["result"])
        self.local_receiver.add_listener(self.supervisor,["job-done","client-id"])
        self.local_receiver.start()

        self.client_manager = ClientManager(self.local_receiver,self.local_sender)
        self.client_manager.await_clients(clients)

        self.pending_tasks = dict() # Set of clients for task_id
        self.task_queue = dict()    # Task data
        self.task_results  = list()  
        

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
            task_results.append(message["result"])
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
            task_data = task["data"]

            self.task_queue[task_id] = task_data
            self.local_sender.send_message(routing_key = "task",
					   exchange = settings.LOCAL_EXCHANGE,
					   	message = json.dumps(task))

            self.pending_tasks[task_id] = self.connection_manager.active_clients

        return

    def report_manager(self):
        if len(self.task_results) > 0:
            self.sender.send_message(routing_key = "task_result",
					exchange = CC_EXCHANGE,
					message = json.dumps(self.task_results))
        self.task_results = list()


def main():
    processor = Processor(cc_conn, local_conn,["Python"])
    connected = super(Client,self).connect(timeout = 20.0)
    if not connected:
        print "Processor could not connect to server!"

    return

if __name__=="__main__":
    main()
    
        
#def signal_handler(signal, frame):
#    print 'You pressed Ctrl+C!'
#    sys.exit(0)

#signal.signal(signal.SIGINT, signal_handler)
# ps -ax  | grep processor.py | cut -d " " -f1| xargs -I {} kill -9 {}


        

