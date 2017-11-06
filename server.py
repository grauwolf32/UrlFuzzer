import pika
import threading
import settings
import random
import json
import time

from amqp_conn import Receiver, Sender

cred = pika.PlainCredentials(settings.CC_USER, settings.CC_PASSWD)
conn = pika.BlockingConnection(pika.ConnectionParameters(credentials=cred,host=settings.LOCAL_IP))

class ClientManager():
    def __init__(self, receiver, sender, max_clients=100):
        self.connected_clients = dict()
	self.notready_clients = set()
        self.active_clients = set()

        self.receiver = receiver
        self.sender = sender

        self.receiver.add_listener(self.on_connect,["client_connect"]) 
        self.receiver.add_listener(self.on_client_ready,["client_ready"]) 
        self.receiver.add_listener(self.on_keepalive,["keepalive"]) 

    def on_connect(self, receiver, message):
        try:
            connect_message = json.loads(message)
            client_id = int(connect_message["client_id"])
            routing_key = "".join(("client_",str(client_id)))

            if client_id in self.notready_clients
               or client_id in self.active_clients:

                new_client_id = random.randint(0,10**6)
                while new_client_id in self.notready_clients
                      or new_client_id in self.active_clients:
                    new_client_id = random.randint(0,10**6)

                response = { 
			     "status" : "new_id",
			     "new_id" : new_client_id,
			   }

                self.connected_clients[new_client_id] = dict()
                self.connected_clients[new_client_id]["last_seen"] = time.time()
                self.connected_clients[new_client_id]["active"] = 0;

                self.notready_clients.add(new_client_id)
                self.sender.send_message(routing_key=routing_key, message=json.loads(response))
            else:
                response = { "status" : "ok"}

                self.connected_clients[new_client_id] = dict()
                self.connected_clients[new_client_id]["last_seen"] = time.time()
                self.connected_clients[new_client_id]["active"] = 0;

                self.notready_clients.add(client_id)
                self.sender.send_message(routing_key=routing_key, message=json.loads(response))
            
        except:
            print "Client connection error. Invalid message".format(message)

    def on_client_ready(self, receiver, message):
	try:
            ready_message = json.loads(message)
            client_id = int(ready_message["client_id"])

            if client_id in self.notready_clients:
                self.connected_clients[new_client_id]["last_seen"] = time.time()
                self.connected_clients[new_client_id]["active"] = 1;

                self.active_clients.add(client_id)
                self.notready_clients.remove(client_id)
            else:
                print "Unknown client {0}. Message: {1}".format(ready_message["client_id"],message)
        except:
            print "Client ready error. Invalid message: {0}".format(message)

    def on_keepalive(self, receiver, message):
        keepalive_message = json.loads(message)
        client_id = keepalive_message["client_id"]
        
    

class Server():
    def __init__(self,conn):
        self.conn = conn
        self.sender = Sender(conn,settings.CC_EXCHANGE)
        self.receiver = Receiver(conn,settings.CC_EXCHANGE,settings.CC_QUEUE)
        self.receiver.add_listener(self.on_result,["task_result"])

        self.task_counter = 0

    def start(self):
        self.receiver.start()
    
    def on_result(self, receiver, message):
        print message

    def on_connect(self, receiver, message):
        
    
    
    def send_tasks(self, payloads, reported_ids):
        message = dict()
        message["reported_ids"] = list()
        message["tasks"] = dict()

        reported_ids = set(reported_ids)
     
        for payload_id in xrange(0,len(payloads)):
            message["tasks"][str(self.task_counter)] = payloads[payload_id]
            if payload_id in reported_ids:
                message["reported_ids"].append(self.task_counter)

            self.task_counter += 1
        
        self.sender.send_message(routing_key="task",message=json.dumps(message))


def main():
    s = Server(conn)
    s.start()
    tasks = ["http://example.com","https://google.com"]
    s.send_tasks(tasks,list(xrange(0,len(tasks))))

if __name__== "__main__":
    main()  
          
    

    
