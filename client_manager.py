import threading
import settings
import random
import time
import json
import pika

from amqp_conn import *

class ClientManager():
    def __init__(self, binder, queue, max_clients=100, keep_alive=10, fuck_up=3):
        self.binder = binder
        self.queue = queue

        self.receiver = Receiver(self.binder, self.queue)
        self.receiver.add_listener(self.on_connect,["client_connect"]) 
        self.receiver.add_listener(self.on_client_ready,["client_ready"]) 
        self.receiver.add_listener(self.on_keepalive,["keepalive"])

        self.sender = Sender(self.binder)

        self.connected_clients = dict()
        self.awaited_clients = set()
        self.active_clients = set()
        
        self.keep_alive = keep_alive
        self.fuck_up = fuck_up

        self.receiver.start()
        threading.Timer(self.keep_alive, self.manage_clients).start() # Repeated task

    def on_connect(self, receiver, method, message):
        print "On connect"
        try:
            connect_message = json.loads(message)
            client_id = int(connect_message["client_id"])
            client_name = connect_message["client_name"]

            routing_key = "".join(("client_",str(client_id)))
            holded_ids = self.awaited_clients.union(self.active_clients)

            if client_id in holded_ids:
                new_client_id = random.randint(0,10**6)

                while new_client_id in holded_ids:
                    new_client_id = random.randint(0,10**6)

                response = { 
			     "status" : "new_id",
			     "new_id" : new_client_id,
                             "keep-alive" : str(self.keep_alive),
			   }

                client_id = new_client_id
                self.sender.send_message(routing_key=routing_key, message=json.dumps(response))

            else:
                response = { 
			     "status" : "ok",
                             "keep-alive" : str(self.keep_alive),
			   }
                self.sender.send_message(routing_key=routing_key, message=json.dumps(response))

            self.connected_clients[client_id] = dict()
            self.connected_clients[client_id]["last_seen"] = time.time()
            self.connected_clients[client_id]["client_name"] = client_name

            self.awaited_clients.add(client_id) 
            
        except:
            print "Client connection error. Invalid message {0}".format(message)

    def on_client_ready(self, receiver, method, message):
        print "On client ready"
        try:
            ready_message = json.loads(message)
            client_id = int(ready_message["client_id"])

            if client_id in self.awaited_clients:
                self.connected_clients[client_id]["last_seen"] = time.time()
                
                print "Client {0} connected with id: {1}".format(
				self.connected_clients[client_id]["client_name"],client_id)

                self.active_clients.add(client_id)
                self.awaited_clients.remove(client_id)

            else:
                print "Unknown client {0}. Message: {1}".format(ready_message[client_id],message)
        except:
            print "Client ready error. Invalid message: {0}".format(message)

    def on_keepalive(self, receiver, method, message):
        print "On keepalive"
        keepalive_message = json.loads(message)
        client_id = int(keepalive_message["client_id"])

        if client_id not in set(self.connected_clients.keys()):
            print "Received keepalive from unknown client {0}".format(client_id)
            return
                
        if client_id in self.awaited_clients:
            self.active_clients.add(client_id)
            self.awaited_clients.remove(client_id)
            
        self.connected_clients[client_id]["last_seen"] = time.time()

    def manage_clients(self):
        print "Manage Clients"
        threading.Timer(self.keep_alive, self.manage_clients).start()
        now = time.time()
        print "Conneted clients: " 
        print self.connected_clients

        for client_id in self.connected_clients.keys():
            last_seen = self.connected_clients[client_id]["last_seen"] 
            fucked_up = int((now - last_seen)/self.keep_alive)

            if fucked_up > 1:
                if client_id in self.active_clients:
                    self.awaited_clients.add(client_id)
                    self.active_clients.remove(client_id)
                    print "Client {0} not responding".format(client_id)

                if fucked_up > self.fuck_up:
                    self.awaited_clients.remove(client_id)
                    del self.connected_clients[client_id]
                    print "Client {0} wasn't respond and has been kicked up".format(client_id)

    def await_clients(self, client_names, timeout=100.0):
        client_names = set(client_names)
        active_clients = set([self.connected_clients[i]["client_name"] for i in self.active_clients])

        while not client_names.issubset(active_clients) and timeout > 0:
            active_clients = set([self.connected_clients[i]["client_name"] for i in self.active_clients])
            timeout -= 1
            time.sleep(1)

        if  client_names.issubset(active_clients):
            return True
        
        print "Awaiting for clients has been canceled due timeout!"
	print "These clients hasn't connected yet:"

        for client_name in client_names.difference(active_clients):
            print client_name

        return False
