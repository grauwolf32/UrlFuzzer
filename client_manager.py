from amqp_conn import Receiver, Sender

cred = pika.PlainCredentials(settings.CC_USER, settings.CC_PASSWD)
conn = pika.BlockingConnection(pika.ConnectionParameters(credentials=cred,host=settings.LOCAL_IP))

class ClientManager():
    def __init__(self, receiver, sender, max_clients=100, keep_alive=10, fuck_up=3):
        self.connected_clients = dict()
        self.awaited_clients = set()
        self.active_clients = set()
        
        self.keep_alive = keep_alive
        self.receiver = receiver
        self.fuck_up = fuck_up
        self.sender = sender

        self.receiver.add_listener(self.on_connect,["client_connect"]) 
        self.receiver.add_listener(self.on_client_ready,["client_ready"]) 
        self.receiver.add_listener(self.on_keepalive,["keepalive"]) 

        threading.Timer(self.keep_alive, self.manage_clients).start() # Repeated task

    def on_connect(self, receiver, message):
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
                self.sender.send_message(routing_key=routing_key, message=json.loads(response))
                
            else:
                response = { 
			     "status" : "ok",
                             "keep-alive" : str(self.keep_alive),
			   }
                self.sender.send_message(routing_key=routing_key, message=json.loads(response))

            self.connected_clients[client_id] = dict()
            self.connected_clients[client_id]["last_seen"] = time.time()
            self.connected_clients[client_id]["client_name"] = client_name

            self.awaited_clients.add(client_id)
                
            
        except:
            print "Client connection error. Invalid message".format(message)

    def on_client_ready(self, receiver, message):
        try:
            ready_message = json.loads(message)
            client_id = int(ready_message["client_id"])

            if client_id in self.awaited_clients:
                self.connected_clients[client_id]["last_seen"] = time.time()
                
                print "Client {0} connected with id: {1}".format(
				self.connected_clients[client_id]["client_name"]),
				client_id
				)

                self.active_clients.add(client_id)
                self.awaited_clients.remove(client_id)

                print "
            else:
                print "Unknown client {0}. Message: {1}".format(ready_message["client_id"],message)
        except:
            print "Client ready error. Invalid message: {0}".format(message)

    def on_keepalive(self, receiver, message):
        keepalive_message = json.loads(message)
        client_id = keepalive_message["client_id"]

        if client_id not in set(self.connected_clients.keys()):
            print "Received keepalive from unknown client {0}".format(client_id)
            return
                
        if client_id in self.awaited_clients:
            self.active_clients.add(client_id)
            self.awaited_clients.remove(client_id)
            
        self.connected_clients[client_id]["last_seen"] = time.time()
        #self.connected_clients[client_id]["active"] = 1

    def manage_clients(self):
        threading.Timer(self.keep_alive, self.manage_clients).start()
        now = time.time()
        for client_id in connected_clients:
            last_seen = connected_clients[client_id]["last_seen"] 
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

    def await_clients(self, client_names):
        client_names = set(client_names)
        active_clients = set([self.connected_clients[i]["client_name"] for i in self.active_clients])

        while not client_names.issubset(active_clients):
            active_clients = set([self.connected_clients[i]["client_name"] for i in self.active_clients])
            time.sleep(1)
        
        return
