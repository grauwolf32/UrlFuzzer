import threading
import settings
import random
import time
import pika
import json

from amqp_conn import *

class Client():
    def __init__(self, binder, queue, client_name="", client_id=random.randint(0,10**6)):
        self.binder = binder
        self.queue = queue

        self.receiver = Receiver(self.binder, self.queue)
        self.sender = Sender(self.binder)
        self.keep_alive_ = Sender(self.binder)

        self.client_name = client_name
        self.client_id = client_id

        self.receiver.start()
        print "client id: {0}".format(client_id)

    def accept_connection(self, receiver, method, body):
            print "Accept connection"
            if self.connection_stage != 0:
                print "Connection has been accepted! Duplicate."
                return 

            routing_key = "".join(("client_",str(self.client_id))) # TODO Do it more smart

            if method.routing_key != routing_key:
                return 

            self.connection_stage = 1
            message = json.loads(body)
            try:
                status = message["status"]
                if status == "ok":
                    pass

                if status == "new_id":
                    self.client_id = int(message["new_id"])

                self.keep_alive = int(message["keep-alive"])
                threading.Timer(self.keep_alive, self.keepalive).start()

            except:
                print "Wrong message format:\n{0}".format(message)
        
    def connect(self, timeout=10.0, delay = 0.5):
        self.connection_stage = 0

        def stage_0(self):
            if self.connection_stage != 0:
                print '''Current connection stage {0}.\n 
                         You need to reset stage counter before going to stage 0.'''.format(self.connection_stage)
                return

            request = {
                 "client_id" : str(self.client_id),
                 "client_name" : self.client_name,
	    }

            self.sender.send_message(routing_key="client_connect", message=json.dumps(request))

        def stage_2(self):
            if self.connection_stage != 1:
                print '''Current connection stage {0}.\n 
                         You can go to stage 2 only after stage 1.'''.format(self.connection_stage)
                return

            self.connection_stage = 2
            response = {
			"client_id" : str(self.client_id),
            }

            self.sender.send_message(routing_key="client_ready", message=json.dumps(response))

        stage_0(self)

        routing_key = "".join(("client_",str(self.client_id)))
        self.receiver.add_listener(self.accept_connection,[routing_key])
        
        while self.connection_stage != 1 and timeout > 0:
            time.sleep(delay)
            timeout -= delay

        if self.connection_stage == 1:
            stage_2(self)
            self.connection_stage = 2 

            return True

        else:
            print "Connection timeout!"
            return False

    def keepalive(self):
        if self.keep_alive == None:
            print "Keep-alive time not defined!"
            return

        threading.Timer(self.keep_alive, self.keepalive).start() # Repeated task
        response = {
		"client_id" : str(self.client_id),
        }

        self.keep_alive_.send_message(routing_key="keepalive", message=json.dumps(response))

    def kill(self):
        if self.receiver.ch.is_open:
            self.receiver.stop_consuming()
        self.receiver.join()
            
                        
        
        
            
         
