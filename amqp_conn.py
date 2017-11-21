import pika
import threading
import json

class Connection()
    def __init__(self,user,password,host):
        self.credenials = pika.PlainCredentials(user, password)
        self.host = host

    def get_connection(self):
        return pika.BlockingConnection(pika.ConnectionParameters(credentials=self.credenials, host=self.host)) 

class Connection():
    def __init__(self, conn, exchange):
        self.connection = conn
        self.exchange = exchange

class Receiver(threading.Thread):
    def __init__(self,conn, queue, exch_type="direct",prefetch_count=1):
        super(Receiver, self).__init__()      

        self.conn = conn.connection
        self.exch = conn.exchange
        self.queue = queue
        self.listeners = dict()

        self.ch = self.conn.channel()
        self.ch.exchange_declare(exchange=self.exch,exchange_type=exch_type)
        self.ch.queue_declare(queue=self.queue)

        self.ch.basic_qos(prefetch_count=prefetch_count)
        self.ch.basic_consume(self.callback, queue=self.queue)

    def callback(self, ch, method, properties, body):
        listeners = []
        try:
            listeners = self.listeners[method.routing_key]
        except KeyError:
            pass

        print body

        for listener in listeners:   
            try:
                listener(self, method, body)
            except:
                print "Error while calling the listener {0}".format(listener)

        self.ch.basic_ack(delivery_tag = method.delivery_tag)
        
        return 

    def add_listener(self, callback, routing_keys):
        existing_bindings = set(self.listeners.keys())

        for routing_key in routing_keys:
            if routing_key in existing_bindings:
                self.listeners[routing_key].add(callback)
            else:
                self.listeners[routing_key] = [callback]
                self.ch.queue_bind(exchange=self.exch, queue=self.queue, routing_key=routing_key)
        return

    def run(self):
        self.ch.start_consuming()


class Sender():
    def __init__(self, conn, exch_type="direct"):
        self.conn = conn.connection
        self.exch = conn.exchange

        self.ch = self.conn.channel()
        self.ch.exchange_declare(exchange=self.exch,exchange_type=exch_type)

    def send_message(self, routing_key, message):
        self.ch.basic_publish(exchange=self.exch, routing_key=routing_key, body=message)
 
