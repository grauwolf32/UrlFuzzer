import pika
import threading
import json

class Receiver(threading.Thread):
    def __init__(self,conn,exch,queue,exch_type="direct",prefetch_count=1):
        super(threading.Thread,self).__init__()      

        self.conn = conn
        self.exch = exch
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
  
        if len(listeners) == 0:
            return

        for listener in listeners:   
            try:
                listener(self, method, body)
            except:
                pass #TODO Add logs

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
    def __init__(self, conn,exch, exch_type="direct"):
        self.conn = conn
        self.exch = exch

        self.ch = self.conn.channel()
        self.ch.exchange_declare(exchange=self.exch,exchange_type=exch_type)

    def send_message(self, routing_key, message):
        self.ch.basic_publish(exchange=self.exch, routing_key=routing_key, body=message)
 
