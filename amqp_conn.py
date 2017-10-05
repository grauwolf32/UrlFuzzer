import pika
import threading
import json

class Receiver(threading.Thread):
    def __init__(self,conn,exch,queue,bindings,cb_func,exch_type="direct",pc=1):
        super(Receiver,self).__init__()      

        self.conn = conn
        self.exch = exch
        self.queue = queue

        self.ch = self.conn.channel()
        self.ch.exchange_declare(exchange=self.exch,exchange_type=exch_type)
        self.ch.queue_declare(queue=self.queue)

        self.ch.basic_qos(prefetch_count=pc)
        self.cb_func = cb_func
        for binding in bindings:
            self.ch.queue_bind(exchange=self.exch, queue=self.queue, routing_key=binding)

        self.ch.basic_consume(self.callback, queue=self.queue)

    def callback(self, ch, method, properties, body):
        return self.cb_func(self,method.delivery_tag,body)

    def run(self):
        self.ch.start_consuming()


class Sender():
    def __init__(self,conn,exch,exch_type="direct"):
        self.conn = conn
        self.exch = exch

        self.ch = self.conn.channel()
        self.ch.exchange_declare(exchange=self.exch,exchange_type=exch_type)

    def send_message(self, routing_key, message):
        self.ch.basic_publish(exchange=self.exch, routing_key=routing_key, body=message)
 
