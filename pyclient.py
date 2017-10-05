import pika
import threading
import settings
import json

from amqp_conn import Receiver, Sender

cred = pika.PlainCredentials(settings.LOCAL_USER, settings.LOCAL_PASSWD)
conn = pika.BlockingConnection(pika.ConnectionParameters(credentials=cred,host=settings.LOCAL_IP))

class PyClient():
    def __init__(self, conn):
        self.conn = conn
        self.sender = Sender(conn,settings.LOCAL_EXCHANGE)
        #self.receiver = Receiver(
        self.sender.send_message(routing_key="client_identification",message='{"client":"python"}')

    def process_result(self, receiver, delivery_tag, message):
        pass
    def do_task(task):
        pass

def main():
    pycl = PyClient(conn)

if __name__== "__main__":
    main()
