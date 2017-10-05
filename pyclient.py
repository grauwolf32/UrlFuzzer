import pika
import threading
import json

from . import settings
from . import amqp_conn.Receiver, amqp_conn.Sender

cred = pika.PlainCredentials(settings.LOCAL_USER, settings.LOCAL_PASSWD)
conn = pika.BlockingConnection(pika.ConnectionParameters(credentials=credentials,host=settings.LOCAL_IP))

class PyClient():
    def __init__(self, conn):
        self.conn = conn
        self.sender = Sender(conn,settings.LOCAL_EXCHANGE)
        self.receiver = 

    def process_result(self, receiver, message):
        
    def do_task(task):
        pass
