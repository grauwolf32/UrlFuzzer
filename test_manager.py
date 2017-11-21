import pika
import threading
import json
import time
import signal
import settings 

from amqp_conn import Receiver, Sender
from client_manager import ClientManager
from client import Client

cred = pika.PlainCredentials(settings.LOCAL_USER, settings.LOCAL_PASSWD)
conn = pika.BlockingConnection(pika.ConnectionParameters(credentials=cred,host=settings.LOCAL_IP))


def main():
    sender = Sender(conn,settings.LOCAL_EXCHANGE)
    receiver = Receiver(conn = conn, exch = settings.LOCAL_EXCHANGE, queue = settings.LOCAL_QUEUE)
    receiver.start()
    client_manager = ClientManager(receiver, sender)
    client_manager.await_clients(["Python"])

if __name__ == '__main__':
    main() 
