import pika
import threading
import settings
import json
import time

cred = pika.PlainCredentials(settings.CC_USER, settings.CC_PASSWD)
conn = pika.BlockingConnection(pika.ConnectionParameters(credentials=cred,host=settings.LOCAL_IP))

class Server():
    def __init__(self,conn):
        self.conn = conn
        self.sender = Sender(conn,settings.CC_EXCHANGE)
        self.receiver = Receiver(conn,settings.CC_EXCHANGE,settings.CC_QUEUE)
        self.receiver.add_listener(self.on_result,["task_result"])
        self.task_counter = 0

    def start():
        self.receiver.start()
    
    def on_result(self, receiver, message):
        print message
    
    
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


def main()
    s = Server(conn)
    s.start()
    tasks = ["http://example.com","https://google.com"]
    s.send_tasks(tasks,list(xrange(0,len(tasks)))

if __name__== "__main__":
    main()  
          
    

    
