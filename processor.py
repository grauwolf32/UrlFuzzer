import pika
import threading
import json
import time
import settings 

from amqp_conn import Receiver, Sender

cc_cred = pika.PlainCredentials(settings.CC_USER, settings.CC_PASSWD)
cc_conn = pika.BlockingConnection(pika.ConnectionParameters(credentials=cc_cred,host=settings.CC_IP))

local_cred = pika.PlainCredentials(settings.LOCAL_USER, settings.LOCAL_PASSWD)
local_conn = pika.BlockingConnection(pika.ConnectionParameters(credentials=local_cred,host=settings.LOCAL_IP))

class ClientsNotReady(Exception):
    pass

class Processor():
    def __init__(self, cc_conn,local_conn,connected_clients):
        self.reported_ids = set()
        self.task_results = dict()
        self.tasks = dict()
        self.task_callbacks = []

        self.connected_clients = set(connected_clients)
        self.n_clients = len(connected_clients)
        self.heartbeat = dict()

        self.time_start = time.time()
        self.heartbeat["supervisor"] = self.time_start
        for client in self.connected_clients:
           self.heartbeat[client] = self.time_start

        self.cc_conn = cc_conn
        self.local_conn = local_conn
        
        self.job_status = set(self.invited_clients)
        self.cc_sender = Sender(cc_conn,settings.CC_EXCHANGE)
        self.local_sender = Sender(local_conn,settings.LOCAL_EXCHANGE)

        self.cc_receiver = Receiver(cc_conn,settings.CC_EXCHANGE,settings.CC_QUEUE,["task"],dispatch_task)
        self.local_receiver = Receiver(local_conn,settings.LOCAL_EXCHANGE,settings.LOCAL_QUEUE,["result"],dispatch_result)
        self.local_supervisor = Receiver(local_conn,settings.LOCAL_EXCHANGE,settings.LOCAL_QUEUE,["job_done","keep-alive"],supervisor)

    def supervisor(self, receiver, message):
        message = json.loads(message)
        client = message["client_name"]
        message_type = message["message_type"]
        
        if message_type == "job_done":
            try:
                self.job_status.remove(client)
            except:
                pass

            if len(self.job_status) == 0:
                self.send_report()
            return 

        if message_type == "keep-alive":
            current_time = time.time()
            self.heartbeat[client] = current_time
        
    def send_report(self):
        self.reported_ids.clear()
        self.task_results.clear()
        self.tasks.clear()
    
    def invite_clients(self, receiver, message):
        client = json.loads(message)["client_name"]
        self.connected_clients.add(client)

        if self.invited_clients.issubset(self.connected_clients):
            receiver.ch.stop_consuming()

    def dispatch_result(self,receiver,result):
        res = json.loads(result)
        task_id = res["task_id"]
        task_lang = res["lang"]
        key_error = False
    
        try:
            self.task_results[task_id]
        except KeyError:
            key_error = True
            self.task_results[task_id] = dict()

        if res["error"] == "1":
            self.reported_ids.add(task_id)
            try:
                task = self.tasks[task_id]
            except KeyError:
                task = "task not found!"

            err_report["error"] = "1"
            err_report["task"] = task

            self.task_results[task_id][task_lang] = json.dumps(err_report)
            return

        if task_id in reported_ids:
            self.task_results[task_id][task_lang] = res["result"]
            return

        if key_error != True:
            for lang in self.task_results[task_id]:
                for key in res["result"]:
                    if self.task_results[task_id][lang][key] != res["result"][key]:
                        self.reported_ids.add(task_id)
                        self.task_results[task_id][task_lang] = res["result"]
        else:
            self.task_results[task_id][task_lang] = res["result"]
        return

    def dispatch_task(self, receiver, task):
        task_ = json.loads(task)
        self.tasks = task_["tasks"]

        for task_id in task_["reported_ids"]:
            self.reported_ids.add(task_id)
    
        for task_id in tasks:
            self.local_sender.basic_publish(
				exchange=settings.LOCAL_EXCHANGE,
			        routing_key="task", 
                                body=self.tasks[task_id])

        return

def main():
    print "Stub"
    #invited_clients = ["python","java","ruby","go"]
    #invite_receiver = Receiver(conn=self.local_conn,
    #invite_receiver.start()
    #invite_receiver.join(60)

    #if  not self.invited_client.issubset(self.connected_clients):
    #    raise ClientsNotReady

    #connected_clients = 
    #proc = Processor(cc_conn,local_conn,invited_clients)
    #proc.start()

if __name__=="__main__":
    main()
    
        
#def signal_handler(signal, frame):
#    print 'You pressed Ctrl+C!'
#    sys.exit(0)

#signal.signal(signal.SIGINT, signal_handler)


        

