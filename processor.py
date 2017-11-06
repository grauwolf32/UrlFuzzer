import pika
import threading
import json
import time
import signal
import settings 

from amqp_conn import Receiver, Sender

cc_cred = pika.PlainCredentials(settings.CC_USER, settings.CC_PASSWD)
cc_conn = pika.BlockingConnection(pika.ConnectionParameters(credentials=cc_cred,host=settings.CC_IP))

local_cred = pika.PlainCredentials(settings.LOCAL_USER, settings.LOCAL_PASSWD)
local_conn = pika.BlockingConnection(pika.ConnectionParameters(credentials=local_cred,host=settings.LOCAL_IP))

class ClientsNotReady(Exception):
    pass

class Processor():
    def __init__(self, cc_conn,local_conn, connected_clients):
        self.reported_ids = set()
        self.task_results = dict()
        self.tasks = dict()

        self.connected_clients = connected_clients
        self.n_clients = len(connected_clients)
        self.heartbeat = dict()

        self.time_start = time.time()
        self.heartbeat["supervisor"] = self.time_start

        for client in self.connected_clients:
           self.heartbeat[client] = self.time_start

        self.cc_conn = cc_conn
        self.local_conn = local_conn
        
        self.job_status = self.connected_clients
        self.cc_sender = Sender(cc_conn,settings.CC_EXCHANGE)
        self.cc_sender.send_message(routing_key="task_result", message="huipizda")

        self.local_sender = Sender(local_conn,settings.LOCAL_EXCHANGE)

        self.cc_receiver = Receiver(conn = cc_conn,
                                       exch = settings.CC_EXCHANGE,
                                            queue = settings.CC_QUEUE)

        self.cc_receiver.add_listener(self.dispatch_task,["task"])

        self.local_receiver = Receiver(conn = local_conn,
                                           exch = settings.LOCAL_EXCHANGE,
                                               queue = settings.LOCAL_QUEUE)

        self.local_receiver.add_listener(self.dispatch_result,["result"])
        self.local_receiver.add_listener(self.supervisor,["job-done","keep-alive","client-id"])
    
    def start(self):
        self.cc_receiver.start()
        self.local_receiver.start()

    def stop(self):
        if self.cc_receiver.ch.is_open:
            self.cc_receiver.stop_consuming()
        self.cc_receiver.join()
        
        if self.local_receiver.ch.is_open:
            self.local_receiver.stop_consuming()
        self.local_receiver.join()

    def supervisor(self, receiver, method, message): #TODO Refactoring !!!!
        message = json.loads(message)
        client = message["client"]

        if method.routing_key == "job-done":
            try:
                self.job_status.remove(client)
            except:
                pass

            if len(self.job_status) == 0:
                self.send_report()
            return 

        if method.routing_key == "keep-alive":
            print "Got keep-alive from {0}".format(client)
            current_time = time.time()
            self.heartbeat[client] = current_time
            return

        if method.routing_key == "client-id":
            return
        
    def send_report(self):
        self.reported_ids.clear()
        self.task_results.clear()
        self.tasks.clear()

    def dispatch_result(self,receiver,method, result):
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

        if key_error == False:
            for lang in self.task_results[task_id]:
                for key in res["result"]:
                    if self.task_results[task_id][lang][key] != res["result"][key]:
                        self.reported_ids.add(task_id)
                        self.task_results[task_id][task_lang] = res["result"]
        else:
            self.task_results[task_id][task_lang] = res["result"]
        return

    def dispatch_task(self, receiver, method, message):
        print "Task dipatched\n{0}".format(message)

        task = json.loads(message)
        self.tasks = task["tasks"]

        for task_id in task["reported_ids"]:
            self.reported_ids.add(task_id)
    
        for task_id in tasks:
            self.local_sender.basic_publish(
                                routing_key = "task", 
				    exchange = settings.LOCAL_EXCHANGE,
                                        body = '{"{0}":"{1}"}'.format(task_id,self.tasks[task_id]))

        return


invited_clients = set(["python"])
connected_clients = set(["python"])

def invite_clients(receiver, delivery_tag, message):
    client = json.loads(message)["client"]
    if client in invited_clients:
        print "Client {0} accept the invitation!".format(client)
        invited_clients.remove(client)
        if len(invited_clients) == 0:
            receiver.ch.stop_consuming()
            print "All clients have been connected!"


def main():
    invite_receiver = Receiver(conn=local_conn,
			           exch=settings.LOCAL_EXCHANGE,
                                       queue=settings.LOCAL_QUEUE)

    invite_receiver.add_listener(invite_clients,["client-id"])
    invite_receiver.start()
    invite_receiver.join(180)

    if len(invited_clients) > 0:
        raise ClientsNotReady
    
    p = Processor(cc_conn=cc_conn,local_conn = local_conn,connected_clients=connected_clients)
    p.start()

    #stub_sender = Sender(local_conn,settings.LOCAL_EXCHANGE)
    #task = dict()
    #task["task_id"] = "12345"
    #task["task"] = "http://example.com"
    print "Stub"
    print p.tasks
if __name__=="__main__":
    main()
    
        
#def signal_handler(signal, frame):
#    print 'You pressed Ctrl+C!'
#    sys.exit(0)

#signal.signal(signal.SIGINT, signal_handler)
# ps -ax  | grep processor.py | cut -d " " -f1| xargs -I {} kill -9 {}


        

