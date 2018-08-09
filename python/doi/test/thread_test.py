'''
Created on Sep 27, 2016

@author: alberto
'''


import threading
import time


class JobThread(threading.Thread):
    def __init__(self):
        super(JobThread, self).__init__()

    def run(self):
        stage_change_thread = JobStageChangeThread()
        stage_change_thread.daemon = True
        stage_change_thread.start()

        print("parent thread started")
        time.sleep(5)
        print("parent thread ended")
        
class JobStageChangeThread(threading.Thread):
    def __init__(self):
        super(JobStageChangeThread, self).__init__()

    def run(self):
        print("child thread started")
        while True:
            print("child thread cycle started")
            time.sleep(5)
            print("child thread cycle ended")
        print("child thread ended")

try:
    job_thread = JobThread()
    #job_thread.daemon = True
    job_thread.start()
except Exception as e:
    print(e)
