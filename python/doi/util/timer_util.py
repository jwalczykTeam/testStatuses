'''
Created on Jan 5, 2017

@author: alberto
'''

import time

class Timer(object):
    def __init__(self, interval):
        self.interval = interval
        self.reset()

    def reset(self):
        self.start_time = None
        self.active = False    

    def activate(self):
        current_time = time.time()
        if self.start_time is None:
            self.start_time = current_time
            self.active = True
        else:
            if current_time - self.start_time > self.interval:
                self.start_time = None
            else:
                self.active = False
