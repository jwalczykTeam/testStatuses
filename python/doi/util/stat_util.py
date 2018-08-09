'''
Created on Feb 6, 2017

@author: alberto
'''

import sys
import time
 
class Throughput(object):
    def __init__(self, interval):
        self.interval = interval
        self.total_count = 0
        self.count_per_interval = 0
        self.min_count_per_interval = sys.maxint
        self.max_count_per_interval = 0
        self.avg_count_per_interval = 0
        self.sample_count = 0
        self.last_time = None
        
    def increase(self):
        self.total_count += 1
        current_time = time.time()
        if self.last_time is None:
            self.last_time = current_time

        if current_time - self.last_time < self.interval: 
            self.count_per_interval += 1
        else:
            self.sample_count += 1
            
            # The interval has passed: take min, max and avg values
            if self.count_per_interval < self.min_count_per_interval:
                self.min_count_per_interval = self.count_per_interval
            
            if self.count_per_interval > self.max_count_per_interval:
                self.max_count_per_interval = self.count_per_interval

            self.avg_count_per_interval += (self.count_per_interval - self.avg_count_per_interval) / float(self.sample_count)
            self.last_time = current_time
            self.count_per_interval = 0
