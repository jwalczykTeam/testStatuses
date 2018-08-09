'''
Created on Mar 2, 2017

@author: alberto
'''

import threading
import time

class RateLimit(object):
    DEFAULT_MAX_COUNT = 5
    DEFAULT_BLOCKED_INTERVAL = 30
    
    class Rate(object):
        def __init__(self):
            self.last_time = time.time()
            self.count = 1
            
        def increment(self):
            self.last_time = time.time()
            self.count += 1

    def __init__(self, max_count=None, blocked_interval=None):
        if max_count is None:
            max_count = RateLimit.DEFAULT_MAX_COUNT

        if blocked_interval is None:
            blocked_interval = RateLimit.DEFAULT_BLOCKED_INTERVAL
            
        self.max_count = max_count  
        self.blocked_interval = blocked_interval
        self.rate_by_source = {}
        self.lock = threading.Lock()

    def increment(self, source):
        with self.lock:
            rate = self.rate_by_source.get(source)
            if rate is None:
                self.rate_by_source[source] = RateLimit.Rate()
            else:
                rate.increment()

    def can_proceed(self, source):
        with self.lock:
            rate = self.rate_by_source.get(source)
            # if this source does not exists: OK
            if rate is None:
                return True
            # if this source rate count is less than max_count: OK
            if rate.count < self.max_count:
                return True

            # if this source count is greater than max_count 
            # and blocked interval did not expire 
            current_time = time.time()
            last_time = rate.last_time
            if current_time - last_time < self.blocked_interval:
                return False

            # remove rate record for this source and return OK
            del self.rate_by_source[source]
            return True
