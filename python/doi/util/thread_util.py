'''
Created on Dec 18, 2016

@author: alberto
'''

import threading


class StoppableThread(threading.Thread):
    """
    Thread class with a stop() method. 
    The thread itself has to check regularly for the stop_requested condition.
    """

    def __init__(self):
        super(StoppableThread, self).__init__()
        self._stop_requested = threading.Event()
        self._stopped = threading.Event()

    def stop(self, timeout=None):
        self._stop_requested.set()
        self._stopped.wait(timeout)

    def stop_requested(self):
        return self._stop_requested.is_set()
            
    def set_stopped(self):
        self._stopped.set()

    def stopped(self):
        return self._stopped.is_set()

        