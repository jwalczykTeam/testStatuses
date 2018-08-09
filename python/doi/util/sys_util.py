'''
Created on Aug 19, 2016

@author: alberto
'''


import os
import psutil


def used_current_process_memory():
    # Return current process memory usage in MB
    process = psutil.Process(os.getpid())
    mem = process.memory_info().rss / float(2 ** 20)
    return int(round(mem))

def used_virtual_memory():
    # Return virtual memory usage in MB
    mem = psutil.virtual_memory().used / float(2 ** 20)
    return int(round(mem)) 

def used_swap_memory():
    # Return swap memory usage in MB
    mem = psutil.swap_memory().used / float(2 ** 20)
    return int(round(mem)) 
    
def current_process_cpu_percent():
    # Return current process cpu percent
    process = psutil.Process(os.getpid())
    cpu = process.cpu_percent(interval=1)
    return int(round(cpu)) 
    