'''
Created on Dec 12, 2016

@author: alberto
'''

import argparse
import os
import psutil
import time


def mon(process_name, log_file, interval=5):
    process = get_process_by_name(process_name)
    if process is None:
        print("Cannot find process: name='{}'".format(process_name)) 
        return
    
    print("Found process: cmdline='{}', pid={}".format(process.cmdline(), process.pid))
    
    try:
        n = 0
        with open(log_file, 'w') as f:
            f.write("time,cpu,mem\n")
            while  True:
                #timestamp = int(time.mktime(time.localtime()))
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S")                
                cpu = int(round(process.cpu_percent(interval=1)))
                mem = int(round(process.memory_info().rss / float(2 ** 20)))
                time.sleep(interval)
                f.write("{},{},{}\n".format(timestamp, cpu, mem))
                
                n += 1
                if n == 10:
                    f.flush()
                    n = 0
    except KeyboardInterrupt:
        print("Process interrupted, closing file ...")
    
            
def get_process_by_name(name):
    this_process_pid = os.getpid()
    name = name.lower()
    for process in psutil.process_iter():
        if process.pid != this_process_pid: 
            for arg in process.cmdline():
                if name in arg:
                    return process
    
    return None


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Log to a file cpu and memory usage of a specified process.')
    parser.add_argument('--name', help='process name')
    parser.add_argument('--file', help='the log file name')
    args = parser.parse_args()
    mon(args.name, args.file)
