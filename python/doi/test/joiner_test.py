'''
Created on Jan 21, 2017

@author: alberto
'''

from doi.common import TaskParams
from doi.util import log_util
import logging
import time
import sys
import threading

logger = log_util.get_logger("astro")
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler(sys.stdout)
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)

start_time = time.time()
from doi.jobs.miner import joiner
stop_event = threading.Event()

params = TaskParams("", "astro@alberto", None, None, None, None, None)
joiner.join_data(params, None, stop_event)
end_time = time.time()
print("elapsed time = {} seconds".format(end_time - start_time))