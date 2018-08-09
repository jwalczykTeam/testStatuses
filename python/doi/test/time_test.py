'''
Created on Feb 1, 2017

@author: alberto
'''

from doi.util import time_util
from datetime import datetime

utc_now = datetime.utcnow()
print(utc_now)
print(utc_now.isoformat())

time = time_util.get_current_utc_millis()
print(time)
print(time_util.utc_millis_to_iso8601_datetime(time))

ms = 123 + 1 * 1000 + 2 * 60 * 1000
print(time_util.utc_millis_to_iso8601_duration(ms))