'''
Created on May 17, 2016

@author: alberto
'''


from datetime import datetime, tzinfo, timedelta
from dateutil import parser
from dateutil.tz import tzutc


ISO_8601_TIME_FORMAT = 'iso-8601'
UNIX_MILLIS_TIME_FORMAT = 'unix-millis'

# ALL IN UTC HERE!

class simple_utc(tzinfo):
    def tzname(self):
        return "UTC"
    def utcoffset(self, dt):
        return timedelta(0)

utc_epoch = datetime.utcfromtimestamp(0)
utc_zone = tzutc() 

def get_current_utc_millis():
    '''
    Return the UTC current time 
    '''
    utc_now = datetime.utcnow()
    return utc_datetime_to_millis(utc_now)

def iso8601_to_utc_millis(dtstr):
    '''
    dtstr is an utc iso8601 datetime string
    '''
    
    dt = parser.parse(__correct_datetime(dtstr))
    utc_dt = dt.astimezone(utc_zone).replace(tzinfo=None)
    return utc_datetime_to_millis(utc_dt)

def utc_millis_to_iso8601_datetime(ms):
    '''
    Return the ISO8601 datetime representation of the passed UTC millis
    '''
    
    dt = datetime.utcfromtimestamp(ms//1000).replace(microsecond=int(ms % 1000 * 1000))
    return dt.isoformat()[:-3] + 'Z'

def utc_millis_to_iso8601_duration(ms):
    '''
    Return the ISO8601 duration representation of the passed UTC delta millis
    '''
    
    td = timedelta(milliseconds=ms)
    minutes, seconds = divmod(td.seconds + td.days * 86400, 60)
    hours, minutes = divmod(minutes, 60)
    milliseconds = td.microseconds//1000
    return "P00-00-{:02}T{:02}:{:02}:{:02}.{:03}".format(td.days, hours, minutes, seconds, milliseconds)

def utc_datetime_to_millis(dt):
    '''
    Return the millis from the epoch time of the passed UTC datetime
    '''

    delta = dt - utc_epoch
    return long(delta.total_seconds() * 1000.0)

def utc_datetime_to_micros(dt):
    delta = dt - utc_epoch
    return long(delta.total_seconds() * 1000.0 + delta.microseconds / 1000.0)
    
# Jira sometimes reports the year 2000 as 0000
def __correct_datetime(dtstr):
    if dtstr[0] == '0':
        return "2" + dtstr[1:]
    else:
        return dtstr

def resolve_from_date(date):
    if date is None:
        # No lower precision than a day (first 10 chars of an iso8601 string
        date = utc_millis_to_iso8601_datetime(0)[0: 10]

    return date

def resolve_to_date(date):
    if date is None:
        now = get_current_utc_millis()
        date = utc_millis_to_iso8601_datetime(now)

    return date

'''
msa = get_current_utc_millis()
print(msa)
iso8601a = utc_millis_to_iso8601_datetime(msa)
print(iso8601a)
msb = iso8601_to_utc_millis(iso8601a)
print(msb)
iso8601b = utc_millis_to_iso8601_datetime(msb)
print(iso8601b)
'''