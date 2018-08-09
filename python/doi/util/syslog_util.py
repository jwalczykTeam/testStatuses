'''
Created on Feb 3, 2017

@author: alberto
'''

import socket
import ssl
import threading

#import logging.handlers.SysLogHandler

class SysLogClient(object):
    # priorities
    LOG_EMERG     = 0       #  system is unusable
    LOG_ALERT     = 1       #  action must be taken immediately
    LOG_CRIT      = 2       #  critical conditions
    LOG_ERR       = 3       #  error conditions
    LOG_WARNING   = 4       #  warning conditions
    LOG_NOTICE    = 5       #  normal but significant condition
    LOG_INFO      = 6       #  informational
    LOG_DEBUG     = 7       #  debug-level messages

    #  facility codes
    LOG_KERN      = 0       #  kernel messages
    LOG_USER      = 1       #  random user-level messages
    LOG_MAIL      = 2       #  mail system
    LOG_DAEMON    = 3       #  system daemons
    LOG_AUTH      = 4       #  security/authorization messages
    LOG_SYSLOG    = 5       #  messages generated internally by syslogd
    LOG_LPR       = 6       #  line printer subsystem
    LOG_NEWS      = 7       #  network news subsystem
    LOG_UUCP      = 8       #  UUCP subsystem
    LOG_CRON      = 9       #  clock daemon
    LOG_AUTHPRIV  = 10      #  security/authorization messages (private)
    LOG_FTP       = 11      #  FTP daemon

    #  other codes through 15 reserved for system use
    LOG_LOCAL0    = 16      #  reserved for local use
    LOG_LOCAL1    = 17      #  reserved for local use
    LOG_LOCAL2    = 18      #  reserved for local use
    LOG_LOCAL3    = 19      #  reserved for local use
    LOG_LOCAL4    = 20      #  reserved for local use
    LOG_LOCAL5    = 21      #  reserved for local use
    LOG_LOCAL6    = 22      #  reserved for local use
    LOG_LOCAL7    = 23      #  reserved for local use
    
    def __init__(self, host, port, 
                 facility=LOG_USER, 
                 private_key_file=None, cert_file=None, ca_certs_file=None):
        super(SysLogClient, self).__init__()
        self.facility = facility
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)    
        cert_reqs = ssl.CERT_REQUIRED if ca_certs_file is not None else ssl.CERT_NONE
        self.socket = ssl.wrap_socket(s, keyfile=private_key_file, certfile=cert_file, cert_reqs=cert_reqs, 
                                      ssl_version=ssl.PROTOCOL_SSLv23, ca_certs=ca_certs_file)
        self.socket.connect((host, port))
        self.lock = threading.Lock()

    def close(self):
        self.socket.close()

    def log(self, priority, message):
        with self.lock:
            # not sure if I need '\n' or '\000' at the end
            record = "<{}>{}\n".format(
                (self.facility << 3) | priority,
                message)
            self.socket.write(record)
            # print("SYSLOG SEND: {}".format(record))
