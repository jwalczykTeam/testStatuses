'''
Created on Dec 5, 2016

@author: alberto
'''

from doi.util import time_util
from doi.util.syslog_util import SysLogClient
import json
import logging
import traceback
import re, copy


class AstroLogger(logging.Logger):
    def __init__(self, logger):
        super(AstroLogger, self).__init__(logger.name, logger.level)
        self.logger = logger
        self.slack_client = None
        self.syslog_client = None
        
    def enable_slack_notification(self, slack_webhook_url):
        if slack_webhook_url:
            from doi.util.slack_util import SlackClient
            self.slack_client = SlackClient(slack_webhook_url)

    def enable_syslog_notification(self, config):
        host = config['host']
        port = config['port']
        facility = config.get('facility', SysLogClient.LOG_USER)
        private_key_file = config.get('private_key_file', SysLogClient.LOG_USER)
        cert_file = config.get('cert_file', SysLogClient.LOG_USER)
        ca_certs_file = config.get('ca_certs_file', SysLogClient.LOG_USER)
        self.syslog_client = SysLogClient(
            host, port, facility,
            private_key_file, cert_file, ca_certs_file)

    def log(self, level, msg, *args, **kwargs):
        notification_requested = kwargs.pop('notify', False)         
        self.logger.log(level, msg, *args, **kwargs)
        if notification_requested:
            self._notify(msg, *args, **kwargs)
                             
    def debug(self, msg, *args, **kwargs):
        self.log(logging.DEBUG, msg, *args, **kwargs)
        
    def info(self, msg, *args, **kwargs):
        self.log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.log(logging.WARNING, msg, *args, **kwargs)
        
    warn = warning

    def error(self, msg, *args, **kwargs):
        self.log(logging.ERROR, msg, *args, **kwargs)
        
    def exception(self, msg, *args, **kwargs):
        notification_requested = kwargs.pop('notify', False)         
        self.logger.error(msg, *args, **kwargs)
        stack_trace = traceback.format_exc()        
        self.logger.error(stack_trace.encode('string-escape'))
        if notification_requested:
            self._notify_exception(msg, *args, **kwargs)
        
    def critical(self, msg, *args, **kwargs):
        self.log(logging.CRITICAL, msg, *args, **kwargs)

    def _notify(self, msg, *args, **kwargs):
        if self.slack_client is not None:
            self.slack_client.send(msg, *args)                

    def _notify_exception(self, msg, *args, **kwargs):
        if self.slack_client is not None:
            self.slack_client.send_exception(msg, *args)
        
    fatal = critical
    
    def addHandler(self, handler):
        self.logger.addHandler(handler)

    def setLevel(self, level):
        self.logger.setLevel(level)
    
#
#   High level logging
#
    _EVENT_WEB_SERVICE_LOGIN_SUCCEEDED = "Web Service Login Succeeded"
    _EVENT_WEB_SERVICE_LOGIN_FAILED = "Web Service Login Failed"
    _EVENT_WEB_SERVICE_LOGOUT = "Web Service Logout"
    _APPLICATION_ID = "devops.ng.bluemix.net"
    _COMPONENT_ID = "astro"
    
    '''
    Examples: 
    <182>1 2017-01-18T20:16:12.163Z devops.ng.bluemix.net otc-tiam - - - {"res":"\/identity\/v1\/whoami","src":"169.46.120.117","url":"https:\/\/tiam.ng.bluemix.net\/identity\/v1\/whoami","oper":"GET","event":"Web Service Login Succeeded","usrName":"pipeline"}
    '''
       
    def log_web_service_auth_succeeded(
            self, method, url, user_name, 
            source_addr=None, source_port=None, 
            dest_addr=None, dest_port=None):
        message = self.encode_message(
            AstroLogger._EVENT_WEB_SERVICE_LOGIN_SUCCEEDED,
            method, url, user_name,
            source_addr, source_port,
            dest_addr, dest_port)
        self.info(message)
        if self.syslog_client is not None:
            self.syslog_client.log(SysLogClient.LOG_INFO, message)
            
    def log_web_service_auth_failed(
            self, method, url, user_name, 
            source_addr=None, source_port=None, 
            dest_addr=None, dest_port=None):
        message = self.encode_message(
            AstroLogger._EVENT_WEB_SERVICE_LOGIN_FAILED,
            method, url, user_name,
            source_addr, source_port,
            dest_addr, dest_port)
        self.error(message)
        if self.syslog_client is not None:
            self.syslog_client.log(SysLogClient.LOG_ERR, message)

    def encode_message(self, 
            event, method, url, user_name,
            source_addr=None, source_port=None, 
            dest_addr=None, dest_port=None):
        payload = {
            'event': event,
            "oper": method,
            "url": url
        }

        if user_name is not None:
            payload['usrName'] = user_name
        
        if source_addr is not None:
            payload['src'] = source_addr

        if source_port is not None:
            payload['srcPort'] = source_port

        if dest_addr is not None:
            payload['dst'] = dest_addr

        if dest_port is not None:
            payload['dstPort'] = dest_port
        
        current_time = time_util.get_current_utc_millis()
        iso_current_time = time_util.utc_millis_to_iso8601_datetime(current_time)
        message = "1 {} {} {} - - - {}".format(
            iso_current_time,
            AstroLogger._APPLICATION_ID,
            AstroLogger._COMPONENT_ID,
            json.dumps(payload))        
        return message    


def get_logger(name=None):
    logger = logging.getLogger(name)
    return AstroLogger(logger)


def mask_sensitive_info(adict):
    '''
    Takes as input a dictionary and returns a new one where
    sensitive information are masked: e.g. password, api keys, etc.
    '''
    mask_with = '********'
    p = re.compile(r"(api.*key|passw([ord]+)?|pwd|secret|access.*token|token|license|web.*hook|http_auth)", re.IGNORECASE)
    maskd = copy.deepcopy(adict)
    for k in maskd.keys():
        if p.search(k):
            maskd[k] = mask_with
        elif type(maskd[k]) == dict:
            maskd[k] = mask_sensitive_info(maskd[k])
        elif type(maskd[k]) == list:
            for n, elem in enumerate(maskd[k]):
                if type(elem) == dict:
                    maskd[k][n] = mask_sensitive_info(elem)

    return maskd
