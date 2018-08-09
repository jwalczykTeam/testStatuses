'''
Created on Dec 21, 2016

@author: alberto
'''

from doi.util import slack_util


class Service(object):
    def __init__(self, config, logger):
        super(Service, self).__init__() 
        self.config = config
        self.logger = logger
        
        self.group_id = self.config['group_id']
        self.error_retry_interval = self.config['error_retry_interval']
        self.slack_webhook_url = self.config['slack_webhook_url']        
    
    def open(self):
        self.logger.info("group_id='%s'", self.group_id)        
        self.logger.info("error_retry_interval=%d", self.error_retry_interval)
        self.logger.info("slack_webhook_url=%s", "present" if self.slack_webhook_url else "absent")
        
        self.slack_client = None
        if self.slack_webhook_url:
            self.slack_client = slack_util.SlackClient(self.slack_webhook_url)

    def close(self):
        if self.slack_client is not None:
            self.slack_client.close()
            self.slack_client = None

    def notify(self, msg, *args):
        self.logger.info(msg, *args)
        if self.slack_client is not None:
            self.slack_client.send(msg, *args)                

    def notify_exception(self, msg, *args, **kwargs):
        self.logger.exception(msg, *args)
        notify = kwargs.get('notify', False) 
        if notify:
            if self.slack_client is not None:
                self.slack_client.send_exception(msg, *args)                
