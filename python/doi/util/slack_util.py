'''
Created on Dec 5, 2016

@author: alberto
'''

from doi.util import rest
import json
import traceback


class SlackClient(object):
    DEFAULT_USER_NAME = "DevOps Insights"
    DEFAULT_ICON_URL = "https://lh3.googleusercontent.com/-SMj9h1kmLN0/VYn8ggydjSI/AAAAAAAAALI/G12DlyEGgAY/decorative-logo.png"
    
    def __init__(self, webhook_url, config=None):
        self.webhook_url = webhook_url
        self.config = config
        self.rest_client = rest.RestClient()
    
    def send(self, msg, *args):
        if args:
            msg = msg % args

        data = {
            "username": SlackClient.DEFAULT_USER_NAME,
            "as_user": False,
            "icon_url": SlackClient.DEFAULT_ICON_URL,
            "pretty": 1,            
            "text": msg
        }
        
        # override default data config fields 
        if self.config is not None:
            data.update(self.config)
        

        try:
            self.rest_client.post(
                self.webhook_url, json.dumps(data),
                {"Content-Type": "application/json"})
        except:
            print "Slack url invalid :{}".format(self.webhook_url)

    def send_exception(self, msg, *args):
        if args:
            msg = msg % args
        
        stack_trace = traceback.format_exc()
        self.send(msg + '\n' + stack_trace)
        
    def close(self):
        self.rest_client.close()