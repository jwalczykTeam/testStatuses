'''
Created on Sep 1, 2016

@author: alberto
'''

class Message(object):
    @property
    def topic(self):
        raise NotImplementedError("topic")
        
    @property
    def partition(self):
        raise NotImplementedError("partition")

    @property
    def offset(self):
        raise NotImplementedError("offset")
    
    @property
    def key(self):
        raise NotImplementedError("key")

    @property
    def value(self):
        raise NotImplementedError("value")


class Producer(object):
    def send(self, topic, value):
        raise NotImplementedError("send")

    def close(self):
        raise NotImplementedError("close")


class Consumer(object):
    def subscribe(self, topics):
        raise NotImplementedError("subscribe")
        
    def messages(self):
        raise NotImplementedError("messages")
    
    def close(self):
        raise NotImplementedError("close")
    

class Manager(object):
    def ensure_topic_exists(self, topic_name):
        raise NotImplementedError("ensure_topic_exists")

    def delete_topic(self, topic_name):
        raise NotImplementedError("delete_topic")

    def close(self):
        raise NotImplementedError("close")

