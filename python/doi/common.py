'''
Created on Sep 6, 2016

@author: alberto
'''

# Service States
class ServiceState(object):
    STARTING = 'starting'
    STARTED = 'started'
    STOPPING = 'stopping'
    STOPPED = 'stopped'


# Member States
class MemberState(object):
    JOINED = 'joined'
    LEFT = 'left'
    DEAD = 'dead'


# Task Method
class TaskMethod(object):
    CREATE = 'create'
    UPDATE = 'update'
    DELETE = 'delete'


# Task States
class TaskState(object):
    '''task is waiting for a worker to be assigned'''
    WAITING = 'waiting'
    
    '''task is ready to start on an assigned worker'''
    READY = 'ready'
    
    '''task is stopped'''
    STOPPED = 'stopped'

    '''task running on worker'''
    RUNNING = 'running'
    
    '''task ended successfully'''
    SUCCESSFUL = 'successful'
    
    '''task ended with failure'''    
    FAILED = 'failed'

    '''task ended with a permanent failure'''    
    PERMANENTLY_FAILED = 'permanently_failed'


# Parameters passed to the task function
class TaskParams(object):
    def __init__(self, job_id, job_name, task_id, method, rev, 
                 input_, previous_input):
        self.job_id = job_id
        self.job_name = job_name
        self.task_id = task_id
        self.method = method
        self.rev = rev
        self.input = input_
        self.previous_input = previous_input


class Error(object):
    def __init__(self, type_, title, status, detail, **kwargs):
        super(Error, self).__init__()
        self.type = type_
        self.title = title
        self.status = status
        self.detail = detail
        self.kwargs = kwargs
        
    @staticmethod
    def decode(obj):
        if obj is None:
            return None
        
        kwargs = obj.copy()
        type_ = kwargs.pop("type", None)
        title = kwargs.pop("title", None)
        status = kwargs.pop("status", None)
        detail = kwargs.pop("detail", None)
        return Error(type_, title, status, detail, **kwargs)
            
    def encode(self):
        obj = {
            "type": self.type, 
            "title": self.title,
            "status": self.status, 
            "detail": self.detail
        }
        
        obj.update(self.kwargs)
        return obj


# Common Resource class
class Resource(object):
    def __init__(self, type_, id_):
        self.type = type_
        self.id = id_


# Root exception
class AstroException(Exception):
    def __init__(self, message):
        super(AstroException, self).__init__(message)


# Based on part of RFC 7807 (https://tools.ietf.org/html/rfc7807)
class FailedException(AstroException):
    def __init__(self, type_, title, status, detail, **kwargs):
        super(FailedException, self).__init__(detail)
        self.type = type_
        self.title = title
        self.status = status
        self.kwargs = kwargs


class PermanentlyFailedException(FailedException):
    def __init__(self, type_, title, status, detail, **kwargs):
        super(PermanentlyFailedException, self).__init__(type_, title, status, detail, **kwargs)


class StoppedException(AstroException):
    def __init__(self):
        super(StoppedException, self).__init__("stopped")
