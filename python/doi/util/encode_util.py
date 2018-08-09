'''
Created on Oct 2, 2016

@author: alberto
'''


def set_value_field(obj, name, resource):
    setattr(obj, name, resource[name])
    
def set_object_field(obj, name, resource, cls):
    setattr(obj, name, cls.decode(resource[name]))    
    
def set_object_array_field(obj, name, resource, cls):
    setattr(obj, name, [cls.decode(x) for x in resource[name]])
    
    
def get_value_field(resource, name, obj):
    resource[name] = getattr(obj, name)
    
def get_object_field(resource, name, obj, cls):
    resource[name] = getattr(obj, name).encode()    
    
def get_object_array_field(resource, name, obj, cls):
    resource[name] = [x.encode() for x in resource[name]]
