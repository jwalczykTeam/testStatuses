'''
Created on Jan 22, 2017

@author: alberto
'''

class Reader(object):
    def iter(self):
        raise NotImplementedError("iter")
    
    def close(self):
        pass


class Writer(object):
    def write(self, resource):
        raise NotImplementedError("write")
    
    def close(self):
        pass
    

class Normalizer(object):
    def normalize(self, resource, norm_resource):
        raise NotImplementedError("normalize")


class Updater(object):
    def update(self, resource):
        raise NotImplementedError("update")
