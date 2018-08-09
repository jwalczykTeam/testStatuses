'''
Created on Apr 17, 2016

@author: alberto
'''


class AltKeysDict(object):
    def __init__(self, key_count):
        self.dicts = [{} for _ in xrange(0, key_count)]

    def get(self, keys, default_value=None):
        for d, key in zip(self.dicts, keys):
            if key is not None:
                value = d.get(key)
                if value is not None:
                    return value

        return default_value    

    def put(self, keys, value):
        for d, key in zip(self.dicts, keys):
            if key is not None:
                d[key] = value
