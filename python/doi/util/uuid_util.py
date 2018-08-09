'''
Created on Aug 16, 2016

@author: alberto
'''


import uuid

class UniqueIDByKeyValues(object):
    class Node(object):
        def __init__(self, key_values, uid):
            self.values_by_key = {}
            self.uid = uid
            self.merge(key_values)
            
        def merge(self, key_values):
            for key, value in key_values.iteritems():
                if value is not None:
                    value = value.lower()
                    values = self.values_by_key.get(key)
                    if values is None:
                        values = set()
                        self.values_by_key[key] = values
                    values.add(value)
                            
    def __init__(self):
        self.nodes = []

    def set(self, key_values, uid):
        if not self.validate(key_values):
            return None
        
        node = UniqueIDByKeyValues.Node(key_values, uid)
        self.nodes.append(node)
        return uid
    
    def get(self, key_values):
        if not self.validate(key_values):
            return None

        for node in self.nodes:
            for key, value in key_values.iteritems():
                if value is not None:
                    value = value.lower()
                    node_values = node.values_by_key.get(key)
                    if node_values is not None and value in node_values:
                        # Merge the other values
                        node.merge(key_values)
                        return node.uid

        uid = str(uuid.uuid1())
        node = UniqueIDByKeyValues.Node(key_values, uid)
        self.nodes.append(node)
        return uid

    def close(self):
        del self.nodes[:]

    def validate(self, key_values):
        for value in key_values.itervalues():
            if value is not None:
                return True
        
        return False

    def print_nodes(self):
        for node in self.nodes:
            print("uid={}, key_values={}".format(node.uid, node.values_by_key))
