'''
Created on Dec 31, 2015

@author: alberto
'''


from time_util import iso8601_to_utc_millis


def set_value(target, target_field, source, source_path, norm=None, value_map=None):
    source_value = source.find(source_path)
    if source_value is not None:
        value = source_value.text
        if norm is not None:
            value = norm(value)
        
        if value_map is not None:
            value = value_map[value]
            
        target[target_field] = value

def set_array(target, target_field, source, source_path, norm=None, value_map=None):
    values = []
    for source_value in source.findall(source_path):
        value = source_value.text
        if norm is not None:
            value = norm(value)
        
        if value_map is not None:
            value = value_map[value]
            
        values.append(value)
    
    target[target_field] = values

def set_time(target, target_field, source, source_path):
    source_value = source.find(source_path)
    if source_value is not None:
        target[target_field] = iso8601_to_utc_millis(source_value.text)
