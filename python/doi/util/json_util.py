'''
Created on Dec 31, 2015

@author: alberto
'''


from doi.util import time_util


def set_value(target, target_field, source, *source_fields):
    source_value = __get_value(source, source_fields)
    if source_value is not None:
        target[target_field] = source_value

def set_array(target, target_field, source, *source_fields):
    source_value = __get_value(source, source_fields)
    if source_value is not None:
        if not target_field in target:
            target[target_field] = [ source_value ]
        else:
            target[target_field].append(source_value)

def set_time(target, target_field, source, *source_fields):
    source_value = __get_value(source, source_fields)
    if source_value is not None:
        target[target_field] = time_util.iso8601_to_utc_millis(source_value)
    
def __get_value(source, source_fields):
    value = source
    for source_field in source_fields:
        value = value.get(source_field)
        if value is None:
            return None
    return value

def find_all_values(source, source_key):
    values = []
                
    def add_values(source, source_key):
        if type(source) is dict:
            for key, value in source.iteritems():
                if type(value) in (list, dict):
                    add_values(value, source_key)
                elif key == source_key:
                    values.append(value)
        elif type(source) is list:
            for item in source:
                if type(item) in (list, dict):
                    add_values(item, source_key)                

    add_values(source, source_key)
    return values

def find_first_value(source, source_key):
    if type(source) is dict:
        for key, value in source.iteritems():
            if type(value) in (list, dict):
                found = find_first_value(value, source_key)
                if found is not None:
                    return found
            elif key == source_key:
                return value
    elif type(source) is list:
        for item in source:
            if type(item) in (list, dict):
                found = find_first_value(item, source_key)
                if found is not None:
                    return found                

    return None

def set_first_value(target, target_key, source, source_key):
    source_value = find_first_value(source, source_key)
    if source_value is not None:
        target[target_key] = source_value

def find_first_dict(source, ref_key, ref_value):
    if type(source) is dict:
        for key, value in source.iteritems():
            if type(value) in (list, dict):
                found = find_first_dict(value, ref_key, ref_value)
                if found is not None:
                    return found
            elif key == ref_key and value == ref_value:
                return source
    elif type(source) is list:
        for item in source:
            if type(item) in (list, dict):
                found = find_first_dict(item, ref_key, ref_value)
                if found is not None:
                    return found                

    return None

def set_first_value_with_ref_item(target, target_key, source, source_key, ref_key, ref_value):
    source_dict = find_first_dict(source, ref_key, ref_value)
    if source_dict is not None and source_key in source_dict:
        target[target_key] = source_dict[source_key]
