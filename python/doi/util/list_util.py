'''
Created on Jan 19, 2016

@author: alberto
'''


def flatten(l):
    return [item for sublist in l for item in sublist]

def remove_duplicates(l):
    return list(set(l))

def find_dict_array_field(array, field, value):
    for elem in array:
        if elem[field] == value:
            return True
    
    return False

'''
return a list of values of a specified field in an array of structs
'''
def get_struct_array_field_values_as_set(array, field_name):
    values = []
    for elem in array:
        values.append(elem[field_name])
    return values

'''
return a set of values of a specified field in an array of structs
'''
def get_struct_array_field_values_as_list(array, field_name):
    values = set()
    for elem in array:
        values.add(elem[field_name])
    return values

def split_list(array, chunk_count):
    chunks = []
    chunk_len = len(array) / chunk_count
    begin_index = 0
    end_index = chunk_len
    for _ in range(0, chunk_count):
        chunks.append(array[begin_index: end_index])
        begin_index += chunk_len
        end_index += chunk_len
        
    if begin_index < len(array):
        chunks.append(array[begin_index:])
    
    return chunks
