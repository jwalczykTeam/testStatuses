'''
Created on Oct 3, 2016

@author: alberto
'''


import string


def strtobool(val):
    val = string.lower(val)
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return 1
    elif val in ('n', 'no', 'f', 'false', 'off', '0'):
        return 0
    else:
        raise ValueError, "invalid truth value %r" % (val,)
