'''
Created on Jan 1, 2016

@author: alberto
'''


def remove_prefix(s, prefixes):
	"""
	Removes the first letter of a string
	"""
	for prefix in prefixes:
		if s.startswith(prefix):
			return s[len(prefix):]

	return s

def remove_suffix(s, suffixes):
	"""
	Removes the last letter of a string
	"""
	for suffix in suffixes:
		if s.endswith(suffix):
			return s[0:len(s) - len(suffix)]

	return s
