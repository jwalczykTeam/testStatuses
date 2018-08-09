'''
Created on Jan 13, 2016

@author: alberto
'''

import os, urllib, urlparse


def build_file_path_from_uri(uri, root=None, ext=None):
    parsed_uri = urlparse.urlparse(urllib.unquote(uri))
    uri_hostname = parsed_uri.hostname
    uri_path = parsed_uri.path[1:]
    file_path = os.path.join(uri_hostname, uri_path)
    
    if parsed_uri.query:
        file_path = os.path.join(file_path, parsed_uri.query)
    
    if parsed_uri.fragment:
        file_path = os.path.join(file_path, parsed_uri.fragment)

    if root is not None:
        file_path = os.path.join(root, file_path)
        
    if ext is not None:
        file_path = file_path + ext
        
    return file_path

def add_creds_to_uri(uri, username, password):
    parsed_uri = urlparse.urlparse(uri)

    creds = None
    if username is not None:
        creds = username
    if password is not None:
        creds += ':' + password

    netloc = parsed_uri.netloc
    if creds is not None:
        netloc = creds + '@' + netloc

    new_parsed_uri = urlparse.ParseResult(
            parsed_uri.scheme, netloc, parsed_uri.path, 
            parsed_uri.params, parsed_uri.query, parsed_uri.fragment)
                     
    return urlparse.urlunparse(new_parsed_uri)

def add_token_to_uri(uri, token):
    parsed_uri = urlparse.urlparse(uri)

    creds = None
    if token is not None:
        creds = token + ':' + token

    netloc = parsed_uri.netloc
    if creds is not None:
        netloc = creds + '@' + netloc

    new_parsed_uri = urlparse.ParseResult(
            parsed_uri.scheme, netloc, parsed_uri.path, 
            parsed_uri.params, parsed_uri.query, parsed_uri.fragment)
                     
    return urlparse.urlunparse(new_parsed_uri)

def unquote_query_param(param):
    if param is None:
        return None

    param = param.strip('"')    
    return urllib.unquote(param)

def sub_uri(uri, n):
    uri_parts = uri.rsplit('/', n)
    return uri_parts[0]
