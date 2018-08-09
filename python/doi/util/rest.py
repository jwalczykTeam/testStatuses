'''
Created on Mar 4, 2016

@author: alberto
'''

from doi.util import log_util
import base64
import requests
import time
requests.packages.urllib3.disable_warnings()


logger = log_util.get_logger("astro.util.rest")


class RestClient(object):
    CONTENT_XML = "text/xml"
    CONTENT_URL_ENCODED = "application/x-www-form-urlencoded"
    DEFAULT_REQUEST_TIMEOUT = 60
    DEFAULT_MAX_RETRY_COUNT = 5
    DEFAULT_RETRY_INTERVAL = 60
    
    def __init__(self):
        self.session = requests.Session()

    def add_basic_auth_header(self, username, password):
        creds = None
        
        if username is not None:
            creds = username
        
        if password is not None:
            creds += ':' + password
        
        if creds is not None:
            base64_creds = base64.encodestring(creds)[:-1]    # trailing \n removed
            self.add_header('Authorization', "Basic {}".format(base64_creds))        

    def add_token_auth_header(self, token):
        self.add_header('Authorization', "token {}".format(token))

    def add_header(self, key, value):
        self.session.headers[key] = value

    def set_allow_redirects(self, value):
        self.session.allow_redirects = value

    def set_verify(self, value):
        self.session.verify = value
    
    def set_auth(self, username, password):
        self.session.auth = (username, password)

    def get_auth_username(self):
        return self.session.auth[0]

    def get_auth_password(self):
        return self.session.auth[1]

    def get(self, uri, headers=None, 
            timeout=DEFAULT_REQUEST_TIMEOUT, **kwargs):
        retry_count = 0    
        max_retry_count = kwargs.get('max_retry_count', RestClient.DEFAULT_MAX_RETRY_COUNT)
        retry_interval = kwargs.get('retry_interval', RestClient.DEFAULT_RETRY_INTERVAL)
        
        while True:
            try:
                response = self.session.get(uri, 
                        headers=headers,
                        timeout=timeout, **kwargs)
                if response.status_code == 200:
                    return response
                else:
                    break
            except requests.RequestException as e:
                if retry_count >= max_retry_count:
                    raise e
                
                time.sleep(retry_interval)
                retry_count += 1
                logger.warning("Possibly recoverable error: exception='%s'", str(e))
                logger.warning("Retrying GET request: time %d ...", retry_count)
        
        response.raise_for_status()

    def post(self, uri, data=None, headers=None, 
             timeout=DEFAULT_REQUEST_TIMEOUT, **kwargs):
        retry_count = 0    
        max_retry_count = kwargs.get('max_retry_count', RestClient.DEFAULT_MAX_RETRY_COUNT)
        retry_interval = kwargs.get('retry_interval', RestClient.DEFAULT_RETRY_INTERVAL)
        
        while True:
            try:
                response = self.session.post(
                    uri, 
                    data=data, 
                    headers=headers,
                    timeout=timeout, **kwargs)
                if response.status_code in [200, 201, 202]:
                    return response            
                else:
                    if retry_count >= max_retry_count:
                        break

                    time.sleep(retry_interval)
                    retry_count += 1
                    logger.warning("Possibly recoverable error: response status code='%s'", response.status_code)
                    logger.warning("Retrying POST request: time {} ...".format(retry_count))
            except requests.RequestException as e:
                if retry_count >= max_retry_count:
                    raise e

                time.sleep(retry_interval)
                retry_count += 1
                logger.warning("Possibly recoverable error: exception='%s'", str(e))
                logger.warning("Retrying POST request: time %d ...", retry_count)

        response.raise_for_status()

    def put(self, uri, data=None, headers=None, 
            timeout=DEFAULT_REQUEST_TIMEOUT, **kwargs):
        retry_count = 0    
        max_retry_count = kwargs.get('max_retry_count', RestClient.DEFAULT_MAX_RETRY_COUNT)
        retry_interval = kwargs.get('retry_interval', RestClient.DEFAULT_RETRY_INTERVAL)
        
        while True:
            try:
                response = self.session.put(
                    uri, 
                    data=data, 
                    headers=headers,
                    timeout=timeout, **kwargs)
                if response.status_code in [200, 201, 202]:
                    return response            
                else:
                    if retry_count >= max_retry_count:
                        break

                    time.sleep(retry_interval)
                    retry_count += 1
                    logger.warning("Possibly recoverable error: response status code='%s'", response.status_code)
                    logger.warning("Retrying PUT request: time {} ...".format(retry_count))
            except requests.RequestException as e:
                if retry_count >= max_retry_count:
                    raise e

                time.sleep(retry_interval)
                retry_count += 1
                logger.warning("Possibly recoverable error: exception='%s'", str(e))
                logger.warning("Retrying PUT request: time %d ...", retry_count)

        response.raise_for_status()

    def delete(self, uri, headers=None, 
               timeout=DEFAULT_REQUEST_TIMEOUT, **kwargs):
        retry_count = 0    
        max_retry_count = kwargs.get('max_retry_count', RestClient.DEFAULT_MAX_RETRY_COUNT)
        retry_interval = kwargs.get('retry_interval', RestClient.DEFAULT_RETRY_INTERVAL)
        
        while True:
            try:
                response = self.session.delete(
                    uri, 
                    headers=headers,
                    timeout=timeout, **kwargs)        
                if response.status_code in [200, 202]:
                    return response
                else:
                    break
                
            except requests.RequestException as e:
                if retry_count == max_retry_count:
                    raise e
                
                time.sleep(retry_interval)
                retry_count += 1
                logger.warning("Possibly recoverable error: exception='%s'", str(e))
                logger.warning("Retrying DELETE request: time %d ...", retry_count)
        
        response.raise_for_status()

    
    def close(self):
        self.session.close()

