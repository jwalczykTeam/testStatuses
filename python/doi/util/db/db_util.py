'''
Created on Sep 1, 2016

@author: alberto
'''


from doi.util import log_util
from elasticsearch_util import ElasticsearchClient, ComposeElasticsearchClient


'''
Config:

    {
        "elasticsearch": {
            "hosts": [ ... ]
        }
    }
    
    Or
    
    {
        "compose_elasticsearch": {
            "public_hostname": ...
            "username": ...
            "password: ...
        }
    }
    
    or 
    
    {
        "cloudant": {
            "username": ...
            "password: ...
            "url": ...
        }
    }
    
'''    


logger = log_util.get_logger("astro.util.db")


class Database(object):
    @staticmethod
    def factory(config):
        if 'elasticsearch' in config:
            return ElasticsearchClient(config['elasticsearch'])
        elif 'compose-elasticsearch' in config:
            return ComposeElasticsearchClient(config['compose-elasticsearch'])
        else:
            raise ValueError("No or unknown database type specified")
        
    @staticmethod
    def is_db_name_valid(name):
        VALID_SPECIAL_CHARS = "_$()+-@"
        
        for c in name:
            if c.islower():
                pass
            elif c.isdigit():
                pass
            elif c in VALID_SPECIAL_CHARS:
                pass
            else:
                return False
            
        return True