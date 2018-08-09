'''
Created on Sep 9, 2016

@author: alberto
'''

class Api(object):
    def build_data(self, status, data):
        return (
            data,
            status
        )
        
    '''
    {
      "type": "about:blank", 
      "title": "Bad Request"
      "status": 400, 
      "detail": "The browser (or proxy) sent a request that this server could not understand."
    }
    '''


    def build_error(self, ex):
        error =   {
            "type": ex.type,
            "title": ex.title,
            "status": ex.status,
            "detail": ex.message
        }
                
        error.update(ex.kwargs)        
        
        return (
            error,
            ex.status
        )    
