'''
Created on Mar 4, 2016

@author: alberto
'''

from doi.util import rest
from doi.util import log_util
import json
import urllib


logger = log_util.get_logger("astro.jobs.miner.oslc") 


class OslcClient(rest.RestClient):
    '''
    A thin wrapper around oslc resources with useful methods to access relevant data
    '''
    
    OSLC_CR_XML = "application/x-oslc-cm-change-request+xml"
    OSLC_CR_JSON = "application/x-oslc-cm-change-request+json"    
    
    def __init__(self):
        super(OslcClient, self).__init__()
        self.add_header('OSLC-Core-Version', "2.0")
        self.add_header('Accept', "application/json,application/rdf+xml")

    def get_service_provider_catalog(self, uri):
        return OslcServiceProviderCatalog(self, uri)


class OslcServiceProviderCatalog(object):    
    def __init__(self, client, query_uri):
        self.client = client
        resp = self.client.get(query_uri)
        self.service_provider_catalog = json.loads(resp.content)
    
    def get_service_provider(self, service_provider_title):
        service_providers = self.service_provider_catalog['oslc:serviceProvider']
        for service_provider in service_providers:
            title = service_provider['dcterms:title']
            if title == service_provider_title:
                return OslcServiceProvider(self.client, service_provider['rdf:about'])
        
        return None


class OslcServiceProvider(object):    
    def __init__(self, client, query_uri):
        self.client = client
        resp = self.client.get(query_uri)
        self.service_provider = json.loads(resp.content)
    
    def _get_capability(self, capability_type, capability_title):
        for service in self.service_provider['oslc:service']:
            if capability_type in service:
                for capability in service[capability_type]:
                    title = capability['dcterms:title']
                    if title == capability_title:
                        return capability
        
        return None
    
    def get_query_capability(self, query_capability_title):
        return OslcQueryCapability(self._get_capability('oslc:queryCapability', 
                                                    query_capability_title))

    def get_creation_factory(self, creation_factory_title):
        return OslcCreationFactory(self._get_capability('oslc:creationFactory', 
                                                        creation_factory_title))
   
    def get_selection_dialog(self, selection_dialog_title):
        return OslcDialog(self._get_capability('oslc:selectionDialog', 
                                               selection_dialog_title))
    
    def get_creation_dialog(self, creation_catalog_title):
        return OslcDialog(self._get_capability('oslc:creationDialog', 
                                               creation_catalog_title))


class OslcQueryCapability(object):
    def __init__(self, query_capability):
        self.query_capability = query_capability
        
    def get_query_base_uri(self):
        return self.query_capability['oslc:queryBase']['rdf:resource']

        
class OslcCreationFactory(object):
    def __init__(self, creation_factory):
        self.creation_factory = creation_factory

    def get_creation_uri(self):
        return self.creation_factory['oslc:dialog']['rdf:resource']


class OslcDialog(object):
    def __init__(self, dialog):
        self.dialog = dialog

    def get_dialog_uri(self):
        return self.dialog['oslc:dialog']['rdf:resource']
    
    
class OslcPageIterator(object):
    def __init__(self, client, query_uri, page_size=50):
        self.client = client
        params = urllib.urlencode({'oslc.pageSize': page_size})
        self.next_page_uri = "{}?{}".format(query_uri, params)

    def __iter__(self):
        return self
        
    def next(self):
        if self.next_page_uri is None:
            raise StopIteration
        
        logger.info("Get '%s'", self.next_page_uri)
        resp = self.client.get(self.next_page_uri)
        page = json.loads(resp.content)
        self.results = page['oslc:results']
        response_info = page['oslc:responseInfo']
        self.next_page_uri = response_info.get('oslc:nextPage')
        return self.results
