'''
Created on Mar 4, 2016

@author: alberto
'''


from doi.jobs.miner import fields as f
from doi.jobs.miner import exception
from doi.jobs.miner import oslc
from doi.jobs.miner import pipeline
from doi.util import json_util
from doi.util import log_util
import json


logger = log_util.get_logger("astro.jobs.miner.rtc")


class RtcClient(oslc.OslcClient):
    # {api_base_url}, {workspaceItemId}, {changesetId}
    GET_CHANGES_SET_PLUS_URI = ("{}/service/com.ibm.team.filesystem.common.internal.rest.IFilesystemRestService2/changeSetPlus?"
                                "knownTextContentTypePrefixes=text/&"
                                "knownTextContentTypeStrings=application/xml&"
                                "knownTextContentTypeStrings=application/javascript&"
                                "knownTextContentTypeStrings=application/ecmascript&"
                                "knownTextContentTypeStrings=application/x-javascript&"
                                "knownTextContentTypeStrings=application/unknown&"                           
                                "changeSetPath=workspaceId/{}/changeSetId/{}")
    
    # {api_base_url}, {workspaceName}
    SEARCH_WORKSPACES_URI =("{}/service/com.ibm.team.scm.common.internal.rest.IScmRestService2/searchWorkspaces?"
                            "workspaceName={}&workspaceNameKind=partial ignorecase&maxResultSize=50&workspaceKind=both")

    # {api_base_url}, {workspaceItemId}, {componentItemId}, {fileNameOrFILE_PATH}, {fileItemId}, {fileStateId}
    GET_FILE_CONTENT_URI = ("{}/service/com.ibm.team.filesystem.service.internal.rest.IFilesystemContentService/"
                            "{}/{}/{}?itemId={}&stateId={}")
    
    # {api_base_url}, {issueNumber}
    ISSUE_URI_FORMAT = "{}/resource/itemName/com.ibm.team.workitem.WorkItem/{}"

    # {api_base_url}, {project title}
    PROJECT_URI_FORMAT = "{}/web/projects/{}"
    
    AUTH_IDENTITY_PATH = '/authenticated/identity'
    SECURITY_CHECK_PATH = '/j_security_check'    
    VERIFY_RESPONSE_CONTENT = False

    def __init__(self, api_base_url, username, password, auth_method='rtc', verify=False, allow_redirects=True):
        super(RtcClient, self).__init__()
        self.api_base_url = api_base_url
        self.set_auth(username, password)
        self.auth_method = auth_method
        self.set_verify(verify)
        self.set_allow_redirects(allow_redirects)
        
    def login(self):
        if self.auth_method == 'rtc':            
            login_headers = {'accept':'application/xml'}            
            response = super(RtcClient, self).get(self.api_base_url + RtcClient.AUTH_IDENTITY_PATH, headers=login_headers)
            auth_msg = response.headers.get('x-com-ibm-team-repository-web-auth-msg')
            if auth_msg is not None and (auth_msg == 'authfailed' or auth_msg ==  'authrequired'):
                response = super(RtcClient, self).post(self.api_base_url + RtcClient.SECURITY_CHECK_PATH, 
                        data={'j_username': self.get_auth_username(), 'j_password': self.get_auth_password()},
                        headers=login_headers)
                
                auth_msg = response.headers.get('x-com-ibm-team-repository-web-auth-msg')
                if auth_msg is not None and (auth_msg == 'authfailed' or auth_msg ==  'authrequired'):
                    raise exception.AuthenticationException("Authentication error: message: {}".format(auth_msg))
                response = super(RtcClient, self).get(self.api_base_url + RtcClient.AUTH_IDENTITY_PATH, headers=login_headers)
                logger.info("Auth response: %s", response.text)
        elif self.auth_method == 'basic':
            self.add_basic_auth_header(self.get_auth_username(), self.get_auth_password())
        else:
            raise exception.AuthenticationException("Unsupported authentication method: {}".format(self.auth_method))

    # Override rest get to handle the RTC auth required issue
    def get(self, uri, headers=None, timeout=60, **kwargs):
        response = super(RtcClient, self).get(uri, headers, timeout, **kwargs)

        if self.auth_method == 'rtc':            
            # When auth_method is 'rtc', RTC returns status code 200 even if the operation failed because auth is required!!! Thanks, RTC team!
            # We need to check by ourselves if the operation really completed successfully.
            auth_msg = response.headers.get('x-com-ibm-team-repository-web-auth-msg')
            if auth_msg is not None and (auth_msg == 'authfailed' or auth_msg ==  'authrequired'):
                logger.warning("Auth required again. Login again and execute the operation again ...")
                self.login()
                response = super(RtcClient, self).get(uri, headers, timeout, **kwargs)                
        
        return response

    def get_work_items_catalog(self):
        return self.get_service_provider_catalog(self.api_base_url + "/oslc/workitems/catalog")

    def get_file_content(self, workspaceItemId, componentItemId, filePath, fileItemId, fileStateId):
        get_file_content_uri = RtcClient.GET_FILE_CONTENT_URI.format(self.api_base_url, workspaceItemId, componentItemId, filePath, fileItemId, fileStateId)
        resp = self.get(get_file_content_uri, { 'Accept': 'application/octet-stream'})
        return resp.content
        
        
class RtcIssuesReader(pipeline.Reader):
    def __init__(self, api_base_url, project_title, username, password, 
                 auth_method='rtc', verify=False, allow_redirects=True):
        super(RtcIssuesReader, self).__init__()
        self.api_base_url = api_base_url
        self.project_title = project_title
        self.client = RtcClient(
            api_base_url, username, password, 
            auth_method, verify, allow_redirects)

    def iter(self):
        self.client.login()
        work_items_catalog = self.client.get_work_items_catalog()
        work_items_service_provider = work_items_catalog.get_service_provider(self.project_title)
        if work_items_service_provider is None:
            raise exception.NotFoundException("Project not found: '{}'".format(self.project_title))        
        work_items_query_capability = work_items_service_provider.get_query_capability("Change request queries")
        work_items_query_base_uri = work_items_query_capability.get_query_base_uri()
        self.page_iterator = oslc.OslcPageIterator(self.client, work_items_query_base_uri)

        for page in self.page_iterator:
            for result in page:
                response = self.client.get(result['rdf:resource'])
                resource = json.loads(response.content)
                yield resource
            

class RtcStreamIssuesReader(pipeline.Reader):
    def __init__(self, api_base_url, project_title, username, password, 
                 auth_method='rtc', verify=False, allow_redirects=True, streaming=False):
        super(RtcStreamIssuesReader, self).__init__()
        self.api_base_url = api_base_url
        self.project_title = project_title
        self.client = RtcClient(
            api_base_url, username, password, 
            auth_method, verify, allow_redirects)

        self.client.login()
        work_items_catalog = self.client.get_work_items_catalog()
        work_items_service_provider = work_items_catalog.get_service_provider(self.project_title)
        if work_items_service_provider is None:
            raise exception.NotFoundException("Project not found: '{}'".format(self.project_title))        
        work_items_query_capability = work_items_service_provider.get_query_capability("Change request queries")
        work_items_query_base_uri = work_items_query_capability.get_query_base_uri()
        self.page_iterator = oslc.OslcPageIterator(self.client, work_items_query_base_uri)


    def iter(self):
        
        results = self.page_iterator.next()
        for result in results:
            response = self.client.get(result['rdf:resource'])
            resource = json.loads(response.content)
            yield resource   
            

class RtcIssuesNormalizer(pipeline.Normalizer):
    def __init__(self, client):
        super(RtcIssuesNormalizer, self).__init__()
        self.client = client
        self.cached_resources = {}

    def normalize(self, issue, norm_issue):
        json_util.set_value(norm_issue, f.ISSUE_URI,   issue, 'rdf:about')
        json_util.set_value(norm_issue, f.ISSUE_ID,    issue, 'dcterms:identifier')
        json_util.set_value(norm_issue, f.ISSUE_KEY,   issue, 'rdf:about')        
        json_util.set_array(norm_issue, f.ISSUE_TYPES, issue, 'dcterms:type')
        
        # Time fields
        json_util.set_time(norm_issue, f.ISSUE_CREATED_TIME, issue, 'dcterms:created')   
        json_util.set_time(norm_issue, f.ISSUE_UPDATED_TIME, issue, 'dcterms:modified')   
        json_util.set_time(norm_issue, f.ISSUE_CLOSED_TIME,  issue, 'oslc_cm:closeDate')   
        
        # Resource fields
        self.set_resource_item(norm_issue, f.ISSUE_PROJECT,       issue, 'oslc_cmx:project',    'dcterms:title')        
        self.set_resource_item(norm_issue, f.ISSUE_PRIORITY,      issue, 'oslc_cmx:priority',   'dcterms:title')   
        self.set_resource_item(norm_issue, f.ISSUE_RESOLUTION,    issue, 'oslc_cmx:resolution', 'dcterms:title')   
        self.set_resource_item(norm_issue, f.ISSUE_SEVERITY,      issue, 'oslc_cmx:severity',   'dcterms:title')   
        self.set_resource_item(norm_issue, f.ISSUE_STATUS,        issue, 'rtc_cm:state',        'dcterms:title')
        
        self.set_resource_item(norm_issue, f.ISSUE_DEFECT_TARGET, issue, 'rtc_ext:com.ibm.imp.defecttarget', 'dcterms:title')   
        self.set_resource_item(norm_issue, f.ISSUE_FEATURE,       issue, 'rtc_ext:com.ibm.imp.feature',      'dcterms:title')   
        self.set_resource_item(norm_issue, f.ISSUE_FILED_AGAINST, issue, 'rtc_cm:filedAgainst',              'dcterms:title')   
        self.set_resource_item(norm_issue, f.ISSUE_FOUND_IN,      issue, 'rtc_cm:foundIn',                   'dcterms:title')   
        self.set_resource_item(norm_issue, f.ISSUE_PLANNED_FOR,   issue, 'rtc_cm:plannedFor',                'dcterms:title')   
        self.set_resource_item(norm_issue, f.ISSUE_SUBFEATURE,    issue, 'rtc_ext:com.ibm.imp.subfeature',   'dcterms:title')  
        
        json_util.set_value(norm_issue, f.ISSUE_SUMMARY,     issue, 'dcterms:title')
        json_util.set_value(norm_issue, f.ISSUE_DESCRIPTION, issue, 'dcterms:description')
            
        # Issue components array
        if f.ISSUE_FEATURE in norm_issue:
            component = norm_issue[f.ISSUE_FEATURE]
            if f.ISSUE_SUBFEATURE in norm_issue:
                component = component + '/' + norm_issue[f.ISSUE_SUBFEATURE]
            norm_issue[f.ISSUE_COMPONENTS] = [ component ]
        else:
            norm_issue[f.ISSUE_COMPONENTS] = []
            
        # Issue versions
        if f.ISSUE_FOUND_IN in norm_issue:
            version = norm_issue[f.ISSUE_FOUND_IN]
            norm_issue[f.ISSUE_VERSIONS] = [ version ]
        else: 
            norm_issue[f.ISSUE_VERSIONS] = []
                     
        # Parent and resolved type
        json_util.set_array(norm_issue, f.ISSUE_RESOLVED_TYPES, issue, 'dcterms:type')
        
        # Person fields
        json_util.set_value(norm_issue, f.ISSUE_CREATOR_URI,  issue, 'dcterms:creator',   'rdf:resource')
        json_util.set_value(norm_issue, f.ISSUE_MODIFIER_URI, issue, 'rtc_cm:modifiedBy', 'rdf:resource')
        json_util.set_value(norm_issue, f.ISSUE_ASSIGNEE_URI, issue, 'rtc_cm:resolvedBy', 'rdf:resource') # hmm, not really the same

        norm_issue[f.ISSUE_TAGS] = []
        
        # Set the source uri to the RTC project uri
        json_util.set_value(norm_issue, f.ISSUE_SOURCE_URI, issue, 'oslc_cmx:project', 'rdf:resource')
        norm_issue[f.ISSUE_SOURCE_TYPE] =  "rtc"
        norm_issue[f.TIMESTAMP] = norm_issue[f.ISSUE_CREATED_TIME]        
    
    def set_resource_item(self, target, target_field, source, source_field, nested_source_field):
        source_value = source.get(source_field)
        if source_value is not None:
            uri = source_value['rdf:resource']
            resource = self.get_resource(uri)
            json_util.set_value(target, target_field, resource, nested_source_field)

    def get_resource(self, uri):
        resource = self.cached_resources.get(uri)
        if resource is None:
            response = self.client.get(uri)
            resource = json.loads(response.content)
            self.cached_resources[uri] = resource
                
        return resource
