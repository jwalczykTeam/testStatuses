'''
Created on Jun 4, 2016

@author: alberto
'''


from doi.jobs.miner import fields as f
from doi.jobs.miner import pipeline
from doi.util import rest
from doi.util import json_util
from doi.util import log_util
import json


logger = log_util.get_logger("astro.jira.miner.jira")


class JiraClient(rest.RestClient):
    SEARCH_URI_FORMAT  = "{}/rest/api/2/search"
    ISSUES_URI_FORMAT  = "{}/rest/api/2/issue"
    ISSUE_URI_FORMAT   = "{}/rest/api/2/issue/{}"
    PROJECT_URI_FORMAT = "{}/rest/api/2/project/{}"
    
    def __init__(self, api_base_url, username=None, password=None):
        super(JiraClient, self).__init__()        
        self.api_base_url = api_base_url
        if username:
            self.add_basic_auth_header(username, password)


class JiraIssuesReader(pipeline.Reader):
    # Designed not to throw exceptions    
    def __init__(self, api_base_url, username, password, project_key, start_at=0, stop_at=0, max_results=100):
        super(JiraIssuesReader, self).__init__()        
        self.client = JiraClient(api_base_url, username, password)
        self.api_base_url = api_base_url
        self.project_key = project_key
        self.start_at = start_at
        self.stop_at = stop_at
        self.max_results = max_results
        
    def iter(self):
        '''
        get next resource
        '''
        
        at = self.start_at
        while self.stop_at == 0 or at <= self.stop_at:
            page = self._get_page(at)
            length = len(page)
            if length == 0:
                break
            
            for resource in page:
                yield resource

            at += length 

    def _get_page(self, at):
        '''
        get current page
        '''
        
        query_uri = JiraClient.SEARCH_URI_FORMAT.format(self.api_base_url)
        data = { 
            'jql': "project={}".format(self.project_key),
            'startAt': at, 
            'maxResults': self.max_results
        }
        
        headers = { 
            'Content-Type': "application/json",
            'Accept': "application/json"
        }
                
        logger.info("Post to '%s' with '%s'", query_uri, data)
        resp = self.client.post(query_uri, data=json.dumps(data), headers=headers, timeout=300)
        page = json.loads(resp.content)
        
        # If first time, set the cookie for the next calls
        if at == self.start_at:
            self.client.add_header('Cookie', resp.headers.get('set-cookie'))

        return page['issues']


class JiraIssuesNormalizer(pipeline.Normalizer):
    def __init__(self):
        super(JiraIssuesNormalizer, self).__init__()        

    def normalize(self, issue, norm_issue):
        norm_issue[f.ISSUE_ID]  = issue['id']
        norm_issue[f.ISSUE_URI] = issue['self']
        norm_issue[f.ISSUE_KEY] = self._get_issue_key_uri(issue['self'], issue['key'])
        
        # Fields
        fields = issue['fields']
        json_util.set_value(norm_issue, f.ISSUE_PROJECT,      fields, 'project', 'name')
        json_util.set_array(norm_issue, f.ISSUE_TYPES,        fields, 'issuetype', 'name')
        json_util.set_value(norm_issue, f.ISSUE_PRIORITY,     fields, 'priority', 'name')
        json_util.set_value(norm_issue, f.ISSUE_RESOLUTION,   fields, 'resolution', 'name')
        json_util.set_value(norm_issue, f.ISSUE_STATUS,       fields, 'status', 'name')
        json_util.set_time (norm_issue, f.ISSUE_CREATED_TIME, fields, 'created')
        json_util.set_time (norm_issue, f.ISSUE_UPDATED_TIME, fields, 'updated')
        json_util.set_value(norm_issue, f.ISSUE_CREATOR_URI,  fields, 'creator', 'self')
        json_util.set_value(norm_issue, f.ISSUE_ASSIGNEE_URI, fields, 'assignee', 'self')
        json_util.set_value(norm_issue, f.ISSUE_REPORTER_URI, fields, 'reporter', 'self')        
        json_util.set_value(norm_issue, f.ISSUE_SUMMARY,      fields, 'summary')
        json_util.set_value(norm_issue, f.ISSUE_DESCRIPTION,  fields, 'description')
        
        # Set parent fields and resolved type
        parent = fields.get('parent')
        if parent is not None:
            json_util.set_value(norm_issue, f.ISSUE_PARENT_KEY, parent, 'key')
            parent_fields = parent['fields']
            json_util.set_array(norm_issue, f.ISSUE_RESOLVED_TYPES,  parent_fields, 'issuetype', 'name')
            json_util.set_value(norm_issue, f.ISSUE_PARENT_PRIORITY, parent_fields, 'priority', 'name')
            json_util.set_value(norm_issue, f.ISSUE_PARENT_STATUS,   parent_fields, 'status', 'name')
            json_util.set_array(norm_issue, f.ISSUE_PARENT_TYPES,    parent_fields, 'issuetype', 'name')
        else:
            json_util.set_array(norm_issue, f.ISSUE_RESOLVED_TYPES, fields, 'issuetype', 'name')
    
        # Components
        component_names = []
        if 'components' in fields:
            for component in fields['components']:
                component_names.append(component['name'])
        json_util.set_value(norm_issue, f.ISSUE_COMPONENTS, component_names)
      
        # FixVersions
        fix_version_names = []
        if 'fixVersions' in fields:
            for fix_version in fields['fixVersions']:
                fix_version_names.append(fix_version['name'])
        json_util.set_value(norm_issue, f.ISSUE_FIX_VERSIONS, fix_version_names)
    
        # Version
        version_names = []
        if 'versions' in fields:
            for version in fields['versions']:
                version_names.append(version['name'])
        json_util.set_value(norm_issue, f.ISSUE_VERSIONS, version_names)
    
        norm_issue[f.ISSUE_TAGS]  = []
        norm_issue[f.ISSUE_SOURCE_URI]  = fields['project']['self']
        norm_issue[f.ISSUE_SOURCE_TYPE] = "jira"
        norm_issue[f.TIMESTAMP] = norm_issue[f.ISSUE_CREATED_TIME]    
        return norm_issue
                
    def _get_issue_key_uri(self, uri, key):
        uri_parts = uri.rsplit('/', 1)
        return uri_parts[0] + '/' + key
    
                
