'''
Created on Jun 2, 2016

@author: alberto
'''


from doi.jobs.miner import fields as f
from doi.jobs.miner import pipeline
from doi.util import rest
from doi.util import log_util
from doi.util import json_util
from doi.util import time_util
from doi.util import uri_util
from requests.exceptions import HTTPError
from requests.utils import parse_header_links
import json
import time
import urllib
import urlparse
import datetime


logger = log_util.get_logger("astro.jobs.miner.gitlab")


class GitLabClient(rest.RestClient):
    REPO_URI_FORMAT            = "{}/projects/{}%2F{}"        # params: api_base_url, repo_owner, repo_name
    USERS_URI_FORMAT           = "{}/users"                   # params: api_base_url
    USER_URI_FORMAT            = "{}/users?username={}"       # params: api_base_url, user_login
    BRANCHES_URI_FORMAT        = "{}/repository/branches"     # params: repo_uri
    BRANCH_URI_FORMAT          = "{}/repository/branches/{}"  # params: repo_uri, branch_name
    CONTRIBUTORS_URI_FORMAT    = "{}/repository/contributors" # params: repo_uri
    ISSUES_URI_FORMAT          = "{}/issues"                  # params: repo_uri
    ISSUE_URI_FORMAT           = "{}/issues/{}"               # params: repo_uri, issue_id
    PULLS_URI_FORMAT           = "{}/merge_requests"          # params: repo_uri
    COMMITS_URI_FORMAT         = "{}/repository/commits"      # params: repo_uri
    COMMIT_URI_FORMAT          = "{}/repository/commits/{}"   # params: repo_uri, commit_id
    FILE_URI_FORMAT            = "{}/repository/files?{}&{}"  # params: repo_uri, file_path, ref

    def __init__(self, access_token=None):
        super(GitLabClient, self).__init__()        
        self.access_token = access_token
        if access_token is not None:
            self.add_header("PRIVATE-TOKEN", access_token)

    # overrides RestClient get method. Adds support for waiting for API rate limit reset time 
    def get(self, uri, headers=None, timeout=360, **kwargs):
        response = self.session.get(uri, headers=headers, timeout=timeout, **kwargs)
        status_code = response.status_code
        if status_code != 200:
            if status_code == 403:
                error = json.loads(response.content)
                message = error['message']
                if "API rate limit exceeded" in message:
                    rate_limit_reset_time = long(response.headers.get('X-RateLimit-Reset'))
                    self._wait_for_api_rate_limit_reset_time(rate_limit_reset_time)
                    return self.get(uri, headers, timeout, **kwargs)            
            
            response.raise_for_status()

        return response

    def _wait_for_api_rate_limit_reset_time(self, rate_limit_reset_time):
        now = time.mktime(time.localtime())
        sleep_time = rate_limit_reset_time - now +1
        rate_limit_reset_strftime = time.strftime("%d %b %Y %H:%M:%S", time.localtime(rate_limit_reset_time))
        logger.warning("API rate limit exceeded. Waiting for %d mins and %d secs. Restarting at %s ...", 
                       sleep_time / 60, sleep_time % 60, 
                       rate_limit_reset_strftime)
        time.sleep(sleep_time)


class GitLabReader(pipeline.Reader):
    # Designed not to throw exceptions
    def __init__(self, collection_uri, access_token,
                 details, 
                 since, 
                 from_page, to_page, per_page, **params):
        super(GitLabReader, self).__init__()        
        self.client = GitLabClient(access_token)
        self.collection_uri = collection_uri
        self.details = details
        self.since = since
        self.from_page = from_page
        self.to_page = to_page
        self.per_page = per_page
        self.page_no = from_page
        self.last_page = None
        self.params = params

        if 'wait' in self.params:
            self.wait = self.params['wait']        

    def iter(self):
        page_no = self.from_page
        while self.to_page == 0 or page_no <= self.to_page:
            page = self._get_page(page_no)
            if page is None or not page:
                break
            
            for resource in page:
                yield self.extend(resource, self.collection_uri)
                
            page_no += 1

    def extend(self, resource, collection_uri):
        return resource
        
    def close(self):
        self.client.close()
       
    def _get_page(self, page_no):
        '''
        get current page, set to_page = last_page
        '''
        
        headers = { 
            'Accept': "application/json"
        }        

        params = {'page': page_no, 'per_page': self.per_page}
        if self.since is not None:
            params['since'] = time_util.utc_millis_to_iso8601_datetime(self.since)        
        params.update(self.params)
        
        query_uri = "{}?{}".format(self.collection_uri, urllib.urlencode(params))
        logger.info("Get '%s'", query_uri)
        response = self.client.get(query_uri, headers=headers)
        self._set_last_page(response)
        
        content = response.content
        if not content:
            return None
        
        page = json.loads(content)
        if self.details:
            resources = []
            for resource in page:
                resource_uri = resource['url']
                try:
                    response = self.client.get(resource_uri)
                    resources.append(json.loads(response.content))
                except HTTPError as e:
                    if self._can_ignore(e):
                        continue
                    else:
                        raise e
                     
            page = resources
        
        return page
    
    def _can_ignore(self, e):
        status_code = e.response.status_code
        if status_code == 404:
            return True
    
    def _set_last_page(self, response):
        link_header = response.headers.get('Link')
        if link_header is not None:
            last_link = None
            for link in parse_header_links(link_header):
                if link['rel'] == 'last':
                    last_link = link
                    break
                  
            if last_link is not None:
                parsed_url = urlparse.urlparse(last_link['url'])
                query_params = urlparse.parse_qs(parsed_url.query)
                self.last_page = int(query_params['page'][0])
                if self.to_page == 0:
                    self.to_page = self.last_page


class GitLabContributorReader(GitLabReader):
    def __init__(
            self,
            api_base_url,
            access_token,
            owner, repo,
            since=None,
            from_page=1,
            to_page=1,
            per_page=100,
            wait=False,
            details=False):
        contributors_uri = GitLabClient.CONTRIBUTORS_URI_FORMAT.format(
            get_repo_uri(api_base_url, owner, repo))
        super(GitLabContributorReader, self).__init__(
            contributors_uri,
            access_token,
            details,
            since,
            from_page,
            to_page,
            per_page,
            wait=wait)


class GitLabIssueReader(GitLabReader):
    def __init__(self, api_base_url, access_token, owner, repo, since=None, from_page=1, to_page=0, per_page=100, wait=False):
        issues_uri =  GitLabClient.ISSUES_URI_FORMAT.format(get_repo_uri(api_base_url, owner, repo))
        super(GitLabIssueReader, self).__init__(issues_uri, access_token, 
                                                 False,
                                                 since,
                                                 from_page, to_page, per_page, filter='all', state='all', wait=wait)


class GitLabBranchReader(GitLabReader):
    def __init__(self, api_base_url, access_token, owner, repo, since=None, from_page=1, to_page=1, per_page=100):
        branches_uri =  GitLabClient.BRANCHES_URI_FORMAT.format(get_repo_uri(api_base_url, owner, repo))
        super(GitLabBranchReader, self).__init__(branches_uri, access_token, 
                                                False, 
                                                since,
                                                from_page, to_page, per_page)

    def extend(self, branch, repo_uri):
        created_at = branch['commit']['committed_date']
        branch['created_at'] = created_at
        branch['url'] = GitLabClient.BRANCH_URI_FORMAT.format(repo_uri, branch['name'])
        return branch
        
#
# Normalization classes   
#

class GitLabIssueNormalizer(pipeline.Normalizer):
    def __init__(self, label_field_map, repo_uri):
        super(GitLabIssueNormalizer, self).__init__()        
        self.label_field_map = label_field_map
        self.repo_uri = repo_uri
        
    def normalize(self, issue, norm_issue):
        url = GitLabClient.ISSUE_URI_FORMAT.format(self.repo_uri, issue['iid'])
        norm_issue[f.ISSUE_URI] = url
        norm_issue[f.ISSUE_ID]  = str(issue['iid'])
        
        json_util.set_value(norm_issue, f.ISSUE_KEY,          issue, 'web_url')
        json_util.set_value(norm_issue, f.ISSUE_STATUS,       issue, 'state')
        json_util.set_time (norm_issue, f.ISSUE_CREATED_TIME, issue, 'created_at')
        json_util.set_time (norm_issue, f.ISSUE_UPDATED_TIME, issue, 'updated_at')
        json_util.set_value(norm_issue, f.ISSUE_CREATOR_URI,  issue, 'author', 'web_url')
        json_util.set_value(norm_issue, f.ISSUE_ASSIGNEE_URI, issue, 'assignee', 'web_url')
        json_util.set_value(norm_issue, f.ISSUE_SUMMARY,      issue, 'title')
        json_util.set_value(norm_issue, f.ISSUE_DESCRIPTION,  issue, 'description')
        if issue['state'] is 'closed': json_util.set_time (norm_issue, f.ISSUE_CLOSED_TIME,  issue, 'closed_at')
    
        tags = []
        for label in issue['labels']:
            label_name = label
            tags.append(label_name)
            if self.label_field_map:
                self._assign_issue_field_by_label(norm_issue, label_name)
        norm_issue[f.ISSUE_TAGS] = tags
        
        if f.ISSUE_TYPES in norm_issue:
            norm_issue[f.ISSUE_RESOLVED_TYPES] = norm_issue[f.ISSUE_TYPES]
        
        norm_issue[f.ISSUE_SOURCE_URI] = uri_util.sub_uri(norm_issue[f.ISSUE_URI], 2)
        norm_issue[f.TIMESTAMP] = norm_issue[f.ISSUE_CREATED_TIME]    
        norm_issue[f.ISSUE_SOURCE_TYPE] = 'gitlab'
    
    def _assign_issue_field_by_label(self, norm_issue, label_name):
        field = self.label_field_map.get(label_name)
        if field is not None:
            field_name, field_value = field
            if field_name in [f.ISSUE_TYPES, f.ISSUE_COMPONENTS, 
                              f.ISSUE_VERSIONS, f.ISSUE_FIX_VERSIONS]:
                # if field is a valid array field
                if not field_name in norm_issue:
                    norm_issue[field_name] = [ field_value ]
                else:
                    norm_issue[field_name].append(field_value)
            elif field_name in [f.ISSUE_PRIORITY, f.ISSUE_RESOLUTION, 
                           f.ISSUE_SEVERITY, f.ISSUE_STATUS]:
                # if field is a valid single value field
                norm_issue[field_name] = field_value
            else:
                # if field is not valid
                raise ValueError("Field not allowed in labels map: {}".format(field))
    

class GitLabUserNormalizer(pipeline.Normalizer):
    def __init__(self, user_key_by_ids, repo_uri):
        super(GitLabUserNormalizer, self).__init__()
        self.user_key_by_ids = user_key_by_ids        
        self.repo_uri = repo_uri
        
    def normalize(self, user, norm_user):
        url = GitLabClient.CONTRIBUTORS_URI_FORMAT.format(self.repo_uri)
        json_util.set_value(norm_user, f.USER_EMAIL, user, 'email')
        json_util.set_value(norm_user, f.USER_NAME,  user, 'name')
        json_util.set_value(norm_user, f.USER_ID,  user, 'email')
        norm_user[f.USER_URI] = '/'.join([ url , norm_user[f.USER_ID]])
        
        json_util.set_value(norm_user, f.USER_NUMBER_COMMITS, user, 'commits')
        json_util.set_value(norm_user, f.USER_NUMBER_ADDITIONS, user, 'additions')
        json_util.set_value(norm_user, f.USER_NUMBER_DELETIONS, user, 'deletions')

        norm_user[f.TIMESTAMP] = datetime.datetime.now().isoformat()
    
        user_ids = {
            "name": norm_user.get(f.USER_NAME),
            "email": norm_user.get(f.USER_EMAIL)
        }
        
        user_key = self.user_key_by_ids.get(user_ids)
        if user_key is not None:
            norm_user[f.USER_KEY] = user_key
        norm_user[f.USER_SOURCE_TYPE] = 'gitlab' 


class GitLabBranchNormalizer(pipeline.Normalizer):
    def __init__(self, repo_uri):
        super(GitLabBranchNormalizer, self).__init__()        
        self.repo_uri = repo_uri
        
    def normalize(self, branch, norm_branch):
        norm_branch[f.BRANCH_URI] = GitLabClient.BRANCH_URI_FORMAT.format(self.repo_uri, branch['name'])
        json_util.set_value(norm_branch, f.BRANCH_NAME,         branch, 'name')
        json_util.set_value(norm_branch, f.BRANCH_COMMIT_SHA,   branch, 'commit', 'id')
        json_util.set_time (norm_branch, f.BRANCH_CREATED_TIME, branch, 'commit', 'committed_date')
        norm_branch[f.TIMESTAMP] = norm_branch[f.BRANCH_CREATED_TIME]
        norm_branch[f.BRANCH_SOURCE_TYPE] = 'gitlab'


def get_repo_uri(api_base_url, owner, repo):
    return GitLabClient.REPO_URI_FORMAT.format(api_base_url, owner, repo)

def get_repo(repo_uri, access_token):
    client = GitLabClient(access_token)
    response = client.get(repo_uri)
    repo = json.loads(response.content)
    return repo
