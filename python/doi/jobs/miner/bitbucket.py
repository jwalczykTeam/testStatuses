
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


logger = log_util.get_logger("astro.jobs.miner.BitBucket")


class BitBucketClient(rest.RestClient):
    REPOS_URI_FORMAT           = "{}/repositories/{}"        # params: api_base_url, owner
    REPO_URI_FORMAT            = "{}/repositories/{}/{}"     # params: api_base_url, owner, repo
    USER_URI_FORMAT            = "{}/users/{}"               # params: api_base_url, user_login
    BRANCHES_URI_FORMAT        = "{}/refs/branches"          # params: repo_uri
    BRANCH_URI_FORMAT          = "{}/refs/branches/{}"       # params: repo_uri, branch_name
    COMMITS_URI_FORMAT         = "{}/commits"                # params: repo_uri
    COMMIT_URI_FORMAT          = "{}/commits/{}"             # params: repo_uri, commit_id
    CONTRIBUTORS_URI_FORMAT    = "?"
    ISSUES_URI_FORMAT          = "{}/issues"                 # params: repo_uri
    ISSUE_URI_FORMAT           = "{}/issues/{}"              # params: repo_uri, issue_id



    def __init__(self, access_token=None):
        super(BitBucketClient, self).__init__()        
        self.access_token = access_token
        if access_token is not None:
            self.add_token_auth_header(access_token)

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
            elif status_code == 404:
                error = json.loads(response.content)
                if 'error' in error and 'message' in error['error']:
                    message = error['error']['message']
                    if "Repository has no issue tracker" in message:
                        return response
            
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



class BitBucketReader(pipeline.Reader):
    # Designed not to throw exceptions
    def __init__(self, collection_uri, access_token,
                 details, 
                 since, 
                 from_page, to_page, per_page, **params):
        super(BitBucketReader, self).__init__()        
        self.client = BitBucketClient(access_token)
        self.collection_uri = collection_uri
        self.details = details
        self.since = since
        self.from_page = from_page
        self.to_page = to_page
        self.per_page = per_page
        self.page_no = from_page
        self.last_page = None
        self.params = params        


    def iter(self):
        page_no = self.from_page
        while self.to_page == 0 or page_no <= self.to_page:
            page = self._get_page(page_no)
            if page is None or not page:
                break
            
            for resource in page:
                yield self.extend(resource)
                
            page_no += 1

    def extend(self, resource):
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

        params = {'page': page_no, 'pagelen': self.per_page}
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

        if 'values' not in content:
            return None

        page = page['values']
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



class BitBucketIssueReader(BitBucketReader):
    def __init__(self, api_base_url, access_token, owner, repo, since=None, from_page=1, to_page=0, per_page=100):
        issues_uri =  BitBucketClient.ISSUES_URI_FORMAT.format(get_repo_uri(api_base_url, owner, repo))
        super(BitBucketIssueReader, self).__init__(issues_uri, access_token, 
                                                 False,
                                                 since,
                                                 from_page, to_page, per_page, filter='all', state='all')


class BitBucketBranchReader(BitBucketReader):
    def __init__(self, api_base_url, access_token, owner, repo, since=None, from_page=1, to_page=0, per_page=100):
        branches_uri =  BitBucketClient.BRANCHES_URI_FORMAT.format(get_repo_uri(api_base_url, owner, repo))
        super(BitBucketBranchReader, self).__init__(branches_uri, access_token, 
                                                False, 
                                                since,
                                                from_page, to_page, per_page)


    def extend(self, branch):
        branch['created_at'] = branch['target']['date']
        branch['url'] = branch['links']['self']['href']
        return branch

    
#
# Normalization classes   
#

class BitBucketIssueNormalizer(pipeline.Normalizer):
    def __init__(self, label_field_map, repo_uri):
        super(BitBucketIssueNormalizer, self).__init__()        
        self.label_field_map = label_field_map
        self.repo_uri = repo_uri
        
    def normalize(self, issue, norm_issue):
        url = BitBucketClient.ISSUE_URI_FORMAT.format(self.repo_uri, issue['id'])
        norm_issue[f.ISSUE_URI] = issue['links']['self']['href']
        norm_issue[f.ISSUE_KEY] = issue['links']['self']['href']
        norm_issue[f.ISSUE_ID]  = str(issue['id'])
        
        json_util.set_value(norm_issue, f.ISSUE_STATUS,       issue, 'state')
        json_util.set_time (norm_issue, f.ISSUE_CREATED_TIME, issue, 'created_on')
        json_util.set_time (norm_issue, f.ISSUE_UPDATED_TIME, issue, 'updated_on')
        json_util.set_value(norm_issue, f.ISSUE_CREATOR_URI,  issue, 'reporter', 'links', 'self', 'href')
        json_util.set_value(norm_issue, f.ISSUE_ASSIGNEE_URI, issue, 'assignee', 'links', 'self', 'href')
        json_util.set_value(norm_issue, f.ISSUE_SUMMARY,      issue, 'title')
        json_util.set_value(norm_issue, f.ISSUE_DESCRIPTION,  issue, 'content', 'raw')
        if issue['state'] is 'closed': json_util.set_time (norm_issue, f.ISSUE_CLOSED_TIME,  issue, 'closed_on')
    

        tags = []
        labels = []
        if type(issue['kind']) is not list: 
            labels.append(issue['kind'])
        else:
            for i in issue['kind']: labels.append(i)

        for label in labels:
            label_name = label
            tags.append(label_name)
            if self.label_field_map:
                self._assign_issue_field_by_label(norm_issue, label_name)
        norm_issue[f.ISSUE_TAGS] = tags
        
        if f.ISSUE_TYPES in norm_issue:
            norm_issue[f.ISSUE_RESOLVED_TYPES] = norm_issue[f.ISSUE_TYPES]
        
        norm_issue[f.ISSUE_SOURCE_URI] = uri_util.sub_uri(norm_issue[f.ISSUE_URI], 2)
        norm_issue[f.TIMESTAMP] = norm_issue[f.ISSUE_CREATED_TIME]    
        norm_issue[f.ISSUE_SOURCE_TYPE] = 'bitbucket'
    
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


class BitBucketBranchNormalizer(pipeline.Normalizer):
    def __init__(self, repo_uri):
        super(BitBucketBranchNormalizer, self).__init__()        
        self.repo_uri = repo_uri
        
    def normalize(self, branch, norm_branch):
        norm_branch[f.BRANCH_URI] = BitBucketClient.BRANCH_URI_FORMAT.format(self.repo_uri, branch['name'])
        json_util.set_value(norm_branch, f.BRANCH_URI,          branch, 'links', 'self', 'href')
        json_util.set_value(norm_branch, f.BRANCH_NAME,         branch, 'name')
        json_util.set_value(norm_branch, f.BRANCH_COMMIT_SHA,   branch, 'target', 'id')
        json_util.set_time (norm_branch, f.BRANCH_CREATED_TIME, branch, 'date')
        json_util.set_time (norm_branch, f.TIMESTAMP,           branch, 'date')
        norm_branch[f.BRANCH_SOURCE_TYPE] = 'BitBucket'


def get_repo_uri(api_base_url, owner, repo):
    return BitBucketClient.REPO_URI_FORMAT.format(api_base_url, owner, repo)

def get_repo(repo_uri, access_token):
    client = BitBucketClient(access_token)
    response = client.get(repo_uri)
    repo = json.loads(response.content)
    return repo
