
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


logger = log_util.get_logger("astro.jobs.miner.BitBucketServer")


class BitBucketServerClient(rest.RestClient):
    REPOS_URI_FORMAT           = "{}/projects/{}/repos"      # params: api_base_url, owner
    REPO_URI_FORMAT            = "{}/projects/{}/repos/{}"            # params: api_base_url, owner, repo
    USERS_URI_FORMAT           = "{}/users"                  # params: api_base_url
    USER_URI_FORMAT            = "{}/users/{}"               # params: api_base_url, user_login
    BRANCHES_URI_FORMAT        = "{}/branches"               # params: repo_uri
    BRANCH_DEFAULT_URI_FORMAT  = "{}/branches/default"       # params: repo_uri
    COMMITS_URI_FORMAT         = "{}/commits"                # params: repo_uri
    COMMIT_URI_FORMAT          = "{}/commits/{}"             # params: repo_uri, commit_id
    


    def __init__(self, access_token=None):
        super(BitBucketServerClient, self).__init__()        
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


class BitBucketServerReader(pipeline.Reader):
    # Designed not to throw exceptions
    def __init__(self, collection_uri, access_token,
                 details, 
                 since, 
                 from_page, to_page, per_page, **params):
        super(BitBucketServerReader, self).__init__()        
        self.client = BitBucketServerClient(access_token)
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
            page,wrapper = self._get_page(page_no)
            if page is None or not page:
                break
            
            for resource in page:
                yield self.extend(resource)
            
            if wrapper['isLastPage'] is True: 
                break
            else: 
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

        params = {'page': page_no, 'limit': self.per_page}
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

        wrapper = page
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

        return page,wrapper
    
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


class BitBucketServerBranchReader(BitBucketServerReader):
    def __init__(self, api_base_url, access_token, owner, repo, since=None, from_page=1, to_page=0, per_page=100):
        branches_uri =  BitBucketServerClient.BRANCHES_URI_FORMAT.format(get_repo_uri(api_base_url, owner, repo))
        super(BitBucketServerBranchReader, self).__init__(branches_uri, access_token, 
                                                False, 
                                                since,
                                                from_page, to_page, per_page)

        def extend(self, branch):
            return branch

    
class BitBucketServerBranchNormalizer(pipeline.Normalizer):
    def __init__(self, repo_uri):
        super(BitBucketServerBranchNormalizer, self).__init__()        
        self.repo_uri = repo_uri
        
    def normalize(self, branch, norm_branch):
        norm_branch[f.BRANCH_URI] = BitBucketServerClient.BRANCHES_URI_FORMAT.format(self.repo_uri)
        json_util.set_value(norm_branch, f.BRANCH_NAME,         branch, 'name')
        json_util.set_value(norm_branch, f.BRANCH_COMMIT_SHA,   branch, 'latestCommit')
        norm_branch[f.TIMESTAMP] = "0" # data not available
        norm_branch[f.BRANCH_SOURCE_TYPE] = 'BitBucketServer'


def get_repo_uri(api_base_url, owner, repo):
    return BitBucketServerClient.REPO_URI_FORMAT.format(api_base_url, owner, repo)

def get_repo(repo_uri, access_token):
    client = BitBucketServerClient(access_token)
    response = client.get(repo_uri)
    repo = json.loads(response.content)
    return repo

