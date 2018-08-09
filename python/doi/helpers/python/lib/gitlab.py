
try:
    # Astro
    from doi.jobs.miner import fields as f
    from doi.jobs.miner import pipeline
    from doi.util import rest
    from doi.util import diff_util
    from doi.util import log_util
    from doi.util import json_util
    from doi.util import time_util
    from doi.util import uri_util
    from doi.util.cleaning_util import remove_suffix
except ImportError:
    # Rudi
    from rudi.miner.doi.jobs.miner import fields as f
    from rudi.miner.doi.jobs.miner import pipeline
    from rudi.miner.doi.util import rest
    from rudi.miner.doi.util import diff_util
    from rudi.miner.doi.util import log_util
    from rudi.miner.doi.util import json_util
    from rudi.miner.doi.util import time_util
    from rudi.miner.doi.util import uri_util
    from rudi.miner.doi.util.cleaning_util import remove_suffix

from requests.exceptions import HTTPError
from requests.utils import parse_header_links
import json
import time
import urllib
import urlparse

logger = log_util.get_logger("helpers.python.lib.github")


class GitLabClient(rest.RestClient):
    # params: api_base_url, repo_owner, repo_name
    REPO_URI_FORMAT = "{}/projects/{}%2F{}"
    USERS_URI_FORMAT = "{}/users"                   # params: api_base_url
    # params: api_base_url, user_login
    USER_URI_FORMAT = "{}/users?username={}"
    BRANCHES_URI_FORMAT = "{}/repository/branches"     # params: repo_uri
    # params: repo_uri, branch_name
    BRANCH_URI_FORMAT = "{}/repository/branches/{}"
    CONTRIBUTORS_URI_FORMAT = "{}/repository/contributors"  # params: repo_uri
    ISSUES_URI_FORMAT = "{}/issues"                  # params: repo_uri
    # params: repo_uri, issue_id
    ISSUE_URI_FORMAT = "{}/issues/{}"
    PULLS_URI_FORMAT = "{}/merge_requests"          # params: repo_uri
    PULL_URI_FORMAT = "{}/merge_requests/{}"        # params: repo_uri, pull_id
    # params: repo_uri, pull_id
    PULL_COMMITS_URI_FORMAT = "{}/merge_requests/{}/commits"
    COMMITS_URI_FORMAT = "{}/repository/commits"      # params: repo_uri
    # params: repo_uri, commit_id
    COMMIT_URI_FORMAT = "{}/repository/commits/{}"
    # params: repo_uri, file_path, ref
    FILE_URI_FORMAT = "{}/repository/files?{}&{}"

    def __init__(self, source=None, wait=True):
        super(GitLabClient, self).__init__()
        self.source = source
        self.access_token = source.get('access_token')
        self.wait = wait
        if self.access_token is not None:
            if source.get('service_id') == 'gitlab':
                self.add_header("Authorization", "Bearer {}"
                                .format(self.access_token))
            else:
                self.add_header("PRIVATE-TOKEN", self.access_token)

    # Overrides RestClient get method. Adds support for waiting for API rate
    # limit reset time
    def get(self, uri, headers=None, timeout=360, data=None, **kwargs):
        response = self.session.get(
            uri, headers=headers, timeout=timeout, **kwargs)

        return self._check_response(
            uri, self.get, response, headers, timeout, data, **kwargs)

    # Overrides RestClient post method. Adds support for waiting for API rate
    # limit reset time
    def post(self, uri, headers=None, timeout=360, data=None, **kwargs):
        if headers is None:
            headers = {'content-type': 'application/json'}
        response = self.session.post(
            uri, data=data, headers=headers, timeout=timeout, **kwargs)

        return self._check_response(
            uri, self.post, response, headers, timeout, data, **kwargs)

    # Overrides RestClient put method. Adds support for waiting for API rate
    # limit reset time
    def put(self, uri, headers=None, timeout=360, data=None, **kwargs):
        if headers is None:
            headers = {'content-type': 'application/json'}
        response = self.session.put(
            uri, data=data, headers=headers, timeout=timeout, **kwargs)

        return self._check_response(
            uri, self.put, response, headers, timeout, data, **kwargs)


    # Overrides RestClient delete method. Adds support for waiting for API rate
    # limit reset time
    def delete(self, uri, headers=None, timeout=360, data=None, **kwargs):
        response = self.session.delete(
            uri, headers=headers, timeout=timeout, **kwargs)

        return self._check_response(
            uri, self.delete, response, headers, timeout, data, **kwargs)

    def _check_response(self, uri, function, response, headers, timeout, data, **kwargs):

        status_code = response.status_code

        if status_code != 200:
            if status_code == 403:
                error = json.loads(response.content)
                message = error['message']
                if "API rate limit exceeded" in message:
                    logger.warning(
                        'API rate limit exceeded for uri: {}'.format(uri))
                    if self.wait:
                        rate_limit_reset_time = long(
                            response.headers.get('X-RateLimit-Reset'))
                        self._wait_for_api_rate_limit_reset_time(
                            rate_limit_reset_time)
                        return function(uri, headers, timeout, data, **kwargs)
                    else:
                        logger.debug(
                            "Waiting flag is {}, break out".format(
                                self.wait))

            response.raise_for_status()

        return response

    def _wait_for_api_rate_limit_reset_time(self, rate_limit_reset_time):
        now = time.mktime(time.localtime())
        sleep_time = rate_limit_reset_time - now + 1
        rate_limit_reset_strftime = time.strftime(
            "%d %b %Y %H:%M:%S", time.localtime(rate_limit_reset_time))
        logger.warning(
            "API rate limit exceeded." +
            "Waiting for %d mins and %d secs. Restarting at %s ...",
            sleep_time / 60, sleep_time % 60,
            rate_limit_reset_strftime)
        time.sleep(sleep_time)


class GitLabReader(pipeline.Reader):
    # Designed not to throw exceptions
    def __init__(self, collection_uri, source,
                 details,
                 since,
                 from_page, to_page, per_page, **params):
        super(GitLabReader, self).__init__()
        self.client = GitLabClient(
            source=source,
            wait=params.get('wait', True))
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
            params['since'] = time_util.utc_millis_to_iso8601_datetime(
                self.since)
        params.update(self.params)

        query_uri = "{}?{}".format(
            self.collection_uri, urllib.urlencode(params))
        logger.debug("Get '%s'", query_uri)
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


def get_repo_uri(api_base_url, owner, repo):
    return GitLabClient.REPO_URI_FORMAT.format(api_base_url, owner, repo)


def get_repo(repo_uri, source, wait=True):
    client = GitLabClient(source, wait=wait)
    response = client.get(repo_uri)
    repo = json.loads(response.content)
    return repo
