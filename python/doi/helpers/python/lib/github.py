
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


class GitHubClient(rest.RestClient):
    USERS_URI_FORMAT = "{}/users"           # params: api_base_url
    USER_URI_FORMAT = "{}/users/{}"        # params: api_base_url, user_login
    REPOS_URI_FORMAT = "{}/repos/{}"        # params: api_base_url, owner
    REPO_URI_FORMAT = "{}/repos/{}/{}"     # params: api_base_url, owner, repo
    BRANCHES_URI_FORMAT = "{}/branches"        # params: repo_uri
    BRANCH_URI_FORMAT = "{}/branches/{}"     # params: repo_uri, branch_name
    COMMENTS_URI_FORMAT = "{}/comments"        # params: repo_uri
    COMMITS_URI_FORMAT = "{}/commits"         # params: repo_uri
    COMMIT_URI_FORMAT = "{}/commits/{}"      # params: repo_uri, commit_id
    CONTRIBUTORS_URI_FORMAT = "{}/contributors"    # params: repo_uri
    EVENTS_URI_FORMAT = "{}/events"          # params: repo_uri
    ISSUES_URI_FORMAT = "{}/issues"          # params: repo_uri
    ISSUE_URI_FORMAT = "{}/issues/{}"       # params: repo_uri, issue_id
    ISSUES_COMMENTS_URI_FORMAT = "{}/issues/comments"  # params: repo_uri
    ISSUES_EVENTS_URI_FORMAT = "{}/issues/events"   # params: repo_uri
    PULLS_URI_FORMAT = "{}/pulls"           # params: repo_uri

    def __init__(self, source=None, wait=True):
        super(GitHubClient, self).__init__()
        self.source = source
        self.access_token = source.get('access_token')
        self.wait = wait
        if self.access_token is not None:
            self.add_token_auth_header(self.access_token)

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

    # Overrides RestClient patch method. Adds support for waiting for API rate
    # limit reset time
    def patch(self, uri, headers=None, timeout=360, data=None, **kwargs):
        if headers is None:
            headers = {'content-type': 'application/json'}
        response = self.session.patch(
            uri, data=data, headers=headers, timeout=timeout, **kwargs)

        return self._check_response(
            uri, self.patch, response, headers, timeout, data, **kwargs)

    # Overrides RestClient delete method. Adds support for waiting for API rate
    # limit reset time
    def delete(self, uri, headers=None, timeout=360, data=None, **kwargs):
        response = self.session.delete(
            uri, headers=headers, timeout=timeout, **kwargs)

        return self._check_response(
            uri, self.delete, response, headers, timeout, data, **kwargs)

    def _check_response(self, uri, function, response, headers, timeout, data, **kwargs):

        status_code = response.status_code

        if self._check_api_limit(uri, response):
            return function(uri, headers, timeout, data, **kwargs)

        if status_code != 200:
            if status_code == 403:
                try:
                    error = json.loads(response.content)
                except Exception as e:

                    if self._check_api_limit(uri, response):
                        return function(uri, headers, timeout, data, **kwargs)

                    logger.error(e.message)
                    raise ValueError(
                        "Response content from {} not json parseable: {}".format(
                            uri, response.content))

                message = error['message']
                if "API rate limit exceeded" in message:
                    logger.warning(
                        'API rate limit exceeded for uri: {}'.format(uri))
                    if self.wait:
                        rate_limit_reset_time = long(
                            response.headers.get('X-RateLimit-Reset'))
                        self._wait_for_api_rate_limit_reset_time(
                            uri, rate_limit_reset_time)
                        return function(uri, headers, timeout, data, **kwargs)
                    else:
                        logger.info(
                            "Waiting flag is {}, breaking out".format(
                                self.wait))
                elif "abuse detection mechanism" in message:
                    logger.warning(
                        'Abuse detection mechanism triggered \
                        for uri: {}'.format(uri))
                    if self.wait:
                        retry_time = long(
                            response.headers.get('Retry-After'))
                        self._wait_for_retry_time_reset(
                            uri, retry_time)
                        return function(uri, headers, timeout, data, **kwargs)
                    else:
                        logger.info(
                            "Waiting flag is {}, breaking out".format(
                                self.wait))

            response.raise_for_status()

        return response

    def _check_api_limit(self, uri, response):
        if 'X-RateLimit-Remaining' in response.headers:
            remaining_limit = long(response.headers['X-RateLimit-Remaining'])
            if remaining_limit < 500:

                # When a user's account is suspended, their API
                # token's limit is always 0, even after we wait
                if "account was suspended" in response.content:
                    raise Exception(
                        "Github account was suspended, try another token.")

                if self.wait:
                    rate_limit_reset_time = long(
                        response.headers.get('X-RateLimit-Reset'))
                    self._wait_for_api_rate_limit_reset_time(
                        uri, rate_limit_reset_time)
                    return True
        return False

    def _wait_for_api_rate_limit_reset_time(self, uri, rate_limit_reset_time):
        now = time.mktime(time.localtime())
        sleep_time = rate_limit_reset_time - now + 1
        rate_limit_reset_strftime = time.strftime(
            "%d %b %Y %H:%M:%S", time.localtime(rate_limit_reset_time))
        logger.warning(
            "API rate limit exceeded for uri: {}. \
            Waiting for %d mins and %d secs. Restarting at %s ...".format(uri),
            sleep_time / 60, sleep_time % 60,
            rate_limit_reset_strftime)
        time.sleep(sleep_time)

    def _wait_for_retry_time_reset(self, uri, retry_time):
        logger.warning(
            "Abuse detection mechanism triggered for uri: {}. \
            Waiting for %d secs.".format(uri),
            retry_time)
        time.sleep(retry_time)


class GitHubReader(pipeline.Reader):
    # Designed not to throw exceptions

    def __init__(self, collection_uri, source,
                 details,
                 since,
                 from_page, to_page, per_page, **params):
        super(GitHubReader, self).__init__()
        self.client = GitHubClient(source, wait=params.get('wait', True))
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
            self.params.pop('wait')

    def iter(self):
        page_no = self.from_page
        while self.to_page == 0 or page_no <= self.to_page:
            page = self._get_page(page_no)
            if page is None or not page:
                break

            if 'single_value' in self.params:
                yield page
            else:
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
            if isinstance(page, list):
                for resource in page:
                    resource_uri = resource['url']
                    try:
                        logger.debug("Get '%s'", resource_uri)
                        response = self.client.get(resource_uri)
                        resources.append(json.loads(response.content))
                    except HTTPError as e:
                        if self._can_ignore(e):
                            continue
                        else:
                            raise e
                page = resources
            else:
                resource_uri = page['url']
                try:
                    logger.debug("Get '%s'", resource_uri)
                    response = self.client.get(resource_uri)
                except HTTPError as e:
                    if not self._can_ignore(e):
                        raise e
                    page = json.loads(response.content)

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
        else:
            self.to_page = 1


def get_repo_uri(api_base_url, owner, repo):
    return GitHubClient.REPO_URI_FORMAT.format(api_base_url, owner, repo)


def get_repo(repo_uri, source, wait=True):
    client = GitHubClient(source, wait=wait)
    response = client.get(repo_uri)
    try:
        repo = json.loads(response.content)
    except Exception as e:
        logger.error(e.message)
        raise ValueError(
            "Response content from {} not json parseable: {}".format(
                repo_uri, response.content))
    return repo
