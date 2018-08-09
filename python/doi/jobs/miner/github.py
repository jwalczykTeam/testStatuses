'''
Created on Jun 2, 2016

@author: alberto
'''


from doi.jobs.miner import fields as f
from doi.jobs.miner import pipeline
from doi.util import rest
from doi.util import diff_util
from doi.util import log_util
from doi.util import json_util
from doi.util import time_util
from doi.util import uri_util
from doi.util.cleaning_util import remove_suffix
from requests.exceptions import HTTPError
from requests.utils import parse_header_links
import json
import time
import urllib
import urlparse


logger = log_util.get_logger("astro.jobs.miner.github")


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

    def __init__(self, access_token=None, wait=True):
        super(GitHubClient, self).__init__()
        self.access_token = access_token
        self.wait = wait
        if access_token is not None:
            self.add_token_auth_header(access_token)

    # Overrides RestClient get method. Adds support for waiting for API rate limit reset time
    def get(self, uri, headers=None, timeout=360, **kwargs):
        response = self.session.get(uri, headers=headers, timeout=timeout, **kwargs)
        status_code = response.status_code
        if status_code != 200:
            if status_code == 403:
                error = json.loads(response.content)
                message = error['message']
                if "API rate limit exceeded" in message:
                    if self.wait:
                        rate_limit_reset_time = long(response.headers.get('X-RateLimit-Reset'))
                        self._wait_for_api_rate_limit_reset_time(rate_limit_reset_time)
                        return self.get(uri, headers, timeout, **kwargs)
                    else:
                        logger.info("Waiting flag is {}, break out of API calls".format(
                            self.wait))

            response.raise_for_status()

        return response

    def _wait_for_api_rate_limit_reset_time(self, rate_limit_reset_time):
        now = time.mktime(time.localtime())
        sleep_time = rate_limit_reset_time - now + 1
        rate_limit_reset_strftime = time.strftime(
            "%d %b %Y %H:%M:%S", time.localtime(rate_limit_reset_time))
        logger.warning("API rate limit exceeded. Waiting for %d mins and %d secs. Restarting at %s ...",
                       sleep_time / 60, sleep_time % 60,
                       rate_limit_reset_strftime)
        time.sleep(sleep_time)


class GitHubReader(pipeline.Reader):
    # Designed not to throw exceptions
    def __init__(self, collection_uri, access_token,
                 details,
                 since,
                 from_page, to_page, per_page, **params):
        super(GitHubReader, self).__init__()
        self.client = GitHubClient(access_token)
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
            if page is None:
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
                    logger.info("Get '%s'", resource_uri)
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


class GitHubCommitReader(GitHubReader):
    def __init__(self, api_base_url, access_token, owner, repo, since=None, from_page=1, to_page=0, per_page=100):
        commits_uri = GitHubClient.COMMITS_URI_FORMAT.format(
            get_repo_uri(api_base_url, owner, repo))
        super(GitHubCommitReader, self).__init__(commits_uri, access_token,
                                                 True,
                                                 since,
                                                 from_page, to_page, per_page)


class GitHubContributorReader(GitHubReader):
    def __init__(
        self,
        api_base_url,
        access_token,
        owner,
        repo,
        since=None,
        from_page=1,
        to_page=0,
        per_page=100,
        wait=True,
        details=True):
        contributors_uri = GitHubClient.CONTRIBUTORS_URI_FORMAT.format(
            get_repo_uri(api_base_url, owner, repo))
        super(GitHubContributorReader, self).__init__(
            contributors_uri,
            access_token,
            details,
            since,
            from_page,
            to_page,
            per_page,
            wait=wait)


class GitHubIssueReader(GitHubReader):
    def __init__(
            self,
            api_base_url,
            access_token,
            owner,
            repo,
            since=None,
            from_page=1,
            to_page=0,
            per_page=100,
            wait=True):
        
        issues_uri = GitHubClient.ISSUES_URI_FORMAT.format(
            get_repo_uri(api_base_url, owner, repo))

        super(GitHubIssueReader, self).__init__(
            issues_uri,
            access_token,
            False,
            since,
            from_page,
            to_page,
            per_page,
            filter='all',
            state='all',
            wait=wait)


class GitHubLatestIssueReader(GitHubReader):
    def __init__(
        self,
        api_base_url,
        access_token,
        owner,
        repo,
        since=None,
        from_page=1,
        to_page=1,
        per_page=1,
        wait=True):
        issues_uri = GitHubClient.ISSUES_URI_FORMAT.format(
            get_repo_uri(api_base_url, owner, repo))
        
        super(GitHubLatestIssueReader, self).__init__(
            issues_uri,
            access_token,
            False,
            since,
            from_page,
            to_page=1,
            per_page=1,
            filter='all',
            state='all',
            sort='created',
            wait=wait)


class GitHubStreamIssueReader(GitHubReader):
    def __init__(
        self,
        api_base_url,
        access_token,
        owner,
        repo,
        since=None,
        from_page=1,
        to_page=0,
        per_page=100,
        wait=True):
        issues_uri = GitHubClient.ISSUES_URI_FORMAT.format(
            get_repo_uri(api_base_url, owner, repo))
        super(GitHubStreamIssueReader, self).__init__(
            issues_uri,
            access_token,
            False,
            since,
            from_page,
            to_page,
            per_page,
            filter='all',
            state='all',
            sort='created',
            wait=wait)


class GitHubPullReader(GitHubReader):
    def __init__(self, api_base_url, access_token, owner, repo, since=None, from_page=1, to_page=0, per_page=100):
        pulls_uri = GitHubClient.PULLS_URI_FORMAT.format(get_repo_uri(api_base_url, owner, repo))
        super(GitHubPullReader, self).__init__(pulls_uri, access_token,
                                               True,
                                               since,
                                               from_page, to_page, per_page)
        # from_page, to_page, per_page, filter='all', state='all')

    def extend(self, pull):
        commits_uri = pull['commits_url']
        commits_reader = GitHubReader(commits_uri,
                                      self.client.access_token,
                                      False,
                                      self.since,
                                      self.from_page,
                                      self.to_page,
                                      self.per_page)
        try:
            commit_urls = []
            for commit in commits_reader:
                commit_urls.append(commit['url'])
            pull['commit_urls'] = commit_urls
            return pull
        finally:
            commits_reader.close()


class GitHubBranchReader(GitHubReader):
    def __init__(self, api_base_url, access_token, owner, repo, since=None, from_page=1, to_page=0, per_page=100):
        branches_uri = GitHubClient.BRANCHES_URI_FORMAT.format(
            get_repo_uri(api_base_url, owner, repo))
        super(GitHubBranchReader, self).__init__(branches_uri, access_token,
                                                 False,
                                                 since,
                                                 from_page, to_page, per_page)

    def extend(self, branch):
        commit_uri = branch['commit']['url']
        response = self.client.get(commit_uri)
        commit = json.loads(response.content)
        created_at = commit['commit']['committer']['date']
        branch['created_at'] = created_at
        branch['url'] = GitHubClient.BRANCH_URI_FORMAT.format(self.collection_uri, branch['name'])
        return branch


class GitHubCommitCommentReader(GitHubReader):
    def __init__(self, api_base_url, access_token, owner, repo, since=None, from_page=1, to_page=0, per_page=100):
        comments_uri = GitHubClient.COMMENTS_URI_FORMAT.format(
            get_repo_uri(api_base_url, owner, repo))
        super(GitHubCommitCommentReader, self).__init__(comments_uri, access_token,
                                                        False,
                                                        since,
                                                        from_page, to_page, per_page)


class GitHubIssueCommentReader(GitHubReader):
    def __init__(self, api_base_url, access_token, owner, repo, since=None, from_page=1, to_page=0, per_page=100):
        issues_comments_uri = GitHubClient.ISSUES_COMMENTS_URI_FORMAT.format(
            get_repo_uri(api_base_url, owner, repo))
        super(GitHubIssueCommentReader, self).__init__(issues_comments_uri, access_token,
                                                       False,
                                                       since,
                                                       from_page, to_page, per_page)


#
# Normalization classes
#

class GitHubCommitNormalizer(pipeline.Normalizer):
    def __init__(self, find_issue_keys=None, issue_key_finders=None):
        super(GitHubCommitNormalizer, self).__init__()
        self.issue_key_finders = issue_key_finders
        self.find_issue_keys = find_issue_keys

    def normalize(self, commit, norm_commit):
        json_util.set_value(norm_commit, f.COMMIT_URI, commit, 'url')
        json_util.set_value(norm_commit, f.COMMIT_ID,  commit, 'sha')

        commit_commit = commit['commit']
        json_util.set_value(norm_commit,  f.COMMIT_MESSAGE,  commit_commit, 'message')

        # Commit/author
        commit_author = commit_commit['author']
        json_util.set_value(norm_commit, f.COMMIT_AUTHOR_NAME,  commit_author, 'name')
        json_util.set_value(norm_commit, f.COMMIT_AUTHOR_EMAIL, commit_author, 'email')
        json_util.set_time(norm_commit, f.COMMIT_AUTHORED_TIME,  commit_author, 'date')

        # Commit/committer
        commit_committer = commit_commit['committer']
        json_util.set_value(norm_commit, f.COMMIT_COMMITTER_NAME,  commit_committer, 'name')
        json_util.set_value(norm_commit, f.COMMIT_COMMITTER_EMAIL, commit_committer, 'email')
        json_util.set_time(norm_commit, f.COMMIT_COMMITTED_TIME,  commit_committer, 'date')

        # Author
        author = commit.get('author')
        if author is not None:
            json_util.set_value(norm_commit, f.COMMIT_AUTHOR_ID, author, 'id')
            json_util.set_value(norm_commit, f.COMMIT_AUTHOR_LOGIN, author, 'login')

        # Committer
        committer = commit.get('committer')
        if committer is not None:
            json_util.set_value(norm_commit, f.COMMIT_COMMITTER_ID, committer, 'id')
            json_util.set_value(norm_commit, f.COMMIT_COMMITTER_LOGIN, committer, 'login')

        norm_commit[f.COMMIT_SOURCE_URI] = uri_util.sub_uri(norm_commit[f.COMMIT_URI], 2)
        if self.issue_key_finders is not None:
            norm_commit[f.COMMIT_ISSUE_KEYS] = self.find_issue_keys(norm_commit)
        norm_commit[f.TIMESTAMP] = norm_commit[f.COMMIT_COMMITTED_TIME]
        norm_commit[f.COMMIT_SOURCE_TYPE] = 'github'


class GitHubCommittedFileNormalizer(pipeline.Normalizer):
    def __init__(self, issue_key_finders):
        super(GitHubCommittedFileNormalizer, self).__init__()
        self.issue_key_finders = issue_key_finders

    def normalize(self, file_, norm_file):
        json_util.set_value(norm_file, f.FILE_PATH,               file_, 'filename')
        json_util.set_value(norm_file, f.FILE_ACTION,             file_, 'status')
        json_util.set_value(norm_file, f.FILE_ADDED_LINE_COUNT,   file_, 'additions')
        json_util.set_value(norm_file, f.FILE_DELETED_LINE_COUNT, file_, 'deletions')
        json_util.set_value(norm_file, f.FILE_CHANGED_LINE_COUNT, file_, 'changes')
        json_util.set_value(norm_file, f.FILE_LINE_COUNT,         file_, 'lines')
        json_util.set_value(norm_file, f.FILE_PREVIOUS_PATH,      file_, 'previous_filename')

        if 'patch' in file_:
            diff = diff_util.parse_patch(file_['patch'])
            out_changes = []
            for hunk in diff.hunks:
                for in_change in hunk.changes:
                    out_changes.append(self._normalize_change(in_change))
            norm_file[f.FILE_CHANGES] = out_changes

    def _normalize_change(self, change):
        out_change = {}
        out_change[f.CHANGE_TYPE] = change.kind
        out_change[f.CHANGE_LINE_NUMBER] = change.line_number
        out_change[f.CHANGE_LINE] = change.line
        return out_change


class GitHubIssueNormalizer(pipeline.Normalizer):
    def __init__(self, label_field_map):
        super(GitHubIssueNormalizer, self).__init__()
        self.label_field_map = label_field_map

    def normalize(self, issue, norm_issue):
        norm_issue[f.ISSUE_URI] = issue['url']
        norm_issue[f.ISSUE_KEY] = issue['url']
        norm_issue[f.ISSUE_ID] = str(issue['number'])

        json_util.set_value(norm_issue, f.ISSUE_STATUS,       issue, 'state')
        json_util.set_time(norm_issue, f.ISSUE_CREATED_TIME, issue, 'created_at')
        json_util.set_time(norm_issue, f.ISSUE_UPDATED_TIME, issue, 'updated_at')
        json_util.set_time(norm_issue,  f.ISSUE_CLOSED_TIME,  issue, 'closed_at')
        json_util.set_value(norm_issue, f.ISSUE_CREATOR_URI,  issue, 'user', 'url')
        json_util.set_value(norm_issue, f.ISSUE_ASSIGNEE_URI, issue, 'assignee', 'url')
        json_util.set_value(norm_issue, f.ISSUE_PULL_URI,     issue, 'pull_request', 'url')
        json_util.set_value(norm_issue, f.ISSUE_SUMMARY,      issue, 'title')
        json_util.set_value(norm_issue, f.ISSUE_DESCRIPTION,  issue, 'body')

        tags = []
        for label in issue['labels']:
            label_name = label['name']
            tags.append(label_name)
            if self.label_field_map:
                self._assign_issue_field_by_label(norm_issue, label_name)
        norm_issue[f.ISSUE_TAGS] = tags

        if f.ISSUE_TYPES in norm_issue:
            norm_issue[f.ISSUE_RESOLVED_TYPES] = norm_issue[f.ISSUE_TYPES]

        norm_issue[f.ISSUE_SOURCE_URI] = uri_util.sub_uri(norm_issue[f.ISSUE_URI], 2)
        norm_issue[f.TIMESTAMP] = norm_issue[f.ISSUE_CREATED_TIME]
        norm_issue[f.ISSUE_SOURCE_TYPE] = 'github'

    def _assign_issue_field_by_label(self, norm_issue, label_name):
        field = self.label_field_map.get(label_name)
        if field is not None:
            field_name, field_value = field
            if field_name in [f.ISSUE_TYPES, f.ISSUE_COMPONENTS,
                              f.ISSUE_VERSIONS, f.ISSUE_FIX_VERSIONS]:
                # If field is a valid array field
                if field_name not in norm_issue:
                    norm_issue[field_name] = [field_value]
                else:
                    norm_issue[field_name].append(field_value)
            elif field_name in [f.ISSUE_PRIORITY, f.ISSUE_RESOLUTION,
                                f.ISSUE_SEVERITY, f.ISSUE_STATUS]:
                # If field is a valid single value field
                norm_issue[field_name] = field_value
            else:
                # If field is not valid
                raise ValueError("Field not allowed in labels map: {}".format(field))


class GitHubUserNormalizer(pipeline.Normalizer):
    def __init__(self, user_key_by_ids):
        super(GitHubUserNormalizer, self).__init__()
        self.user_key_by_ids = user_key_by_ids

    def normalize(self, user, norm_user):
        json_util.set_value(norm_user, f.USER_URI,   user, 'url')
        json_util.set_value(norm_user, f.USER_LOGIN, user, 'login')
        json_util.set_value(norm_user, f.USER_ID,    user, 'id')
        json_util.set_value(norm_user, f.USER_EMAIL, user, 'email')
        json_util.set_value(norm_user, f.USER_NAME,  user, 'name')
        json_util.set_value(norm_user, f.USER_TYPE,  user, 'type')

        # Company is free text that requires normalization
        company = user.get('company')
        if company is not None:
            normalized_company = self._normalize_company(company)
            json_util.set_value(norm_user, f.USER_COMPANY, normalized_company)

        json_util.set_value(norm_user, f.USER_PUBLIC_REPO_COUNT, user, 'public_repos')
        json_util.set_value(norm_user, f.USER_PUBLIC_GIST_COUNT, user, 'public_gists')
        json_util.set_value(norm_user, f.USER_FOLLOWER_COUNT,    user, 'followers')
        json_util.set_value(norm_user, f.USER_FOLLOWING_COUNT,   user, 'following')
        json_util.set_time(norm_user,  f.USER_CREATED_TIME,      user, 'created_at')
        json_util.set_time(norm_user,  f.USER_UPDATED_TIME,      user, 'updated_at')

        if f.USER_ID in norm_user:
            norm_user[f.USER_ID] = str(norm_user[f.USER_ID])

        user_ids = {
            "name": norm_user.get(f.USER_NAME),
            "email": norm_user.get(f.USER_EMAIL)
        }
        
        user_key = self.user_key_by_ids.get(user_ids)
        if user_key is not None:
            norm_user[f.USER_KEY] = user_key

        norm_user[f.USER_SOURCE_URI] = uri_util.sub_uri(norm_user[f.USER_URI], 1)
        norm_user[f.TIMESTAMP] = norm_user[f.USER_CREATED_TIME]
        norm_user[f.USER_SOURCE_TYPE] = 'github'

    def _normalize_company(self, company):
        lowercase_company = company.lower()
        # Supersimple for the moment, need to do much better using a world-wide company registry
        return remove_suffix(lowercase_company, [".com", " inc", " inc.", " co., ltd."])


class GitHubPullNormalizer(pipeline.Normalizer):
    def __init__(self):
        super(GitHubPullNormalizer, self).__init__()

    def normalize(self, pull, norm_pull):
        json_util.set_value(norm_pull, f.PULL_URI,                pull, 'url')
        json_util.set_value(norm_pull, f.PULL_ID,                 pull, 'id')
        json_util.set_value(norm_pull, f.PULL_STATE,              pull, 'state')
        json_util.set_value(norm_pull, f.PULL_LOCKED,             pull, 'locked')
        json_util.set_value(norm_pull, f.PULL_TITLE,              pull, 'title')
        json_util.set_value(norm_pull, f.PULL_USER_URI,           pull, 'user', 'url')
        json_util.set_time(norm_pull, f.PULL_CREATED_TIME,       pull, 'created_at')
        json_util.set_time(norm_pull, f.PULL_CLOSED_TIME,        pull, 'closed_at')
        json_util.set_time(norm_pull, f.PULL_UPDATED_TIME,       pull, 'updated_at')
        json_util.set_time(norm_pull, f.PULL_MERGED_TIME,        pull, 'merged_at')
        json_util.set_value(norm_pull, f.PULL_HEAD_ID,            pull, 'head', 'sha')
        json_util.set_value(norm_pull, f.PULL_HEAD_REPO_URI,      pull, 'head', 'repo', 'url')
        if f.PULL_HEAD_REPO_URI in norm_pull and f.PULL_HEAD_ID in norm_pull:
            norm_pull[f.PULL_HEAD_URI] = norm_pull[f.PULL_HEAD_REPO_URI] + \
                "/commits/" + norm_pull[f.PULL_HEAD_ID]
        json_util.set_value(norm_pull, f.PULL_BASE_ID,            pull, 'base', 'sha')
        json_util.set_value(norm_pull, f.PULL_BASE_REPO_URI,      pull, 'base', 'repo', 'url')
        if f.PULL_BASE_REPO_URI in norm_pull and f.PULL_BASE_ID in norm_pull:
            norm_pull[f.PULL_BASE_URI] = norm_pull[f.PULL_BASE_REPO_URI] + \
                "/commits/" + norm_pull[f.PULL_BASE_ID]
        json_util.set_value(norm_pull, f.PULL_MERGED,             pull, 'merged')
        json_util.set_value(norm_pull, f.PULL_MERGEABLE,          pull, 'mergeable')
        json_util.set_value(norm_pull, f.PULL_MERGEABLE_STATE,    pull, 'mergeable_state')
        json_util.set_value(norm_pull, f.PULL_COMMIT_COUNT,       pull, 'commits')
        json_util.set_value(norm_pull, f.PULL_ADDED_LINE_COUNT,   pull, 'additions')
        json_util.set_value(norm_pull, f.PULL_DELETED_LINE_COUNT, pull, 'deletions')
        json_util.set_value(norm_pull, f.PULL_CHANGED_FILE_COUNT, pull, 'changed_files')
        json_util.set_value(norm_pull, f.PULL_COMMIT_URIS,        pull, 'commit_urls')
        norm_pull[f.TIMESTAMP] = norm_pull[f.PULL_CREATED_TIME]
        norm_pull[f.PULL_SOURCE_TYPE] = 'github'


class GitHubBranchNormalizer(pipeline.Normalizer):
    def __init__(self):
        super(GitHubBranchNormalizer, self).__init__()

    def normalize(self, branch, norm_branch):
        json_util.set_value(norm_branch, f.BRANCH_URI,          branch, 'url')
        json_util.set_value(norm_branch, f.BRANCH_NAME,         branch, 'name')
        json_util.set_value(norm_branch, f.BRANCH_COMMIT_URI,   branch, 'commit', 'url')
        json_util.set_time(norm_branch, f.BRANCH_CREATED_TIME, branch, 'created_at')
        norm_branch[f.TIMESTAMP] = norm_branch[f.BRANCH_CREATED_TIME]
        norm_branch[f.BRANCH_SOURCE_TYPE] = 'github'


class GitHubCommitCommentNormalizer(pipeline.Normalizer):
    def __init__(self):
        super(GitHubCommitCommentNormalizer, self).__init__()

    def normalize(self, comment, norm_comment):
        json_util.set_value(norm_comment, f.COMMENT_URI,          comment, 'url')
        json_util.set_value(norm_comment, f.COMMENT_BODY,         comment, 'body')
        json_util.set_time(norm_comment, f.COMMENT_CREATED_TIME, comment, 'created_at')
        json_util.set_time(norm_comment, f.COMMENT_UPDATED_TIME, comment, 'updated_at')
        json_util.set_value(norm_comment, f.COMMENT_AUTHOR_URI,   comment, 'user', 'url')
        json_util.set_value(norm_comment, f.COMMENT_POSITION,     comment, 'position')
        json_util.set_value(norm_comment, f.COMMENT_LINE,         comment, 'line')
        json_util.set_value(norm_comment, f.COMMENT_PATH,         comment, 'path')
        repo_uri = uri_util.sub_uri(comment['url'], 2)
        norm_comment[f.COMMENT_COMMIT_URI] = GitHubClient.COMMIT_URI_FORMAT.format(
            repo_uri, comment['commit_id'])


class GitHubIssueCommentNormalizer(pipeline.Normalizer):
    def __init__(self):
        super(GitHubIssueCommentNormalizer, self).__init__()

    def normalize(self, comment, norm_comment):
        json_util.set_value(norm_comment, f.ISSUE_COMMENT_URI,          comment, 'url')
        json_util.set_value(norm_comment, f.ISSUE_COMMENT_ISSUE_URI,    comment, 'issue_url')
        json_util.set_value(norm_comment, f.ISSUE_COMMENT_BODY,         comment, 'body')
        json_util.set_time(norm_comment, f.ISSUE_COMMENT_CREATED_TIME, comment, 'created_at')
        json_util.set_time(norm_comment, f.ISSUE_COMMENT_UPDATED_TIME, comment, 'updated_at')
        json_util.set_value(norm_comment, f.ISSUE_COMMENT_AUTHOR_URI,   comment, 'user', 'url')


def get_repo_uri(api_base_url, owner, repo):
    return GitHubClient.REPO_URI_FORMAT.format(api_base_url, owner, repo)


def get_repo(repo_uri, access_token):
    client = GitHubClient(access_token)
    response = client.get(repo_uri)
    repo = json.loads(response.content)
    return repo
