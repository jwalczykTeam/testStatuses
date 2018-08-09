
from doi.jobs.miner import github, gitlab, jira, rtc
from doi.jobs.miner import resourcedb, git_miner, jira_miner, rtc_miner
from doi.jobs.miner import git2repo as gitrepo
from doi.jobs.miner import fields as f
from doi.jobs.miner import joiner
from doi.jobs.miner import counter
from doi.util import uri_util, file_util, cf_util, log_util, uuid_util
from doi import constants

import os
from operator import itemgetter
import itertools
import json

logger = log_util.get_logger("astro.jobs.miner.stream_miner")


class PullRequest(object):

    def __init__(self, config):

        self.config = config
        self.sources = config['input']['sources']
        self.index = config['full_name']

    def set_vars(self, source):

        self.api_base_url = self.miner_class.get_source_api_base_url(source)
        self.access_token = self.miner_class.get_source_access_token(source)
        self.repo_owner = self.miner_class.get_source_repo_owner(source)
        self.repo_name = self.miner_class.get_source_repo_name(source)
        self.repo_uri = self.miner_class.get_source_repo_uri_with_formatter(
            source,
            self.formatter)

    def get_pull_reader(self, from_page=1, to_page=0, per_page=100):
        return self.pull_reader(
            self.api_base_url,
            self.access_token,
            self.repo_owner,
            self.repo_name,
            from_page=from_page,
            to_page=to_page,
            per_page=per_page,
            wait=False)

    def get_pull_normalizer(self):
        return self.pull_normalizer()

    def get_new_pulls_from_source(self):
        to_page = 1
        from_page = 1
        new_pulls = []

        creds = cf_util.get_db_credentials()
        with resourcedb.ResourceDB(self.index, creds) as resource_db:

            while True:
                latest_reader = self.get_pull_reader(
                    from_page=from_page,
                    to_page=to_page,
                    per_page=100)

                self.normalizer = self.get_pull_normalizer()

                latest_pulls = []
                for pull in latest_reader.iter():
                    norm_pull = {}
                    self.normalizer.normalize(pull, norm_pull)
                    latest_pulls.append(norm_pull)

                if len(latest_pulls) == 0:
                    break

                pulls_not_in_es, mine_more_from_es = self.check_pulls_es(
                    latest_pulls, resource_db)

                if mine_more_from_es:
                    new_pulls += pulls_not_in_es
                    to_page += 1
                    from_page += 1
                else:
                    new_pulls += pulls_not_in_es
                    break

        return new_pulls

    def check_pulls_es(self, pulls_from_source, resource_db):

        filtered_pulls = []

        for pull in pulls_from_source:
            query_list = []
            query_params = pull[f.PULL_URI].split("/")
            for param in query_params:
                if param != '' and param != "https:" and param != "http:":
                    query_dict = {}
                    query_dict[f.ISSUE_KEY] = param
                    query_list.append(query_dict)

            pull_from_es = resource_db.db.match_tokenized_string(
                self.index,
                'pull',
                query_list)

            if not pull_from_es:
                filtered_pulls.append(pull)
            else:
                return filtered_pulls, False

        return filtered_pulls, True

    def _stream_data(self):

        all_pull = []
        for source in self.sources:
            if source['source_type'] != self.source_type:
                continue

            self.set_vars(source)

            try:
                pull = self.stream_source()
                if pull is not None:
                    all_pull.extend(pull)
            except Exception as e:
                logger.exception("Source pull  mine {}".format(source))
                logger.error("Print Error: {}".format(str(e)))
                logger.error("Continuing to other sources")

        if len(all_pull) == 0:
            return None
        else:
            return all_pull

    def stream_source(self):

        latest_pulls_from_source = self.get_new_pulls_from_source()

        logger.info(
            "(Streaming) Mining Pull Requests: repo_url='%s'", self.repo_uri)

        if len(latest_pulls_from_source) == 0:
            return None
        else:
            return latest_pulls_from_source

    def stream_data(self):
        return self._stream_data()


class Issue(object):

    def __init__(self, config):

        self.config = config
        self.sources = config['input']['sources']
        self.index = config['full_name']

    def set_vars(self, source):

        self.api_base_url = self.miner_class.get_source_api_base_url(source)
        self.access_token = self.miner_class.get_source_access_token(source)
        self.repo_owner = self.miner_class.get_source_repo_owner(source)
        self.repo_name = self.miner_class.get_source_repo_name(source)
        self.repo_uri = self.miner_class.get_source_repo_uri_with_formatter(
            source,
            self.formatter)
        self.label_field_map = self.miner_class.build_label_field_map(
            source.get('labels_map'))

    def get_issue_reader(self, from_page=1, to_page=0, per_page=100):
        return self.issue_reader(
            self.api_base_url,
            self.access_token,
            self.repo_owner,
            self.repo_name,
            from_page=from_page,
            to_page=to_page,
            per_page=per_page,
            wait=False)

    def get_issue_normalizer(self):

        return self.issue_normalizer(self.label_field_map)

    def get_new_issues_from_source(self):
        to_page = 1
        from_page = 1
        new_issues = []

        creds = cf_util.get_db_credentials()
        with resourcedb.ResourceDB(self.index, creds) as resource_db:

            while True:
                latest_reader = self.get_issue_reader(
                    from_page=from_page, to_page=to_page, per_page=100)
                self.normalizer = self.get_issue_normalizer()

                latest_issues = []
                for issue in latest_reader.iter():
                    if "pull_request" in issue:
                        # Skip pull requests
                        continue

                    norm_issue = {}
                    self.normalizer.normalize(issue, norm_issue)
                    latest_issues.append(norm_issue)

                if isinstance(self, RTCIssue):
                    latest_issues.sort(key=lambda d: int(d[f.ISSUE_ID]), reverse=True)

                if len(latest_issues) == 0:
                    break

                issues_not_in_es, mine_more_from_es = self.check_issues_es(
                    latest_issues, resource_db)

                if mine_more_from_es:
                    new_issues += issues_not_in_es
                    to_page += 1
                    from_page += 1
                else:
                    new_issues += issues_not_in_es
                    break

        return new_issues

    def check_issues_es(self, issues_from_source, resource_db):

        filtered_issues = []

        for issue in issues_from_source:
            query_list = []
            query_params = issue[f.ISSUE_KEY].split("/")
            for param in query_params:
                if param != '' and param != "https:":
                    if '-' in param:
                        for p in param.split('-'):
                            query_dict = {}
                            query_dict[f.ISSUE_KEY] = p.lower()
                            query_list.append(query_dict)
                    else:
                        query_dict = {}
                        query_dict[f.ISSUE_KEY] = param.lower()
                        query_list.append(query_dict)

            issue_from_es = resource_db.db.match_tokenized_string(
                self.index,
                'issue',
                query_list)

            if not issue_from_es:
                filtered_issues.append(issue)
            else:
                return filtered_issues, False

        return filtered_issues, True

    def _stream_data(self):

        all_issue = []

        for source in self.sources:
            if source['source_type'] != self.source_type:
                continue

            self.set_vars(source)

            try:
                issue = self.stream_source()
                if issue is not None:
                    all_issue.extend(issue)
            except Exception as e:
                logger.exception("Source failed issue mine {}".format(source))
                logger.error("Print Error: {}".format(str(e)))
                logger.error("Continuing to other sources")

        if len(all_issue) == 0:
            return None
        else:
            return all_issue

    def stream_source(self):

        latest_issues_from_source = self.get_new_issues_from_source()

        logger.info("(Streaming) Mining issues: repo_url='%s'", self.repo_uri)

        if len(latest_issues_from_source) == 0:
            return None
        else:
            return latest_issues_from_source

    def stream_data(self):
        return self._stream_data()


class Contributor(object):

    def __init__(self, config):

        self.config = config
        self.sources = config['input']['sources']
        self.index = config['full_name']

    def set_vars(self, source):

        self.api_base_url = self.miner_class.get_source_api_base_url(source)
        self.access_token = self.miner_class.get_source_access_token(source)
        self.repo_owner = self.miner_class.get_source_repo_owner(source)
        self.repo_name = self.miner_class.get_source_repo_name(source)
        self.repo_uri = self.miner_class.get_source_repo_uri_with_formatter(
            source,
            self.formatter)
        self.user_key_by_ids = uuid_util.UniqueIDByKeyValues()

    def get_contributor_reader(self, from_page=1, to_page=0, per_page=100):
        return self.contributor_reader(
            self.api_base_url,
            self.access_token,
            self.repo_owner,
            self.repo_name,
            from_page=from_page,
            to_page=to_page,
            per_page=per_page,
            wait=False,
            details=False)

    def get_contributors_from_source(self):
        # get latest users from github
        return self.get_contributor_reader().iter()

    def get_contributor_from_es(self, contributor, resource_db):

        # Check if latest issue exists in elasticsearch
        contributor = resource_db.db.exists(
            self.index,
            'user',
            key_values={f.USER_ID: contributor})

        return contributor

    def get_user_normalizer(self):

        return self.user_normalizer(self.user_key_by_ids)

    def _stream_data(self):

        all_contributor = []

        for source in self.sources:
            if source['source_type'] != self.source_type:
                continue

            self.set_vars(source)

            try:
                contributor = self.stream_source()

                if contributor is not None:
                    all_contributor.extend(contributor)
            except Exception as e:
                logger.exception(
                    "Source failed contributor mine {}".format(source))
                logger.error("Print Error: {}".format(str(e)))
                logger.error("Continuing to other sources")

        if len(all_contributor) == 0:
            return None
        else:
            return all_contributor

    def stream_source(self):

        latest_contributors = self.get_contributors_from_source()
        filtered_contributors = []

        if not latest_contributors:
            return None

        creds = cf_util.get_db_credentials()

        self.normalizer = self.get_user_normalizer()
        with resourcedb.ResourceDB(self.index, creds) as resource_db:

            for latest_contributor in latest_contributors:
                if not self.get_contributor_from_es(
                        latest_contributor['id'],
                        resource_db):
                    logger.info(
                        "(Streaming) Returning New User: repo_url='%s'",
                        self.repo_uri)

                    resource_uri = latest_contributor['url']
                    logger.info("Get '%s'", resource_uri)
                    response = self.get_contributor_reader().client.get(
                        resource_uri)
                    user = json.loads(response.content)

                    norm_user = {}
                    self.normalizer.normalize(user, norm_user)

                    filtered_contributors.append(norm_user)

        if filtered_contributors:
            return filtered_contributors
        else:
            return None

    def stream_data(self):
        return self._stream_data()


class Commit(object):

    def __init__(self, config):

        self.config = config
        self.sources = config['input']['sources']
        self.job_name = config['name']
        self.index = config['full_name']

    def set_vars(self, source):

        self.source = source
        self.api_base_url = self.miner_class.get_source_api_base_url(source)
        self.access_token = self.miner_class.get_source_access_token(source)
        self.repo_owner = self.miner_class.get_source_repo_owner(source)
        self.repo_name = self.miner_class.get_source_repo_name(source)
        self.repo_uri = self.miner_class.get_source_repo_uri_with_formatter(
            source,
            self.repo_uri_format)

        self.mine_blame_info = source.get("mine_blame_info", True)
        self.file_black_list = source.get("file_black_list")

        self.merc_repo = source.get("mercurial_repo")
        self.branch_name = git_miner.GitMiner.get_branch_name(
            source, self.repo_uri)
        self.git_url = git_miner.GitMiner.get_source_git_url(source)
        self.git_dir = self.get_git_dir(self.git_url)

    def get_git_dir(self, git_url):

        work_dir = os.environ[constants.ASTRO_OUT_DIR_ENV_VAR]
        project_work_dir = os.path.join(work_dir, self.job_name)
        stream_in_path = os.path.join(project_work_dir, "stream/in")
        file_util.makedirs(stream_in_path)

        return uri_util.build_file_path_from_uri(git_url, stream_in_path)

    def get_latest_seq_no(self, resource_db):
        dic = resource_db.db.get_latest_record(
            self.index,
            'commit',
            {f.COMMIT_SOURCE_URI : self.repo_uri},
            query_string=True)
        return dic.get('sequence_no',0)

    def update_seq_no(self, commits, committed_files, latest_seq_no):
        current_seq_no = latest_seq_no
        mapping = {}
        commits.reverse()
        for commit in commits:
            current_seq_no += 1
            commit[f.SEQUENCE_NO] = current_seq_no
            mapping[commit[f.COMMIT_ID]] = current_seq_no

        for committed_file in committed_files:
            committed_file[f.SEQUENCE_NO] = mapping[committed_file[f.COMMIT_ID]]

    def get_stream_commit_reader(self):
        return gitrepo.GitStreamCommitReader(
            self.git_dir,
            self.branch_name)

    def get_commit_normalizer(self):

        return github.GitHubCommitNormalizer()

    def get_stream_committed_file_reader(self):
        return gitrepo.GitStreamCommittedFileReader(
            self.git_dir,
            mine_blame_info=self.mine_blame_info,
            file_black_list=self.file_black_list)

    def clone_repo(self):
        repo = gitrepo.GitReader(self.git_dir)
        logger.info("Cloning '%s' to '%s' ...", self.git_url, self.git_dir)

        if self.merc_repo == "True" \
                or self.merc_repo == "true" \
                or self.merc_repo is True:
            repo.mercurial_to_git(self.git_url)
        else:
            repo.clone(self.git_url, 
                self.access_token, 
                streaming=True, 
                source_type=self.source_type)

        if repo.is_empty():
            logger.info(
                "Repo is empty, skipping mining commits: repo_url='%s' ...",
                self.repo_uri)
            return
        return repo

    def _update_commit(self, norm_commit):
        norm_commit[f.COMMIT_URI] = self.commit_uri_format.format(
            self.repo_uri, norm_commit[f.COMMIT_ID])
        norm_commit[f.COMMIT_SOURCE_URI] = self.repo_uri
        norm_commit[f.COMMIT_SOURCE_TYPE] = self.source_type
        norm_commit[
            f.COMMIT_ISSUE_KEYS] = git_miner.GitCommitMiner.find_issue_keys(
            norm_commit, self.issue_key_finders)

    def _update_committed_file(self, norm_file):
        norm_file[f.COMMIT_URI] = self.commit_uri_format.format(
            self.repo_uri, norm_file[f.COMMIT_ID])
        norm_file[f.COMMIT_SOURCE_URI] = self.repo_uri
        norm_file[f.COMMIT_SOURCE_TYPE] = self.source_type

    def _stream_data(self):
        self.issue_key_finders = \
            git_miner.GitCommitMiner.get_issue_key_finders(
                self.sources, self.repo_uri_format, self.issue_uri_format)

        all_commit, all_committed_file = [], []

        for source in self.sources:

            if source['source_type'] != self.source_type:
                continue

            self.set_vars(source)

            source_list = [source]

            repo_list = git_miner.GitMiner.get_repo_list()

            for item in self.sources:
                if item['source_type'] not in repo_list:
                    source_list.append(item)

            self.issue_key_finders = \
                git_miner.GitCommitMiner.get_issue_key_finders(
                    source_list,
                    self.repo_uri_format,
                    self.issue_uri_format)

            try:
                commit, committed_file = self.stream_source()

                if commit is not None:
                    all_commit.extend(commit)

                if committed_file is not None:
                    all_committed_file.extend(committed_file)

            except Exception as e:
                logger.exception("Source failed commit mine {}".format(source))
                logger.error("Print Error: {}".format(str(e)))
                logger.error("Continuing to other sources")

        if len(all_commit) == 0:
            return None, None
        elif len(all_committed_file) == 0:
            return all_commit, None
        else:
            return all_commit, all_committed_file

    def stream_source(self):

        streamed_commits = []
        streamed_committed_files = []

        creds = cf_util.get_db_credentials()

        with resourcedb.ResourceDB(self.index, creds) as resource_db:

            logger.info(
                "(Streaming) Mining commits: repo_url='%s', branch='%s'",
                self.repo_uri, self.branch_name)
            repo = self.clone_repo()
            latest_seq_no = self.get_latest_seq_no(resource_db)
            commit_reader = self.get_stream_commit_reader()
            committed_file_reader = self.get_stream_committed_file_reader()
            committed_file_reader.mine_blame_info = True

            for norm_commit in commit_reader.iter():

                commit_id = norm_commit[f.COMMIT_ID]

                exists = resource_db.db.exists(
                    self.index,
                    'commit',
                    {f.COMMIT_ID: commit_id})

                if exists is True:
                    break

                logger.info("Mining commit: id='%s'", commit_id)
                self._update_commit(norm_commit)
                streamed_commits.append(norm_commit)

                for norm_committed_file in committed_file_reader.iter(
                        commit_id):
                    norm_committed_file.update(norm_commit)
                    self._update_committed_file(norm_committed_file)
                    streamed_committed_files.append(norm_committed_file)

        # logger.info("(Streaming) Mined commits: repo_url='%s'", self.repo_uri)

        self.update_seq_no(
            streamed_commits, 
            streamed_committed_files,
            latest_seq_no)

        del(commit_reader)
        del(committed_file_reader)
        del(repo)

        if len(streamed_commits) > 0:
            if len(streamed_committed_files) > 0:
                return streamed_commits, streamed_committed_files
            else:
                return streamed_commits, None
        else:
            return None, None

    def stream_data(self):
        return self._stream_data()


class GitLab(object):

    def __init__(self):
        self.change_api()

    def change_api(self):
        for source in self.sources:
            if source['source_type'] == 'gitlab_repo':
                source['api_base_url'] = source['api_base_url'].replace(
                    'gitlab.com/api/v3', 
                    'gitlab.com/api/v4')


class GitHubPullRequest(PullRequest):

    miner_class = git_miner.GitMiner
    formatter = github.GitHubClient.REPO_URI_FORMAT
    pull_reader = github.GitHubPullReader
    pull_normalizer = github.GitHubPullNormalizer
    source_type = 'github_repo'


class GitHubIssue(Issue):

    miner_class = git_miner.GitMiner
    formatter = github.GitHubClient.REPO_URI_FORMAT
    issue_reader = github.GitHubIssueReader
    issue_normalizer = github.GitHubIssueNormalizer
    source_type = 'github_repo'


class GitLabIssue(Issue, GitLab):

    miner_class = git_miner.GitMiner
    formatter = gitlab.GitLabClient.REPO_URI_FORMAT
    issue_reader = gitlab.GitLabIssueReader
    issue_normalizer = gitlab.GitLabIssueNormalizer
    source_type = 'gitlab_repo'

    def __init__(self, config):
        Issue.__init__(self, config)
        GitLab.__init__(self)

    def get_issue_normalizer(self):

        return self.issue_normalizer(self.label_field_map, self.repo_uri)


class JiraIssue(Issue):

    miner_class = jira_miner.JiraIssueMiner
    formatter = jira.JiraClient.PROJECT_URI_FORMAT
    issue_reader = jira.JiraIssuesReader
    issue_normalizer = jira.JiraIssuesNormalizer
    source_type = 'jira_project'

    def set_vars(self, source):

        self.api_base_url = self.miner_class.get_source_api_base_url(source)
        self.username = self.miner_class.get_source_username(source)
        self.password = self.miner_class.get_source_password(source)
        self.project_key = self.miner_class.get_source_project_key(source)
        self.repo_uri = self.miner_class.get_source_repo_uri(
            source, self.formatter)

    def get_issue_reader(self, from_page=0, to_page=0, per_page=100):
        return self.issue_reader(
            self.api_base_url,
            self.username,
            self.password,
            self.project_key,
            start_at=(from_page - 1) * 100,
            stop_at=(to_page) * 100 - 1,
            max_results=per_page)

    def get_issue_normalizer(self):

        return self.issue_normalizer()


class RTCIssue(Issue):

    miner_class = rtc_miner.RTCIssueMiner
    formatter = rtc.RtcClient.PROJECT_URI_FORMAT
    issue_reader = rtc.RtcStreamIssuesReader
    issue_normalizer = rtc.RtcIssuesNormalizer
    source_type = 'rtc_project'

    def set_vars(self, source):

        self.api_base_url = self.miner_class.get_source_api_base_url(source)
        self.username = self.miner_class.get_source_username(source)
        self.password = self.miner_class.get_source_password(source)
        self.project_title = self.miner_class.get_source_project_title(source)
        self.repo_uri = self.miner_class.get_source_repo_uri(
            source, self.formatter)
        self.auth_method = self.miner_class.get_source_auth_method(source)
        self.verify = self.miner_class.get_source_verify(source)
        self.allow_redirects = self.miner_class.get_source_allow_redirects(
            source)
        self.reader = self.issue_reader(
            self.api_base_url,
            self.project_title,
            self.username,
            self.password,
            self.auth_method,
            self.verify,
            self.allow_redirects)

    def get_issue_reader(self, **kargs):

        return self.reader

    def get_issue_normalizer(self):

        return self.issue_normalizer(self.reader.client)


class GitHubContributor(Contributor):

    miner_class = git_miner.GitMiner
    formatter = github.GitHubClient.REPO_URI_FORMAT
    contributor_reader = github.GitHubContributorReader
    user_normalizer = github.GitHubUserNormalizer
    source_type = 'github_repo'


class GitLabContributor(Contributor, GitLab):

    miner_class = git_miner.GitMiner
    formatter = gitlab.GitLabClient.REPO_URI_FORMAT
    contributor_reader = gitlab.GitLabContributorReader
    user_normalizer = gitlab.GitLabUserNormalizer
    source_type = 'gitlab_repo'

    def __init__(self, config):
        Contributor.__init__(self, config)
        GitLab.__init__(self)

    def get_user_normalizer(self):

        return self.user_normalizer(self.user_key_by_ids, self.repo_uri)

    def get_contributors_from_source(self):

        latest_reader = self.get_contributor_reader(to_page=1)
        self.normalizer = self.get_user_normalizer()

        latest_users = []
        # get latest issue from gitlab
        for user in latest_reader.iter():
            norm_user = {}
            self.normalizer.normalize(user, norm_user)
            latest_users.append(norm_user)
            # latest_issue = norm_issue[f.ISSUE_ID]
        return latest_users

    def get_contributor_from_es(self, contributor, resource_db):

        # Check if latest issue exists in elasticsearch
        contributor = resource_db.db.exists(
            self.index,
            'user',
            key_values={'user_email.raw': contributor})

        return contributor

    def stream_source(self):

        latest_contributors = self.get_contributors_from_source()
        filtered_contributors = []

        if len(latest_contributors) == 0:
            return None

        creds = cf_util.get_db_credentials()

        self.normalizer = self.get_user_normalizer()
        with resourcedb.ResourceDB(self.index, creds) as resource_db:

            for latest_contributor in latest_contributors:

                if not self.get_contributor_from_es(
                        latest_contributor[f.USER_ID],
                        resource_db):
                    logger.info(
                        "(Streaming) Returning New User: repo_url='%s'",
                        self.repo_uri)

                    filtered_contributors.append(latest_contributor)

        if filtered_contributors:
            return filtered_contributors
        else:
            return None


class GitHubCommit(Commit):

    miner_class = git_miner.GitMiner
    repo_uri_format = github.GitHubClient.REPO_URI_FORMAT
    commit_uri_format = github.GitHubClient.COMMIT_URI_FORMAT
    issue_uri_format = github.GitHubClient.ISSUE_URI_FORMAT
    source_type = 'github_repo'


class GitLabCommit(Commit, GitLab):

    miner_class = git_miner.GitMiner
    repo_uri_format = gitlab.GitLabClient.REPO_URI_FORMAT
    commit_uri_format = gitlab.GitLabClient.COMMIT_URI_FORMAT
    issue_uri_format = gitlab.GitLabClient.ISSUE_URI_FORMAT
    source_type = 'gitlab_repo'

    def __init__(self, config):
        Commit.__init__(self, config)
        GitLab.__init__(self)


class Joiner(object):

    def __init__(self, config):
        self.config = config
        self.sources = config['input']['sources']
        self.index = config['full_name']

    def join_data(self, commits, committed_files):

        joined_commits = []
        joined_commited_files = []

        creds = cf_util.get_db_credentials()

        with resourcedb.ResourceDB(self.index, creds) as resource_db:

            commits.sort(key=itemgetter(f.SEQUENCE_NO))
            committed_files.sort(key=itemgetter(f.SEQUENCE_NO))

            # Join commits with issues and contributors
            commit_joiner = joiner.CommitEventIssueUserJoiner(resource_db)
            commit_counter = counter.CommitEventCounter(resource_db)
            committed_file_counter = counter.CommittedFileEventCounter(
                resource_db)

            # Join commits
            for commit in commits:
                commit_joiner.update(commit)
                commit_counter.update(commit)
                joined_commits.append(commit)

            logger.info("Total number of commits joined: %d", len(commits))

            # Join committed files with commits (already joined with issues and
            # contributors above)
            total_committed_file_count = 0
            commit_pointer = 0

            for sequence_no, cfs in itertools.groupby(
                    committed_files,
                    key=itemgetter(f.SEQUENCE_NO)):
                while commits[commit_pointer][f.SEQUENCE_NO] != sequence_no:
                    commit_pointer += 1
                committed_file_count = 0
                commit = commits[commit_pointer]
                committed_file_count = 0
                for committed_file in cfs:
                    committed_file.update(commit)
                    committed_file_counter.update(committed_file)
                    joined_commited_files.append(committed_file)
                    committed_file_count += 1
                    total_committed_file_count += 1
                    logger.info("Commit '%s': %d files joined", commit[
                                f.COMMIT_ID], committed_file_count)

            logger.info("Total number of committed files joined: %d",
                        total_committed_file_count)

        if joined_commits:
            if joined_commited_files:
                return joined_commits, joined_commited_files
            else:
                return joined_commits, None
        else:
            return None, None


commit_type_to_miner_class = {

    GitHubCommit.source_type: GitHubCommit,

    GitLabCommit.source_type: GitLabCommit,

}

issue_type_to_miner_class = {

    GitHubIssue.source_type: GitHubIssue,

    GitLabIssue.source_type: GitLabIssue,

    JiraIssue.source_type: JiraIssue,

    RTCIssue.source_type: RTCIssue
}

contributor_type_to_miner_class = {

    GitHubContributor.source_type: GitHubContributor,

    GitLabContributor.source_type: GitLabContributor
}

pull_request_type_to_miner_class = {

    GitHubPullRequest.source_type: GitHubPullRequest
}
