'''
Created on Mar 10, 2016

@author: alberto
'''

from doi.common import StoppedException
from doi.jobs.miner.resourcedb import ResourceDB
from doi.jobs.miner.resourcedb import ResourceDBBulkInserter
from doi.jobs.miner import fields as f
from doi.jobs.miner import git2repo as gitrepo
from doi.jobs.miner import jira, rtc
from doi.jobs.miner import bitbucket, bitbucketserver
from doi.jobs.miner import github, gitlab
from doi.jobs.miner.miner import Miner
from doi.util import file_util
from doi.util import log_util
from doi.util import uri_util
from doi.util import uuid_util
from doi.jobs.miner import gitblame
import os
import re
import time

logger = log_util.get_logger("astro.jobs.miner")


class IssueKeyFinder(object):

    def __init__(self, pattern, builder, **kwargs):
        self.pattern = pattern
        self.builder = builder
        self.params = kwargs


class GitMiner(Miner):

    def __init__(self, params, progress_callback, stop_event,
                 source_type, repo_uri_format):

        if source_type == 'gitlab_repo':
            for source in params.input['sources']:
                source['api_base_url'] = source['api_base_url'].replace(
                    'gitlab.com/api/v3', 
                    'gitlab.com/api/v4')

        super(GitMiner, self).__init__(params, progress_callback, stop_event)
        self.source_type = source_type
        self.repo_uri_format = repo_uri_format
        self.max_git_objects = self.input.get('max_git_objects')

    @staticmethod
    def get_source_api_base_url(source):
        return source['api_base_url']

    @staticmethod
    def get_source_access_token(source):
        return source['access_token']

    @staticmethod
    def get_source_repo_owner(source):
        return source['repo_owner']

    @staticmethod
    def get_repo_list():
        return [
            'github_repo',
            'gitlab_repo',
            'bitbucket_repo',
            'bitbucketserver_repo']

    @staticmethod
    def get_source_repo_name(source):
        return source['repo_name']

    def get_source_label_field_map(self, source):
        return self.build_label_field_map(source.get('labels_map'))

    @staticmethod
    def get_source_git_url(source):
        return source['git_url']

    @staticmethod
    def get_branch_name(source, repo_uri):
        branch_name = source.get('branch_name')
        merc_repo = source.get("mercurial_repo")
        access_token = GitMiner.get_source_access_token(source)
        if branch_name is None:
            # Use the repo default branch

            if merc_repo is None or merc_repo == False:
                branch_name = GitMiner.get_default_branch_name(
                    source, repo_uri, access_token)
            else:
                branch_name = "master"

        return branch_name

    @staticmethod
    def get_default_branch_name(source, repo_uri, access_token):
        source_type = source['source_type']
        if source_type == 'github_repo':
            repo = github.get_repo(repo_uri, access_token)
        elif source_type == 'gitlab_repo':
            repo = gitlab.get_repo(repo_uri, access_token)
        elif source_type == 'bitbucket_repo':
            repo = bitbucket.get_repo(repo_uri, access_token)
        elif source_type == 'bitbucketserver_repo':
            repo = bitbucketserver.get_repo(repo_uri, access_token)
        return repo.get('default_branch', 'master')

    def get_source_repo_uri(self, source):
        api_base_url = GitMiner.get_source_api_base_url(source)
        repo_owner = GitMiner.get_source_repo_owner(source)
        repo_name = GitMiner.get_source_repo_name(source)
        return self.repo_uri_format.format(api_base_url, repo_owner, repo_name)

    @staticmethod
    def get_source_repo_uri_with_formatter(source, repo_uri_format):
        api_base_url = GitMiner.get_source_api_base_url(source)
        repo_owner = GitMiner.get_source_repo_owner(source)
        repo_name = GitMiner.get_source_repo_name(source)
        return repo_uri_format.format(api_base_url, repo_owner, repo_name)

    @staticmethod
    def build_label_field_map(label_field_list):
        DefaultLabelFieldMap = {
            'bug': (f.ISSUE_TYPES, 'Bug'),
            'enhancement': (f.ISSUE_TYPES, 'Improvement'),
            'duplicate': (f.ISSUE_RESOLUTION, 'duplicate'),
            'invalid': (f.ISSUE_RESOLUTION, 'invalid'),
            'wontfix': (f.ISSUE_RESOLUTION, 'wontfix')
        }

        label_field_map = DefaultLabelFieldMap.copy()
        if label_field_list is not None:
            for label_field in label_field_list:
                label_name = label_field['label']
                field_name = label_field['field']
                field_value = label_field.get('value', label_name)
                label_field_map[label_name] = (field_name, field_value)

        return label_field_map


class GitIssueMiner(GitMiner):

    def __init__(self, params, progress_callback, stop_event,
                 source_type, repo_uri_format):
        super(GitIssueMiner, self).__init__(params, progress_callback, stop_event,
                                            source_type, repo_uri_format)
        self.count = 0

    def get_issue_reader(self, api_base_url, access_token, repo_owner, repo_name):
        raise NotImplementedError("get_issue_reader")

    def get_stream_issue_reader(self, api_base_url, access_token, repo_owner, repo_name):
        raise NotImplementedError("get_issue_reader")

    def get_latest_issue_reader(self, api_base_url, access_token, repo_owner, repo_name):
        raise NotImplementedError("get_latest_issue_reader")

    def get_issue_normalizer(self, label_field_map, repo_uri):
        raise NotImplementedError("get_issue_normalizer")

    def mine_data(self):
        with ResourceDB(self.job_name) as resource_db:
            with ResourceDBBulkInserter(resource_db, ResourceDB.ISSUE_TYPE.name) as writer:
                for source in self.sources:
                    if source['source_type'] != self.source_type:
                        continue

                    self.mine_source(source, writer)

                return {
                    "issue_count": self.count
                }

    def delete_data(self):
        return {
            "issue_count": 0
        }

    def mine_source(self, source, writer):
        # Last_updated_time = max of f.ISSUE_UPDATED_TIME
        # Get source properties
        api_base_url = self.get_source_api_base_url(source)
        access_token = self.get_source_access_token(source)
        repo_owner = self.get_source_repo_owner(source)
        repo_name = self.get_source_repo_name(source)
        repo_uri = self.get_source_repo_uri(source)
        label_field_map = self.get_source_label_field_map(source)

        logger.info("Mining issues: repo_url='%s'", repo_uri)
        reader = self.get_issue_reader(
            api_base_url, access_token, repo_owner, repo_name)
        normalizer = self.get_issue_normalizer(label_field_map, repo_uri)

        for issue in reader.iter():
            if self.stop_event.is_set():
                raise StoppedException

            if "pull_request" in issue:
                # Skip pull requests
                continue

            norm_issue = {}
            normalizer.normalize(issue, norm_issue)
            writer.write(norm_issue)
            self.count += 1

        logger.info("Mined issues: repo_url='%s'", repo_uri)


class GitPullMiner(GitMiner):

    def __init__(self, params, progress_callback, stop_event,
                 source_type, repo_uri_format):
        super(GitPullMiner, self).__init__(params, progress_callback, stop_event,
                                           source_type, repo_uri_format)
        self.count = 0

    def get_pull_reader(self, api_base_url, access_token, repo_owner, repo_name):
        raise NotImplementedError("get_pull_reader")

    def get_pull_normalizer(self, repo_uri):
        raise NotImplementedError("get_pull_normalizer")

    def mine_data(self):
        with ResourceDB(self.job_name) as resource_db:
            with ResourceDBBulkInserter(resource_db, ResourceDB.PULL_TYPE.name) as writer:
                for source in self.sources:
                    if source['source_type'] != self.source_type:
                        continue

                    self.mine_source(source, writer)

                return {
                    "pull_count": self.count
                }

    def mine_source(self, source, writer):
        # Last_updated_time = max of f.PULL_UPDATED_TIME
        # Get source properties
        api_base_url = self.get_source_api_base_url(source)
        access_token = self.get_source_access_token(source)
        repo_owner = self.get_source_repo_owner(source)
        repo_name = self.get_source_repo_name(source)
        repo_uri = self.get_source_repo_uri(source)

        logger.info("Mining pull requests: repo_url='%s'", repo_uri)
        reader = self.get_pull_reader(
            api_base_url, access_token, repo_owner, repo_name)
        normalizer = self.get_pull_normalizer(repo_uri)

        for pull in reader.iter():
            if self.stop_event.is_set():
                raise StoppedException

            norm_pull = {}
            normalizer.normalize(pull, norm_pull)
            writer.write(norm_pull)
            self.count += 1

        logger.info("Mined pull requests: repo_url='%s'", repo_uri)

    def delete_data(self):
        return {
            "pull_count": 0
        }


class GitBranchMiner(GitMiner):

    def __init__(self, params, progress_callback, stop_event,
                 source_type, repo_uri_format):
        super(GitBranchMiner, self).__init__(params, progress_callback, stop_event,
                                             source_type, repo_uri_format)
        self.count = 0

    def get_branch_reader(self, api_base_url, access_token, repo_owner, repo_name):
        raise NotImplementedError("get_branch_reader")

    def get_branch_normalizer(self, repo_uri):
        raise NotImplementedError("get_branch_normalizer")

    def mine_data(self):
        with ResourceDB(self.job_name) as resource_db:
            with ResourceDBBulkInserter(resource_db, ResourceDB.BRANCH_TYPE.name) as writer:
                for source in self.sources:
                    if source['source_type'] != self.source_type:
                        continue

                    self.mine_source(source, writer)

                return {
                    "branch_count": self.count
                }

    def mine_source(self, source, writer):
        # There is no last_updated_time for branches
        # Get source properties
        api_base_url = self.get_source_api_base_url(source)
        access_token = self.get_source_access_token(source)
        repo_owner = self.get_source_repo_owner(source)
        repo_name = self.get_source_repo_name(source)
        repo_uri = self.get_source_repo_uri(source)

        logger.info("Mining branches: repo_url='%s'", repo_uri)
        reader = self.get_branch_reader(
            api_base_url, access_token, repo_owner, repo_name)
        normalizer = self.get_branch_normalizer(repo_uri)

        for branch in reader.iter():
            if self.stop_event.is_set():
                raise StoppedException

            norm_branch = {}
            normalizer.normalize(branch, norm_branch)
            writer.write(norm_branch)
            self.count += 1

        logger.info("Mined branches: repo_url='%s'", repo_uri)

    def delete_data(self):
        return {
            "branch_count": 0
        }


class GitContributorMiner(GitMiner):

    def __init__(self, params, progress_callback, stop_event,
                 source_type, repo_uri_format):
        super(GitContributorMiner, self).__init__(params, progress_callback, stop_event,
                                                  source_type, repo_uri_format)
        self.count = 0

    def get_contributor_reader(self, api_base_url, access_token, repo_owner, repo_name):
        raise NotImplementedError("get_contributor_reader")

    def get_user_normalizer(self, user_key_by_ids, repo_uri):
        raise NotImplementedError("get_user_normalizer")

    def mine_data(self):
        with ResourceDB(self.job_name) as resource_db:
            with ResourceDBBulkInserter(resource_db, ResourceDB.USER_TYPE.name) as writer:
                for source in self.sources:
                    if source['source_type'] != self.source_type:
                        continue

                    self.mine_source(source, writer)

                return {
                    "contributor_count": self.count
                }

    def mine_source(self, source, writer):
        # There is no last_updated_time for contributors
        # Get source properties
        api_base_url = self.get_source_api_base_url(source)
        access_token = self.get_source_access_token(source)
        repo_owner = self.get_source_repo_owner(source)
        repo_name = self.get_source_repo_name(source)
        repo_uri = self.get_source_repo_uri(source)

        logger.info("Mining contributors: repo_url='%s'", repo_uri)
        user_key_by_ids = uuid_util.UniqueIDByKeyValues()
        reader = self.get_contributor_reader(
            api_base_url, access_token, repo_owner, repo_name)
        normalizer = self.get_user_normalizer(user_key_by_ids, repo_uri)

        for user in reader.iter():
            if self.stop_event.is_set():
                raise StoppedException

            norm_user = {}
            normalizer.normalize(user, norm_user)
            writer.write(norm_user)
            self.count += 1

        logger.info("Mined contributors: repo_url='%s'", repo_uri)

    def delete_data(self):
        return {
            "contributor_count": 0
        }


class GitCommitMiner(GitMiner):

    def __init__(self, params, progress_callback, stop_event,
                 source_type,
                 repo_uri_format, commit_uri_format, issue_uri_format):
        super(GitCommitMiner, self).__init__(params, progress_callback, stop_event,
                                             source_type, repo_uri_format)
        self.commit_uri_format = commit_uri_format
        self.issue_uri_format = issue_uri_format
        self.commit_count = 0
        self.committed_file_count = 0
        self.issue_key_finders = self.get_issue_key_finders(
            sources=self.sources, repo_uri_format=repo_uri_format, issue_uri_format=issue_uri_format)

    def get_commit_reader(self, git_dir, branch_name):
        raise NotImplementedError("get_commit_reader")

    def get_committed_file_reader(self, git_dir, **options):
        raise NotImplementedError("get_committed_file_reader")

    def get_default_branch_name(self, repo_uri, access_token):
        raise NotImplementedError("get_default_branch_name")

    def mine_data(self):
        # Create project work dir
        self.project_work_dir = os.path.join(self.work_dir, self.job_name)
        if os.path.isdir(self.project_work_dir):
            logger.info(
                "Project work dir '%s' already exists. Deleting it ...",
                self.project_work_dir)
            file_util.remove_dir_tree(self.project_work_dir)
        file_util.makedirs(self.project_work_dir)

        try:
            self.started_at = time.time()
            self.time_limit_exceeded = False
            with ResourceDB(self.job_name) as resource_db:
                with ResourceDBBulkInserter(resource_db, ResourceDB.COMMIT_TYPE.name) as commit_writer, \
                        ResourceDBBulkInserter(resource_db, ResourceDB.COMMITTED_FILE_TYPE.name) as committed_file_writer:
                    for source in self.sources:
                        if source['source_type'] != self.source_type:
                            continue

                        source_list = [source]

                        repo_list = GitMiner.get_repo_list()

                        for item in self.sources:
                            if item['source_type'] not in repo_list:
                                source_list.append(item)

                        self.issue_key_finders = self.get_issue_key_finders(
                            source_list,
                            self.repo_uri_format,
                            self.issue_uri_format)

                        self.mine_source(source, commit_writer,
                                         committed_file_writer)
                        if self.time_limit_exceeded:
                            break

                    return {
                        "time_limit_exceeded:": self.time_limit_exceeded,
                        "commit_count": self.commit_count,
                        "committed_file_count": self.committed_file_count
                    }

        finally:
            logger.info("Deleting project work dir: '%s' ...",
                        self.project_work_dir)
            file_util.remove_dir_tree(self.project_work_dir)
            logger.info("Project work dir deleted: '%s'",
                        self.project_work_dir)

    def delete_data(self):
        return {
            "commit_count": 0,
            "committed_file_count": 0
        }

    def mine_source(self, source, commit_writer, committed_file_writer):
        # Last_updated_time = max of f.COMMIT_COMMITTED_TIME
        # Get source properties

        access_token = self.get_source_access_token(source)
        repo_uri = self.get_source_repo_uri(source)
        git_url = self.get_source_git_url(source)
        mine_blame_info = source.get("mine_blame_info", True)
        file_black_list = source.get("file_black_list")
        merc_repo = source.get("mercurial_repo")

        # Get default branch
        branch_name = GitMiner.get_branch_name(source, repo_uri)

        # Cloning repo
        in_path = os.path.join(self.project_work_dir, "in")
        file_util.makedirs(in_path)
        git_dir = uri_util.build_file_path_from_uri(git_url, in_path)
        repo = gitrepo.GitReader(git_dir)
        logger.info("Cloning '%s' to '%s' ...", git_url, git_dir)

        if merc_repo == "True" \
                or merc_repo == "true" \
                or merc_repo is True:
            repo.mercurial_to_git(git_url)
        else:
            repo.clone(git_url, access_token,
                       max_git_objects=self.max_git_objects,
                       source_type=self.source_type)

        if repo.is_empty():
            logger.info(
                "Repo is empty, skipping mining commits: repo_url='%s' ...",
                repo_uri)
            return

        logger.info("Mining commits: repo_url='%s', branch='%s'",
                    repo_uri, branch_name)
        commit_reader = self.get_commit_reader(git_dir, branch_name)
        committed_file_reader = self.get_committed_file_reader(
            git_dir,
            mine_blame_info=mine_blame_info,
            file_black_list=file_black_list)

        diff_blamer = gitblame.DiffBlamer(gitblame.PyGitUtil(git_dir))
        committed_file_reader.mine_blame_info = False

        for norm_commit in commit_reader.iter():
            if self.stop_event.is_set():
                raise StoppedException

            current_time = time.time()
            if self.max_mining_time is not None and current_time - self.started_at > self.max_mining_time:
                self.time_limit_exceeded = True
                break

            commit_id = norm_commit[f.COMMIT_ID]
            logger.info("Mining commit: id='%s'", commit_id)
            self._update_commit(norm_commit, repo_uri)
            commit_writer.write(norm_commit)
            self.commit_count += 1

            # may not need commit buffer if files come out in correct order
            commit_buffer = []
            # Mine committed files
            for norm_committed_file in committed_file_reader.iter(commit_id):
                norm_committed_file.update(norm_commit)
                self._update_committed_file(norm_committed_file, repo_uri)
                commit_buffer.append(norm_committed_file)

                # this may need to occur after writing should check
                self.committed_file_count += 1

            # diff_blamer.run(commit_buffer)

            if not committed_file_reader.mine_blame_info:
                try:
                    diff_blamer.run(commit_buffer)
                except Exception, e:
                    logger.error(
                        'DiffBlamer failed repo {}, commit_id {}: {}'.format(
                            repo_uri, commit_id, str(e)))
                    logger.info('Skip diff blaming for remainder of mining')
                    committed_file_reader.mine_blame_info = True

            for norm_committed_file in commit_buffer:
                committed_file_writer.write(norm_committed_file)

        del(diff_blamer)

        if 'commit_buffer' in locals():
            del(commit_buffer)

        if 'norm_committed_file' in locals():
            del(norm_committed_file)
        else:
            logger.info("No committed_file in {}".format(repo_uri))

        del(committed_file_reader)
        del(commit_reader)
        del(repo)

        logger.info("Mined commits: repo_url='%s'", repo_uri)

    def _update_commit(self, norm_commit, repo_uri):
        norm_commit[f.COMMIT_URI] = self.commit_uri_format.format(
            repo_uri, norm_commit[f.COMMIT_ID])
        norm_commit[f.COMMIT_SOURCE_URI] = repo_uri
        norm_commit[f.COMMIT_SOURCE_TYPE] = self.source_type
        norm_commit[f.COMMIT_ISSUE_KEYS] = self.find_issue_keys(
            norm_commit, self.issue_key_finders)

    def _update_committed_file(self, norm_file, repo_uri):
        norm_file[f.COMMIT_URI] = self.commit_uri_format.format(
            repo_uri, norm_file[f.COMMIT_ID])
        norm_file[f.COMMIT_SOURCE_URI] = repo_uri
        norm_file[f.COMMIT_SOURCE_TYPE] = self.source_type

    @staticmethod
    def find_issue_keys(norm_commit, issue_key_finders):
        issue_keys = set()
        for issue_key_finder in issue_key_finders:
            issue_key_value = norm_commit.get(f.COMMIT_MESSAGE)
            if issue_key_value is not None:
                issue_key_matches = re.finditer(
                    issue_key_finder.pattern, issue_key_value)
                for issue_key_match in issue_key_matches:
                    issue_key = issue_key_match.group(1)
                    issue_keys.add(issue_key_finder.builder(
                        issue_key_finder, issue_key, norm_commit))
            else:
                logger.warning("Commit '%s' without message.",
                               norm_commit[f.COMMIT_ID])
        return list(issue_keys)

    @staticmethod
    def get_issue_key_finders(sources, repo_uri_format, issue_uri_format):
        def git_issue_builder(finder, issue_link, _):
            api_base_url = finder.params['api_base_url']
            default_repo_owner = finder.params['repo_owner']
            default_repo_name = finder.params['repo_name']

            if issue_link[0] == '#':
                # Default repo owner and repo name
                repo_owner = default_repo_owner
                repo_name = default_repo_name
                issue_id = issue_link[1:]
            elif '/' in issue_link:
                # Specific repo owner and repo name
                repo_ower_name, issue_id = issue_link.split('#')
                repo_owner, repo_name = repo_ower_name.split('/')
            else:
                # Specific repo name, default repo owner
                repo_name, issue_id = issue_link.split('#')
                repo_owner = default_repo_owner

            repo_uri = repo_uri_format.format(
                api_base_url, repo_owner, repo_name)
            issue_uri = issue_uri_format.format(repo_uri, issue_id)
            return issue_uri

        def jira_issue_builder(finder, issue_key, _):
            return jira.JiraClient.ISSUE_URI_FORMAT.format(
                finder.params['api_base_url'],
                issue_key.upper())

        def rtc_issue_builder(finder, issue_number, _):
            issue_number = issue_number.strip()
            return rtc.RtcClient.ISSUE_URI_FORMAT.format(
                finder.params['api_base_url'],
                issue_number)

        issue_key_finders = []

        for source in sources:
            if source['source_type'] in GitMiner.get_repo_list():

                issue_key_finders.append(
                    IssueKeyFinder("((([\\w-]+/)?[\\w-]+)?#\\d+)",
                                   git_issue_builder,
                                   api_base_url=source['api_base_url'],
                                   repo_owner=source['repo_owner'],
                                   repo_name=source['repo_name']))
                break

        for source in sources:
            if source['source_type'] == 'jira_project':
                # multiple jira projects with different keys are allowed
                project_key = source['project_key']
                issue_key_finders.append(
                    IssueKeyFinder("(?i)({}-\\d+)".format(project_key),
                                   jira_issue_builder,
                                   api_base_url=source['api_base_url']))

        for source in sources:
            if source['source_type'] == 'rtc_project':
                # Only one rtc project is allowed
                issue_key_finders.append(
                    IssueKeyFinder("(\\d+)",
                                   rtc_issue_builder,
                                   api_base_url=source['api_base_url']))
                break

        return issue_key_finders


def get_repo_owner_uri(repo_uri):
    return repo_uri.rsplit('/', 1)[0]


def get_api_base_url(repo_uri):
    return repo_uri.rsplit('/', 3)[0]
