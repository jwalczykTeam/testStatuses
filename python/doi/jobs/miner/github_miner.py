'''
Created on Mar 10, 2016

@author: alberto
'''

from doi.jobs.miner import github
from doi.jobs.miner import git2repo as gitrepo
from doi.jobs.miner import git_miner
from doi.jobs.miner.miner import mine_data


class GitHubIssueMiner(git_miner.GitIssueMiner):
    def __init__(self, params, progress_callback, stop_event):
        super(GitHubIssueMiner, self).__init__(params, progress_callback, stop_event,
                                               'github_repo', github.GitHubClient.REPO_URI_FORMAT)

    def get_issue_reader(self, api_base_url, access_token, repo_owner, repo_name):
        return github.GitHubIssueReader(api_base_url, access_token, repo_owner, repo_name)

    def get_issue_normalizer(self, label_field_map, repo_uri):
        return github.GitHubIssueNormalizer(label_field_map)

    def get_stream_issue_reader(self, api_base_url, access_token, repo_owner, repo_name):
        return github.GitHubStreamIssueReader(api_base_url, access_token, repo_owner, repo_name)

    def get_latest_issue_reader(self, api_base_url, access_token, repo_owner, repo_name, from_page=1, to_page=1, per_page=1):
        return github.GitHubLatestIssueReader(api_base_url, access_token, repo_owner, repo_name, from_page, to_page, per_page)

class GitHubPullMiner(git_miner.GitPullMiner):
    def __init__(self, params, progress_callback, stop_event):
        super(GitHubPullMiner, self).__init__(params, progress_callback, stop_event,
                                              'github_repo', github.GitHubClient.REPO_URI_FORMAT)

    def get_pull_reader(self, api_base_url, access_token, repo_owner, repo_name):
        return github.GitHubPullReader(api_base_url, access_token, repo_owner, repo_name)

    def get_pull_normalizer(self, repo_uri):
        return github.GitHubPullNormalizer()


class GitHubBranchMiner(git_miner.GitBranchMiner):
    def __init__(self, params, progress_callback, stop_event):
        super(GitHubBranchMiner, self).__init__(params, progress_callback, stop_event,
                                                'github_repo', github.GitHubClient.REPO_URI_FORMAT)

    def get_branch_reader(self, api_base_url, access_token, repo_owner, repo_name):
        return github.GitHubBranchReader(api_base_url, access_token, repo_owner, repo_name)

    def get_branch_normalizer(self, repo_uri):
        return github.GitHubBranchNormalizer()


class GitHubContributorMiner(git_miner.GitContributorMiner):
    def __init__(self, params, progress_callback, stop_event):
        super(GitHubContributorMiner, self).__init__(params, progress_callback, stop_event,
                                                     'github_repo',
                                                     github.GitHubClient.REPO_URI_FORMAT)

    def get_contributor_reader(self, api_base_url, access_token, repo_owner, repo_name):
        return github.GitHubContributorReader(api_base_url, access_token, repo_owner, repo_name)

    def get_user_normalizer(self, user_key_by_ids, repo_uri):
        return github.GitHubUserNormalizer(user_key_by_ids)


class GitHubCommitMiner(git_miner.GitCommitMiner):
    def __init__(self, params, progress_callback, stop_event):
        super(GitHubCommitMiner, self).__init__(params, progress_callback, stop_event,
                                                'github_repo', github.GitHubClient.REPO_URI_FORMAT,
                                                github.GitHubClient.COMMIT_URI_FORMAT, github.GitHubClient.ISSUE_URI_FORMAT)

    def get_commit_reader(self, git_dir, branch_name):
        return gitrepo.GitCommitReader(git_dir, branch_name)

    def get_committed_file_reader(self, git_dir, **options):
        return gitrepo.GitCommittedFileReader(git_dir, **options)

    def get_commit_normalizer(self, issue_key_finders=None):
        return github.GitHubCommitNormalizer(issue_key_finders)

    @staticmethod
    def get_default_branch_name(repo_uri, access_token):
        repo = github.get_repo(repo_uri, access_token)
        return repo['default_branch']


def mine_github_issues(params, progress_callback, stop_event):
    return mine_data(GitHubIssueMiner, params, progress_callback, stop_event)

def mine_github_pulls(params, progress_callback, stop_event):
    return mine_data(GitHubPullMiner, params, progress_callback, stop_event)

def mine_github_branches(params, progress_callback, stop_event):
    return mine_data(GitHubBranchMiner, params, progress_callback, stop_event)

def mine_github_contributors(params, progress_callback, stop_event):
    return mine_data(GitHubContributorMiner, params, progress_callback, stop_event)

def mine_github_commits(params, progress_callback, stop_event):
    return mine_data(GitHubCommitMiner, params, progress_callback, stop_event)
