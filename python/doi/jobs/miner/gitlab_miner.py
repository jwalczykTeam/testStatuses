'''
Created on Mar 10, 2016

@author: alberto
'''

from doi.jobs.miner import gitlab
from doi.jobs.miner import git2repo as gitrepo
from doi.jobs.miner import git_miner
from doi.jobs.miner.miner import mine_data


class GitLabIssueMiner(git_miner.GitIssueMiner):
    def __init__(self, params, progress_callback, stop_event):
        super(GitLabIssueMiner, self).__init__(params, progress_callback, stop_event,
                                               'gitlab_repo', gitlab.GitLabClient.REPO_URI_FORMAT)

    def get_issue_reader(self, api_base_url, access_token, repo_owner, repo_name):
        return gitlab.GitLabIssueReader(api_base_url, access_token, repo_owner, repo_name)

    def get_issue_normalizer(self, label_field_map, repo_uri):
        return gitlab.GitLabIssueNormalizer(label_field_map, repo_uri)
    

'''
class GitLabPullMiner(git_miner.GitPullMiner):
    def __init__(self, params, progress_callback, stop_event):
        super(GitLabPullMiner, self).__init__(params, progress_callback, stop_event,
                                              'gitlab_repo', gitlab.GitLabClient.REPO_URI_FORMAT)

    def get_pull_reader(self, api_base_url, access_token, repo_owner, repo_name):
        return gitlab.GitLabPullReader(api_base_url, access_token, repo_owner, repo_name)

    def get_pull_normalizer(self):
        return gitlab.GitLabPullNormalizer()
'''


class GitLabBranchMiner(git_miner.GitBranchMiner):
    def __init__(self, params, progress_callback, stop_event):
        super(GitLabBranchMiner, self).__init__(params, progress_callback, stop_event,
                                                'gitlab_repo', gitlab.GitLabClient.REPO_URI_FORMAT)

    def get_branch_reader(self, api_base_url, access_token, repo_owner, repo_name):
        return gitlab.GitLabBranchReader(api_base_url, access_token, repo_owner, repo_name)

    def get_branch_normalizer(self, repo_uri):
        return gitlab.GitLabBranchNormalizer(repo_uri)


class GitLabContributorMiner(git_miner.GitContributorMiner):
    def __init__(self, params, progress_callback, stop_event):
        super(GitLabContributorMiner, self).__init__(params, progress_callback, stop_event,
                                                     'gitlab_repo', 
                                                     gitlab.GitLabClient.REPO_URI_FORMAT)

    def get_contributor_reader(self, api_base_url, access_token, repo_owner, repo_name):
        return gitlab.GitLabContributorReader(api_base_url, access_token, repo_owner, repo_name)

    def get_user_normalizer(self, user_key_by_ids, repo_uri):
        return gitlab.GitLabUserNormalizer(user_key_by_ids, repo_uri)


class GitLabCommitMiner(git_miner.GitCommitMiner):
    def __init__(self, params, progress_callback, stop_event):
        super(GitLabCommitMiner, self).__init__(params, progress_callback, stop_event,
                                                'gitlab_repo', gitlab.GitLabClient.REPO_URI_FORMAT, 
                                                gitlab.GitLabClient.COMMIT_URI_FORMAT, gitlab.GitLabClient.ISSUE_URI_FORMAT)

    def get_commit_reader(self, git_dir, branch_name):
        return gitrepo.GitCommitReader(git_dir, branch_name)

    def get_committed_file_reader(self, git_dir, **options):
        return gitrepo.GitCommittedFileReader(git_dir, **options)
        
    @staticmethod
    def get_default_branch_name(repo_uri, access_token):
        repo = gitlab.get_repo(repo_uri, access_token)
        return repo['default_branch']
        

def mine_gitlab_issues(params, progress_callback, stop_event):
    return mine_data(GitLabIssueMiner, params, progress_callback, stop_event)

'''
def mine_gitlab_pulls(params, progress_callback, stop_event):
    return mine_data(GitLabPullMiner, params, progress_callback, stop_event)
'''

def mine_gitlab_branches(params, progress_callback, stop_event):
    return mine_data(GitLabBranchMiner, params, progress_callback, stop_event)

def mine_gitlab_contributors(params, progress_callback, stop_event):
    return mine_data(GitLabContributorMiner, params, progress_callback, stop_event)

def mine_gitlab_commits(params, progress_callback, stop_event):
    return mine_data(GitLabCommitMiner, params, progress_callback, stop_event)
