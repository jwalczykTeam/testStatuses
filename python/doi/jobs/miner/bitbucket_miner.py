
from doi.jobs.miner import bitbucket
from doi.jobs.miner import git2repo as gitrepo
from doi.jobs.miner import git_miner
from doi.jobs.miner.miner import mine_data


class BitBucketIssueMiner(git_miner.GitIssueMiner):
    def __init__(self, params, progress_callback, stop_event):
        super(BitBucketIssueMiner, self).__init__(params, progress_callback, stop_event,
                                               'bitbucket_repo', bitbucket.BitBucketClient.REPO_URI_FORMAT)

    def get_issue_reader(self, api_base_url, access_token, repo_owner, repo_name):
        return bitbucket.BitBucketIssueReader(api_base_url, access_token, repo_owner, repo_name)

    def get_issue_normalizer(self, label_field_map, repo_uri):
        return bitbucket.BitBucketIssueNormalizer(label_field_map, repo_uri)
    



class BitBucketBranchMiner(git_miner.GitBranchMiner):
    def __init__(self, params, progress_callback, stop_event):
        super(BitBucketBranchMiner, self).__init__(params, progress_callback, stop_event,
                                                'bitbucket_repo', bitbucket.BitBucketClient.REPO_URI_FORMAT)

    def get_branch_reader(self, api_base_url, access_token, repo_owner, repo_name):
        return bitbucket.BitBucketBranchReader(api_base_url, access_token, repo_owner, repo_name)

    def get_branch_normalizer(self, repo_uri):
        return bitbucket.BitBucketBranchNormalizer(repo_uri)



class BitBucketCommitMiner(git_miner.GitCommitMiner):
    def __init__(self, params, progress_callback, stop_event):
        super(BitBucketCommitMiner, self).__init__(params, progress_callback, stop_event,
                                                'bitbucket_repo', bitbucket.BitBucketClient.REPO_URI_FORMAT, 
                                                bitbucket.BitBucketClient.COMMIT_URI_FORMAT, bitbucket.BitBucketClient.ISSUE_URI_FORMAT)

    def get_commit_reader(self, git_dir, branch_name):
        return gitrepo.GitCommitReader(git_dir, branch_name)

    def get_committed_file_reader(self, git_dir, **options):
        return gitrepo.GitCommittedFileReader(git_dir, **options)
        
    @staticmethod
    def get_default_branch_name(repo_uri, access_token):
        repo = bitbucket.get_repo(repo_uri, access_token)
        return repo['mainbranch']['name']
        

def mine_bitbucket_issues(params, progress_callback, stop_event):
    return mine_data(BitBucketIssueMiner, params, progress_callback, stop_event)


def mine_bitbucket_branches(params, progress_callback, stop_event):
    return mine_data(BitBucketBranchMiner, params, progress_callback, stop_event)

def mine_bitbucket_commits(params, progress_callback, stop_event):
    return mine_data(BitBucketCommitMiner, params, progress_callback, stop_event)
