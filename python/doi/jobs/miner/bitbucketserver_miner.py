
from doi.jobs.miner import bitbucketserver
from doi.jobs.miner import git2repo as gitrepo
from doi.jobs.miner import git_miner
from doi.jobs.miner.miner import mine_data


class BitBucketServerBranchMiner(git_miner.GitBranchMiner):
    def __init__(self, params, progress_callback, stop_event):
        super(BitBucketServerBranchMiner, self).__init__(params, progress_callback, stop_event,
                                                'bitbucketserver_repo', bitbucketserver.BitBucketServerClient.REPO_URI_FORMAT)

    def get_branch_reader(self, api_base_url, access_token, repo_owner, repo_name):
        return bitbucketserver.BitBucketServerBranchReader(api_base_url, access_token, repo_owner, repo_name)

    def get_branch_normalizer(self, repo_uri):
        return bitbucketserver.BitBucketServerBranchNormalizer(repo_uri)


class BitBucketServerCommitMiner(git_miner.GitCommitMiner):
    def __init__(self, params, progress_callback, stop_event):
        super(BitBucketServerCommitMiner, self).__init__(params, progress_callback, stop_event,
                                                'bitbucketserver_repo', bitbucketserver.BitBucketServerClient.REPO_URI_FORMAT, 
                                                bitbucketserver.BitBucketServerClient.COMMIT_URI_FORMAT, '')

    def get_commit_reader(self, git_dir, branch_name):
        return gitrepo.GitCommitReader(git_dir, branch_name)

    def get_committed_file_reader(self, git_dir, **options):
        return gitrepo.GitCommittedFileReader(git_dir, **options)
        
    @staticmethod
    def get_default_branch_name(repo_uri, access_token):
        default_branch_uri = bitbucketserver.BitBucketServerClient.BRANCH_DEFAULT_URI_FORMAT.format(repo_uri)
        repo = bitbucketserver.get_repo(default_branch_uri, access_token)
        return repo['displayId']
        

def mine_bitbucketserver_branches(params, progress_callback, stop_event):
    return mine_data(BitBucketServerBranchMiner, params, progress_callback, stop_event)

def mine_bitbucketserver_commits(params, progress_callback, stop_event):
    return mine_data(BitBucketServerCommitMiner, params, progress_callback, stop_event)
