'''
Created on Jun 2, 2016

@author: alberto
'''

from doi.jobs.miner import fields as f
from doi.jobs.miner import github
from doi.jobs.miner import pipeline
from doi.jobs.miner.gitblame import set_change_introduced_commit
from doi.util import diff_util 
from doi.util import file_util
from doi.util import log_util
from doi.util import uri_util
import git
import os
import subprocess
import time
import uuid
from StringIO import StringIO


logger = log_util.get_logger("astro.jobs.miner.gitrepo")


MAX_DIFF_SIZE = 10485760 # 10 MB
FILE_PATCH = 'file_patch'


class GitReader(pipeline.Reader):
    def __init__(self, git_dir):
        super(GitReader, self).__init__() 
        self.git_dir = git_dir

    def checkout(self, branch_name):
        # Checkout the specified branch   
        try:
            subprocess.check_output(
                ['git', 'checkout', branch_name], 
                stderr=subprocess.STDOUT,
                cwd=self.git_dir)
            return True
        except subprocess.CalledProcessError as e:                                                                                                   
            print "error code", e.returncode, e.output
            branch_not_found_err = "error: pathspec '{}' did not match any file(s) known to git.\n".format(branch_name)
            if e.output == branch_not_found_err: 
                return False
            else:
                raise e     
    
    def clone(self, git_url, username=None, password=None):
        start_time = time.mktime(time.localtime())
        if os.path.exists(self.git_dir):
            entries = os.listdir(self.git_dir)
            if ".git" in entries:
                logger.info("Removing previous git repo content: '%s' ...", self.git_dir)
                file_util.remove_dir_tree(self.git_dir)
            else:
                raise ValueError("Not a git repo: {}".format(self.git_dir))

        full_git_url = uri_util.add_creds_to_uri(git_url, username, password)
        git.Repo.clone_from(full_git_url, self.git_dir)
        end_time = time.mktime(time.localtime())
        elapsed_time = end_time - start_time
        logger.info("Elapsed time: %d mins and %d seconds",
            int(elapsed_time / 60), int(elapsed_time % 60))

    def pull(self):
        start_time = time.mktime(time.localtime())
        repo = git.Repo(self.git_dir)
        origin = repo.remotes.origin
        logger.info("Pulling '%s' ...", self.git_dir)
        origin.pull()
        end_time = time.mktime(time.localtime())
        elapsed_time = end_time - start_time
        logger.info("Elapsed time: %d mins and %d seconds",
            int(elapsed_time / 60), int(elapsed_time % 60))
        

class GitCommitsReader(GitReader):
    CommitFields = [
        f.COMMIT_ID, 
        f.COMMIT_AUTHOR_NAME, f.COMMIT_AUTHOR_EMAIL, f.COMMIT_AUTHORED_TIME, 
        f.COMMIT_COMMITTER_NAME, f.COMMIT_COMMITTER_EMAIL, f.COMMIT_COMMITTED_TIME, 
        f.COMMIT_MESSAGE
    ]
    
    CommitFormat = [
        '%H', 
        '%an', '%ae', '%at', 
        '%cn', '%ce', '%ct', 
        '%s'
    ]

    # Designed not to throw exceptions
    def __init__(self, git_dir, branch_name, user_key_by_ids=None):
        super(GitCommitsReader, self).__init__(git_dir)
        self.branch_name = branch_name
        self.user_key_by_ids = user_key_by_ids
        self.done = False
        
    def next(self):
        if self.done:
            raise StopIteration
        
        g = git.Git(self.git_dir)
        
        # Checkout the specified branch   
        try:
            g.execute(['git', 'checkout', self.branch_name])
        except git.exc.GitCommandError as e:
            branch_not_found_err = "error: pathspec '{}' did not match any file(s) known to git.".format(self.branch_name)
            if e.stderr == branch_not_found_err: 
                self.done = True
                return []
            else:
                raise e        
         
        # Get the commits log   
        commit_format = '%x1f'.join(GitCommitsReader.CommitFormat) + '%x1e'
        log_bytes_out = g.log('--pretty=format:{}'.format(commit_format), 
####                          '--full-history',
                              '--reverse',
                              self.branch_name, 
                              stdout_as_string=False)            

        log_out = log_bytes_out.decode(encoding='utf-8', errors='replace')            
        # Split by the record separator
        rows = log_out.strip('\n\x1e').split("\x1e")
        # Split by the field separator
        rows = [row.strip().split("\x1f") for row in rows]
        commits = []
        for row in rows:
            commit = dict(zip(GitCommitsReader.CommitFields, row))

            if f.COMMIT_MESSAGE not in commit:
                commit[f.COMMIT_MESSAGE] = ""
            
            # Convert time field values from seconds to milliseconds
            commit[f.COMMIT_AUTHORED_TIME] = long(commit[f.COMMIT_AUTHORED_TIME]) * 1000 
            commit[f.COMMIT_COMMITTED_TIME] = long(commit[f.COMMIT_COMMITTED_TIME]) * 1000
            commit[f.TIMESTAMP] = commit[f.COMMIT_COMMITTED_TIME]
            
            if self.user_key_by_ids is not None:
                author_ids = {
                    "name": commit.get(f.COMMIT_AUTHOR_NAME), 
                    "email": commit.get(f.COMMIT_AUTHOR_EMAIL)
                }
                
                author_key = self.user_key_by_ids.get(author_ids)
                if author_key is not None:
                    commit[f.COMMIT_AUTHOR_KEY] = author_key 
                
                committer_ids = {
                    "name": commit.get(f.COMMIT_COMMITTER_NAME), 
                    "email": commit.get(f.COMMIT_COMMITTER_EMAIL)
                }
                
                committer_key = self.user_key_by_ids.get(committer_ids)
                if committer_key is not None:
                    commit[f.COMMIT_COMMITTER_KEY] = committer_key 
            
            commits.append(commit)
            
        self.done = True
        return commits


class GitCommittedFilesReader(GitReader):
    # Designed not to throw exceptions
    def __init__(self, git_dir, commit_ids, **options):
        super(GitCommittedFilesReader, self).__init__(git_dir)
        self.commit_ids = commit_ids
        self.file_black_list = options.get('file_black_list')
        self.mine_blame_info = options.get('mine_blame_info')        
        self.commit_ids_index = 0
        self.file_key_by_path = {}

    def next(self):
        if self.commit_ids_index == 0:
            # First time
            out = subprocess.check_output(
                ['git', 'hash-object', "-t", "tree", "/dev/null"], 
                cwd=self.git_dir)
            self.root_commit_id = out.rstrip("\n")
            
        if self.commit_ids_index == len(self.commit_ids):
            raise StopIteration
                    
        commit_id = self.commit_ids[self.commit_ids_index]
        logger.info("Commit: %s (%d of %d - %d%%)",
            commit_id, 
            self.commit_ids_index +1, 
            len(self.commit_ids), 
            (self.commit_ids_index +1) * 100 / len(self.commit_ids))
        committed_files = self.get_committed_files(commit_id)
        self.commit_ids_index += 1
        return committed_files

    def get_committed_files(self, commit_id):
        proc = subprocess.Popen(['git', 'show', '-M', '-m', '-z',
                                 '--pretty=format:%H', 
                                 commit_id], 
                                cwd=self.git_dir,
                                stdout=subprocess.PIPE)
        try:
            line = proc.stdout.readline()
            if not line:
                return []
            
            assert line.startswith(commit_id), "Unexpected git show output"
            
            committed_files = []
            diff_buff = None
            diff_size = 0
            stdout_line_count = 1
            for line in proc.stdout:
                line = line.decode('utf-8', errors='replace')
                if line.startswith("\0"):
                    logger.info("Skipped commit from second parent starting at line %d", stdout_line_count)
                    break        
                
                if line.startswith("diff --git"):
                    diff_line = line.rstrip()
                    if diff_buff is not None:
                        if diff_size < MAX_DIFF_SIZE:
                            committed_file = self.build_committed_file(commit_id, diff_buff.getvalue())
                            if committed_file is not None:
                                committed_files.append(committed_file)
                        else:
                            logger.info("Skipped diff '%s': size=%d", diff_line, diff_size)
                    
                    diff_buff = StringIO()
                    diff_size = 0
                
                if diff_size < MAX_DIFF_SIZE:
                    diff_buff.write(line)
                
                diff_size += len(line)
                stdout_line_count += 1
                
            if diff_buff is not None:
                if diff_size < MAX_DIFF_SIZE:
                    committed_file = self.build_committed_file(commit_id, diff_buff.getvalue())
                    if committed_file is not None:
                        committed_files.append(committed_file)
                else:
                    logger.info("Skipped diff '%s': size=%d", diff_line, diff_size)
            
            return committed_files
        finally:                
            proc.terminate()

    def build_committed_file(self, commit_id, patch):
        committed_file = {
            f.COMMIT_ID: commit_id
        }
        
        diff = diff_util.parse_patch(patch, parse_header=True)
        if diff.is_binary():
            return None
    
        committed_file[f.FILE_PATH] = diff.get_path()
        if self.file_black_list and file_util.file_matches(
                committed_file[f.FILE_PATH], self.file_black_list):
            logger.info("Skipped black listed file: '%s'", committed_file[f.FILE_PATH])
            return None

        committed_file[f.FILE_ACTION]           = diff.get_action()
        if committed_file[f.FILE_ACTION] == 'renamed':
            committed_file[f.FILE_PREVIOUS_PATH] = diff.renamed_from
            
        committed_file[f.FILE_ADDED_LINE_COUNT]   = diff.added_lines
        committed_file[f.FILE_DELETED_LINE_COUNT] = diff.deleted_lines
        committed_file[f.FILE_CHANGED_LINE_COUNT] = diff.added_lines + diff.deleted_lines
           
        file_line_count = 0
        if committed_file[f.FILE_ACTION] != 'removed':
            file_line_count = self.get_file_line_count(commit_id, committed_file[f.FILE_PATH])
        committed_file[f.FILE_LINE_COUNT] = file_line_count
        
        file_key = self.get_file_key(
            committed_file[f.FILE_PATH], 
            committed_file[f.FILE_ACTION], 
            committed_file.get(f.FILE_PREVIOUS_PATH))
        committed_file[f.FILE_KEY] = file_key
            
        committed_changes = []
        for hunk in diff.hunks:
            for change in hunk.changes:
                committed_change = {}
                committed_change[f.CHANGE_TYPE] = change.kind
                committed_change[f.CHANGE_LINE_NUMBER] = change.line_number
                committed_change[f.CHANGE_LINE] = change.line
                committed_changes.append(committed_change)

        committed_file[f.FILE_CHANGES] = committed_changes

        if self.mine_blame_info:
            set_change_introduced_commit(self.git_dir, committed_file)
        
        return committed_file

    def get_file_key(self, path, action, previous_path):
        if action == 'renamed':
            if previous_path in self.file_key_by_path:
                previous_file_key = self.file_key_by_path[previous_path]
                del self.file_key_by_path[previous_path]
                self.file_key_by_path[path] = previous_file_key
                              
        if path not in self.file_key_by_path:
            self.file_key_by_path[path] = str(uuid.uuid1())
            
        file_key = self.file_key_by_path[path]
    
        if action == 'removed':
            del self.file_key_by_path[path]
    
        return file_key


    def get_file_line_count(self, commit_id, file_path):
        def _get_file_line_count(file_path):
            out = subprocess.check_output(
                ["git", "diff", "--stat", 
                self.root_commit_id, commit_id, 
                "--", file_path],
                cwd=self.git_dir)
            if not out:
                return None
            lines = out.split('\n')        
            counts = [int(s) for s in lines[1].split() if s.isdigit()]
            return counts[1]
    
        # By default escape shell control characters
        file_path = file_util.escape_git_diff_path(file_path)
        file_line_count = _get_file_line_count(file_path)
        if file_line_count is not None:
            return file_line_count
        
        raise ValueError('Could not get file line count: git diff --stat {} {} -- "{}"'.format(
            self.root_commit_id, commit_id, file_path))
