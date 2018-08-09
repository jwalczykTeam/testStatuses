'''
Created on Jun 2, 2016

@author: alberto
'''

from doi.common import PermanentlyFailedException
from doi.common import FailedException
from doi.jobs.miner import fields as f
from doi.jobs.miner import pipeline
from doi.jobs.miner import exception
from doi.util import file_util
from doi.util import log_util
from doi.util import retry_util
from doi.util import uri_util
import pygit2
import git
import os
from subprocess import call
import time
import uuid


logger = log_util.get_logger("astro.jobs.miner.git2repo")


class GitReader(pipeline.Reader):

    def __init__(self, git_dir):
        super(GitReader, self).__init__()
        self.git_dir = git_dir
        self._repo = None

    @property
    def repo(self):
        if self._repo is None:
            self._repo = pygit2.Repository(self.git_dir)
        return self._repo

    def mercurial_to_git(self, git_url, access_token=None):
        raise Exception(
            "Mercurial repositories for BitBucket is no longer supported")

    def clone(
            self,
            git_url,
            access_token=None,
            max_git_objects=None,
            streaming=False,
            source_type=None):
        class CloneCallbacks(pygit2.RemoteCallbacks):

            def __init__(self, credentials, max_total_object):
                super(CloneCallbacks, self).__init__(credentials=credentials)
                self.max_total_objects = max_total_object
                self.total_objects = None
                self.repo_limit_exceeded = False

            def transfer_progress(self, stats):
                if (self.max_total_objects is not None and
                        self.total_objects is None):
                    self.total_objects = stats.total_objects
                    if self.total_objects > self.max_total_objects:
                        self.repo_limit_exceeded = True
                        raise Exception("Repo limit exceeded")

        try:
            start_time = time.mktime(time.localtime())
            if os.path.exists(self.git_dir):
                entries = os.listdir(self.git_dir)
                if ".git" in entries:
                    if streaming:
                        logger.info(
                            "Git repo already exists: git_dir='%s' ...",
                            self.git_dir)

                        try:
                            self.pull(access_token, git_url)
                        except Exception as e:
                            # if git fails we try to reclone
                            logger.error("Pull of repo {} failed".format(
                                git_url))
                            logger.error("error: {}".format(e))
                            logger.info("Attempting to reclone")
                            self.clone(
                                git_url,
                                access_token,
                                max_git_objects,
                                False)
                        return None
                    else:
                        logger.info(
                            "Removing previous git repo content: git_dir='%s'",
                            self.git_dir)
                        file_util.remove_dir_tree(self.git_dir)
                else:
                    logger.error("Not a git repo: git_dir='%s'", self.git_dir)
                    raise ValueError(
                        "Not a git repo: git_dir='{}'".format(self.git_dir))
            if source_type is "gitlab_repo":
                # handles new internal git auth methods
                credentials = pygit2.UserPass(access_token, access_token)
            else:
                credentials = pygit2.UserPass(access_token, "x-oauth-basic")
            callbacks = CloneCallbacks(credentials, max_git_objects)
            pygit2.clone_repository(git_url, self.git_dir, callbacks=callbacks)
            end_time = time.mktime(time.localtime())
            elapsed_time = end_time - start_time
            logger.info("Elapsed time: %d mins and %d seconds",
                        int(elapsed_time / 60), int(elapsed_time % 60))
        except pygit2.GitError as e:
            if callbacks.repo_limit_exceeded:
                raise PermanentlyFailedException(
                    "about:blank",
                    "User limit exceeded",
                    403,
                    "Astro repo limit exceeded: max_allowed_git_objects={}, found_git_objects={}".format(
                        max_git_objects, callbacks.total_objects))

            # sometimes libgit2 just errors for the clone with the msg "SSL
            # error: syscall failure:". in that case, tries again
            if "syscall failure" in str(e):
                raise FailedException("about:blank",
                                      "Git SSL Error",
                                      500,
                                      str(e))

            raise e

    def is_empty(self):
        repo = self.repo
        all_branches = repo.listall_branches(
            pygit2.GIT_BRANCH_LOCAL | pygit2.GIT_BRANCH_REMOTE)
        return not all_branches

    def checkout(self, branch_name):
        repo = self.repo
        branch = repo.lookup_branch(branch_name, pygit2.GIT_BRANCH_LOCAL)
        if branch is None:
            remote_branch_name = branch_name if '/' in branch_name else "origin/" + branch_name
            branch = repo.lookup_branch(
                remote_branch_name, pygit2.GIT_BRANCH_REMOTE)
        if branch is None:
            logger.error("Branch not found: branch='%s', git_dir='%s'",
                         branch_name, self.git_dir)
            raise exception.NotFoundException(
                "Branch not found: branch='{}', git_dir='{}'".format(branch_name, self.git_dir))
        ref = repo.lookup_reference(branch.name)
        repo.checkout(ref)

    @retry_util.retry
    def pull(self, access_token, git_url, remote_ref='origin'):
        logger.info("Pulling '%s' ...", self.git_dir)
        start_time = time.mktime(time.localtime())
        url_with_token = uri_util.add_token_to_uri(git_url, access_token)
        repo = git.Git(self.git_dir)
        repo.execute(['git', 'pull', url_with_token])
        end_time = time.mktime(time.localtime())
        elapsed_time = end_time - start_time
        logger.info("Elapsed time of pull: %d mins and %d seconds",
                    int(elapsed_time / 60), int(elapsed_time % 60))


class GitCommitReader(GitReader):
    # Designed not to throw exceptions

    def __init__(self, git_dir, branch_name):
        super(GitCommitReader, self).__init__(git_dir)
        self.branch_name = branch_name

    def iter(self):
        self.checkout(self.branch_name)
        sequence_no = 1
        for commit in self.repo.walk(
                self.repo.head.target,
                pygit2.GIT_SORT_TOPOLOGICAL | pygit2.GIT_SORT_TIME | pygit2.GIT_SORT_REVERSE):
            norm_commit = self.normalize_commit(commit, sequence_no)
            sequence_no += 1
            yield norm_commit

    def normalize_commit(self, commit, sequence_no):
        norm_commit = {}
        norm_commit[f.COMMIT_ID] = commit.hex

        try:
            norm_commit[f.COMMIT_MESSAGE] = commit.message
        except LookupError:
            norm_commit[f.COMMIT_MESSAGE] = commit.message.encode('latin-1')

        author = commit.author
        norm_commit[f.COMMIT_AUTHOR_NAME] = author.name
        norm_commit[f.COMMIT_AUTHOR_EMAIL] = author.email
        norm_commit[f.COMMIT_AUTHORED_TIME] = author.time * 1000

        committer = commit.committer
        norm_commit[f.COMMIT_COMMITTER_NAME] = committer.name
        norm_commit[f.COMMIT_COMMITTER_EMAIL] = committer.email
        norm_commit[f.COMMIT_COMMITTED_TIME] = committer.time * 1000

        norm_commit[f.COMMIT_BRANCH_NAME] = self.branch_name
        norm_commit[f.SEQUENCE_NO] = sequence_no
        norm_commit[f.TIMESTAMP] = norm_commit[f.COMMIT_COMMITTED_TIME]
        return norm_commit


class GitCommittedFileReader(GitReader):
    MAX_FILE_LINE_COUNT = 100000
    MAX_FILE_SIZE = 10485760  # 10 MB

    STATUS_ACTION_MAP = {
        pygit2.GIT_DELTA_ADDED: "added",
        pygit2.GIT_DELTA_DELETED: "removed",
        pygit2.GIT_DELTA_MODIFIED: "modified",
        pygit2.GIT_DELTA_RENAMED: "renamed",
        # same mappping as in github
        pygit2.GIT_DELTA_COPIED: "copied"
    }

    # Designed not to throw exceptions
    def __init__(self, git_dir, **options):
        super(GitCommittedFileReader, self).__init__(git_dir)
        self.file_black_list = options.get('file_black_list')
        self.mine_blame_info = options.get('mine_blame_info')
        self.mine_blame_info = False
        self.file_key_by_path = {}
        self.ignore_list = set()

    def iter(self, commit_id):
        for committed_file in self.get_files(commit_id):
            yield committed_file

    def get_files(self, commit_id):
        commit = self.repo[commit_id]
        parents = commit.parents
        if parents:
            diff = commit.tree.diff_to_tree(
                parents[0].tree, pygit2.GIT_DIFF_REVERSE)
        else:
            diff = commit.tree.diff_to_tree(swap=True)

        diff.find_similar(pygit2.GIT_DIFF_FIND_COPIES |
                          pygit2.GIT_DIFF_FIND_RENAMES)

        norm_files = []
        for patch in diff:
            norm_file = self.normalize_file(commit, patch)
            if norm_file is not None:
                norm_files.append(norm_file)

        return norm_files

    def normalize_file(self, commit, patch):
        delta = patch.delta

        path = delta.new_file.path

        if delta.is_binary:
            self.ignore_list.add(path)
            return None

        # Path
        if self.file_black_list and file_util.file_matches(
                path, self.file_black_list):
            logger.info("Skipped black listed file: '%s'", path)
            self.ignore_list.add(path)
            return None

        if path in self.ignore_list:
            return None

        # Previous path
        previous_path = None
        if delta.new_file.path != delta.old_file.path:
            previous_path = delta.old_file.path
            if previous_path in self.ignore_list:
                self.ignore_list.add(path)
                return None

        # Action
        action = GitCommittedFileReader.STATUS_ACTION_MAP.get(delta.status)
        if action is None:
            raise ValueError(
                "Unexpected git diff status: {}".format(delta.status))

        # Ids
        file_id = commit.hex + '/' + path
        file_key = self.get_file_key(action, path, previous_path)

        # Insertions and deletions
        line_stats = patch.line_stats
        insertions = line_stats[1]
        deletions = line_stats[2]

        if (insertions > GitCommittedFileReader.MAX_FILE_LINE_COUNT or
                deletions > GitCommittedFileReader.MAX_FILE_LINE_COUNT):
            # too big, not a human written file
            self.ignore_list.add(path)
            return None

        # Line count
        blob = self.repo.get(delta.new_file.id)
        if blob is None:
            # If not in repo it means it was deleted
            line_count = 0
            size = 0
        elif not isinstance(blob, pygit2.Blob):
            # not a blob, could be a submodule
            self.ignore_list.add(path)
            return None
        else:
            line_count = self.get_file_line_count(blob)
            size = blob.size

        if size > GitCommittedFileReader.MAX_FILE_SIZE:
            # too big, not a human written file
            self.ignore_list.add(path)
            return None

        # Line changes
        changes = []
        for hunk in patch.hunks:
            for line in hunk.lines:
                if line.origin == '+' or line.origin == '-':
                    change = {
                        f.CHANGE_TYPE: line.origin,
                        f.CHANGE_LINE_NUMBER: line.new_lineno,
                        f.CHANGE_PREVIOUS_LINE_NUMBER: line.old_lineno,
                        f.CHANGE_LINE: line.content
                    }

                    changes.append(change)

        norm_file = {
            f.COMMIT_ID: commit.hex,
            f.FILE_ID: file_id,
            f.FILE_KEY: file_key,
            f.FILE_PATH: path,
            f.FILE_PREVIOUS_PATH: previous_path,
            f.FILE_ACTION: action,
            f.FILE_ADDED_LINE_COUNT: insertions,
            f.FILE_DELETED_LINE_COUNT: deletions,
            f.FILE_CHANGED_LINE_COUNT: insertions + deletions,
            f.FILE_LINE_COUNT: line_count,
            f.FILE_SIZE: size,
            f.FILE_CHANGES: changes
        }

        if self.mine_blame_info and deletions > 0:
            self.set_origin_commit(commit, norm_file)

        return norm_file

    def get_file_key(self, action, path, previous_path):
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

    def get_file_line_count(self, blob):
        data = blob.data
        if not data:
            return 0

        new_lines_count = data.count('\n')
        return new_lines_count if data[-1] == '\n' else new_lines_count + 1

    def set_origin_commit(self, commit, norm_file):
        from doi.jobs.miner.gitblame import set_change_introduced_commit
        set_change_introduced_commit(self.git_dir, norm_file)


class GitStreamCommitReader(GitReader):

    def __init__(self, git_dir, branch_name):
        super(GitStreamCommitReader, self).__init__(git_dir)
        self.branch_name = branch_name

    def iter(self):
        self.checkout(self.branch_name)

        for commit in self.repo.walk(
                self.repo.head.target,
                pygit2.GIT_SORT_TOPOLOGICAL | pygit2.GIT_SORT_TIME):

            norm_commit = self.normalize_commit(commit)
            yield norm_commit

    def normalize_commit(self, commit):
        norm_commit = {}
        norm_commit[f.COMMIT_ID] = commit.hex

        try:
            norm_commit[f.COMMIT_MESSAGE] = commit.message
        except LookupError:
            norm_commit[f.COMMIT_MESSAGE] = commit.message.encode('latin-1')

        author = commit.author
        norm_commit[f.COMMIT_AUTHOR_NAME] = author.name
        norm_commit[f.COMMIT_AUTHOR_EMAIL] = author.email
        norm_commit[f.COMMIT_AUTHORED_TIME] = author.time * 1000

        committer = commit.committer
        norm_commit[f.COMMIT_COMMITTER_NAME] = committer.name
        norm_commit[f.COMMIT_COMMITTER_EMAIL] = committer.email
        norm_commit[f.COMMIT_COMMITTED_TIME] = committer.time * 1000

        norm_commit[f.COMMIT_BRANCH_NAME] = self.branch_name
        norm_commit[f.TIMESTAMP] = norm_commit[f.COMMIT_COMMITTED_TIME]
        return norm_commit


class GitStreamCommittedFileReader(GitReader):
    MAX_FILE_LINE_COUNT = 100000
    MAX_FILE_SIZE = 10485760  # 10 MB

    STATUS_ACTION_MAP = {
        pygit2.GIT_DELTA_ADDED: "added",
        pygit2.GIT_DELTA_DELETED: "removed",
        pygit2.GIT_DELTA_MODIFIED: "modified",
        pygit2.GIT_DELTA_RENAMED: "renamed",
        # same mappping as in github
        pygit2.GIT_DELTA_COPIED: "copied"
    }

    # Designed not to throw exceptions
    def __init__(self, git_dir, **options):
        super(GitStreamCommittedFileReader, self).__init__(git_dir)
        self.file_black_list = options.get('file_black_list')
        self.mine_blame_info = options.get('mine_blame_info')
        self.mine_blame_info = False
        self.file_key_by_path = {}
        self.ignore_list = set()

    def iter(self, commit_id):
        for committed_file in self.get_files(commit_id):
            yield committed_file

    def get_files(self, commit_id):
        commit = self.repo[commit_id]
        parents = commit.parents
        if parents:
            diff = commit.tree.diff_to_tree(
                parents[0].tree, pygit2.GIT_DIFF_REVERSE)
        else:
            diff = commit.tree.diff_to_tree(swap=True)

        diff.find_similar(pygit2.GIT_DIFF_FIND_COPIES |
                          pygit2.GIT_DIFF_FIND_RENAMES)

        norm_files = []
        for patch in diff:
            norm_file = self.normalize_file(commit, patch)
            if norm_file is not None:
                norm_files.append(norm_file)

        return norm_files

    def normalize_file(self, commit, patch):
        delta = patch.delta

        path = delta.new_file.path

        if delta.is_binary:
            self.ignore_list.add(path)
            return None

        # Path
        if self.file_black_list and file_util.file_matches(
                path, self.file_black_list):
            logger.info("Skipped black listed file: '%s'", path)
            self.ignore_list.add(path)
            return None

        if path in self.ignore_list:
            return None

        # Previous path
        previous_path = None
        if delta.new_file.path != delta.old_file.path:
            previous_path = delta.old_file.path
            if previous_path in self.ignore_list:
                self.ignore_list.add(path)
                return None

        # Action
        action = GitStreamCommittedFileReader.STATUS_ACTION_MAP.get(
            delta.status)
        if action is None:
            raise ValueError(
                "Unexpected git diff status: {}".format(delta.status))

        # Ids
        file_id = commit.hex + '/' + path
        file_key = self.get_file_key(action, path, previous_path)

        # Insertions and deletions
        line_stats = patch.line_stats
        insertions = line_stats[1]
        deletions = line_stats[2]

        if (insertions > GitStreamCommittedFileReader.MAX_FILE_LINE_COUNT or
                deletions > GitStreamCommittedFileReader.MAX_FILE_LINE_COUNT):
            # too big, not a human written file
            self.ignore_list.add(path)
            return None

        # Line count
        blob = self.repo.get(delta.new_file.id)
        if blob is None:
            # If not in repo it means it was deleted
            line_count = 0
            size = 0
        elif not isinstance(blob, pygit2.Blob):
            # not a blob, could be a submodule
            self.ignore_list.add(path)
            return None
        else:
            line_count = self.get_file_line_count(blob)
            size = blob.size

        if size > GitStreamCommittedFileReader.MAX_FILE_SIZE:
            # too big, not a human written file
            self.ignore_list.add(path)
            return None

        # Line changes
        changes = []
        for hunk in patch.hunks:
            for line in hunk.lines:
                if line.origin == '+' or line.origin == '-':
                    change = {
                        f.CHANGE_TYPE: line.origin,
                        f.CHANGE_LINE_NUMBER: line.new_lineno,
                        f.CHANGE_PREVIOUS_LINE_NUMBER: line.old_lineno,
                        f.CHANGE_LINE: line.content
                    }

                    changes.append(change)

        norm_file = {
            f.COMMIT_ID: commit.hex,
            f.FILE_ID: file_id,
            f.FILE_KEY: file_key,
            f.FILE_PATH: path,
            f.FILE_PREVIOUS_PATH: previous_path,
            f.FILE_ACTION: action,
            f.FILE_ADDED_LINE_COUNT: insertions,
            f.FILE_DELETED_LINE_COUNT: deletions,
            f.FILE_CHANGED_LINE_COUNT: insertions + deletions,
            f.FILE_LINE_COUNT: line_count,
            f.FILE_SIZE: size,
            f.FILE_CHANGES: changes
        }

        if self.mine_blame_info and deletions > 0:
            self.set_origin_commit(commit, norm_file)

        return norm_file

    def get_file_key(self, action, path, previous_path):
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

    def get_file_line_count(self, blob):
        data = blob.data
        if not data:
            return 0

        new_lines_count = data.count('\n')
        return new_lines_count if data[-1] == '\n' else new_lines_count + 1

    def set_origin_commit(self, commit, norm_file):
        from doi.jobs.miner.gitblame import set_change_introduced_commit
        set_change_introduced_commit(self.git_dir, norm_file)
