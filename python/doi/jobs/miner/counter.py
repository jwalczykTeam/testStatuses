'''
Created on Jul 16, 2016

@author: alberto
'''


from doi.jobs.miner.resourcedb import ResourceDB
from doi.jobs.miner import fields as f
from doi.jobs.miner import pipeline
from doi.util import log_util
import uuid

logger = log_util.get_logger("astro.jobs.miner.counters")


class CommitCounter(pipeline.Updater):
    '''
    Set the following commit fields:
     - COMMIT_AUTHOR_COMMIT_COUNT
     - COMMIT_COMMITTER_COMMIT_COUNT

    It is important that commits are passed in topological/committed time order (asc)
    '''

    class UserCounts(object):

        def __init__(self):
            self.commit_count = 0
            self.changed_line_count = 0

    def __init__(self):
        super(CommitCounter, self).__init__()
        self.counts_by_author = {}
        self.counts_by_committer = {}

    def update(self, commit):
        author_counts = self.get_counts(
            self.counts_by_author, commit[f.COMMIT_AUTHOR_KEY])
        committer_counts = self.get_counts(
            self.counts_by_committer, commit[f.COMMIT_COMMITTER_KEY])

        commit[f.COMMIT_AUTHOR_COMMIT_COUNT] = author_counts.commit_count
        commit[f.COMMIT_COMMITTER_COMMIT_COUNT] = committer_counts.commit_count

    def get_counts(self, counts_by_user, key):
        counts = counts_by_user.get(key)
        if counts is None:
            counts = self.UserCounts()
            counts_by_user[key] = counts
        counts.commit_count += 1
        return counts


class CommittedFileCounter(pipeline.Updater):
    '''
    Set the following commit fields:
     - FILE_TOTAL_COMMIT_COUNT
     - FILE_TOTAL_CHANGED_LINE_COUNT
     - FILE_AUTHOR_COMMIT_COUNT
     - FILE_AUTHOR_CHANGED_LINE_COUNT
     - FILE_COMMITTER_COMMIT_COUNT
     - FILE_COMMITTER_CHANGED_LINE_COUNT

    It is important that that committed files are presented in topological/committed time order (asc)
    '''

    class FileCounts(object):

        def __init__(self):
            self.commit_count = 0
            self.changed_line_count = 0
            self.counts_by_author = {}
            self.counts_by_committer = {}

    class UserCounts(object):

        def __init__(self):
            self.commit_count = 0
            self.changed_line_count = 0

    def __init__(self):
        super(CommittedFileCounter, self).__init__()
        self.counts_by_file_key = {}

    def update(self, committed_file):
        file_key = committed_file[f.FILE_KEY]
        file_changed_line_count = committed_file[f.FILE_CHANGED_LINE_COUNT]

        file_counts = self.counts_by_file_key.get(file_key)
        if file_counts is None:
            file_counts = self.FileCounts()
            self.counts_by_file_key[file_key] = file_counts

        file_counts.commit_count += 1
        file_counts.changed_line_count += file_changed_line_count

        author_counts = self.get_counts(
            file_counts.counts_by_author,
            committed_file[f.COMMIT_AUTHOR_KEY],
            file_changed_line_count)
        committer_counts = self.get_counts(
            file_counts.counts_by_committer,
            committed_file[f.COMMIT_COMMITTER_KEY],
            file_changed_line_count)

        committed_file[f.FILE_TOTAL_COMMIT_COUNT] = file_counts.commit_count
        committed_file[
            f.FILE_TOTAL_CHANGED_LINE_COUNT] = file_counts.changed_line_count
        committed_file[f.FILE_AUTHOR_COMMIT_COUNT] = author_counts.commit_count
        committed_file[
            f.FILE_AUTHOR_CHANGED_LINE_COUNT] = author_counts.changed_line_count
        committed_file[
            f.FILE_COMMITTER_COMMIT_COUNT] = committer_counts.commit_count
        committed_file[
            f.FILE_COMMITTER_CHANGED_LINE_COUNT] = committer_counts.changed_line_count

    def get_counts(self, counts_by_user, key, changed_line_count):
        counts = counts_by_user.get(key)
        if counts is None:
            counts = self.UserCounts()
            counts_by_user[key] = counts

        counts.commit_count += 1
        counts.changed_line_count += changed_line_count
        return counts


class CommitEventCounter(pipeline.Updater):

    def __init__(self, resource_db):
        super(CommitEventCounter, self).__init__()
        self.resource_db = resource_db

    def update(self, commit):
        author_key = commit.get(f.COMMIT_AUTHOR_KEY)
        if author_key is not None:
            commit[f.COMMIT_AUTHOR_COMMIT_COUNT] = self.get_prev_commit_count(
                f.COMMIT_AUTHOR_KEY, author_key,
                commit[f.COMMIT_SOURCE_URI],
                commit[f.COMMIT_COMMITTED_TIME]) + 1
        else:
            commit[f.COMMIT_AUTHOR_COMMIT_COUNT] = 0
        committer_key = commit.get(f.COMMIT_COMMITTER_KEY)
        if committer_key is not None:
            commit[
                f.COMMIT_COMMITTER_COMMIT_COUNT] = self.get_prev_commit_count(
                f.COMMIT_COMMITTER_KEY, committer_key,
                commit[f.COMMIT_SOURCE_URI],
                commit[f.COMMIT_COMMITTED_TIME]) + 1
        else:
            commit[f.COMMIT_COMMITTER_COMMIT_COUNT] = 0

    def get_prev_commit_count(self,
                              user_key, user_key_value,
                              commit_source_uri,
                              committed_time):
        count = self.resource_db.count(
            ResourceDB.COMMIT_TYPE.name,
            {user_key: user_key_value,
             f.COMMIT_SOURCE_URI: commit_source_uri},
            {f.COMMIT_COMMITTED_TIME: committed_time})
        return count


class CommittedFileEventCounter(pipeline.Updater):

    def __init__(self, resource_db, git_repo_url=None):
        super(CommittedFileEventCounter, self).__init__()
        self.resource_db = resource_db
        self.git_repo_url = git_repo_url

        self.mapping = {}  # file_key => committed_file

    def update(self, committed_file):
        last_committed_file = self.add_uri(committed_file)
        self.add_counts_to_last(committed_file, last_committed_file)
        self.mapping[committed_file[f.FILE_KEY]] = committed_file

    def add_uri(self, committed_file):
        last_committed_file = None

        if committed_file[f.FILE_KEY] in self.mapping:
            last_committed_file = self.mapping[committed_file[f.FILE_KEY]]
        else:
            commit_source_uri = committed_file[f.COMMIT_SOURCE_URI]

            if committed_file[f.FILE_ACTION] == 'renamed':
                file_previous_path = committed_file[f.FILE_PREVIOUS_PATH]
                last_committed_file = self.get_last_committed_file(
                    commit_source_uri, file_previous_path)
            else:
                file_path = committed_file[f.FILE_PATH]
                last_committed_file = self.get_last_committed_file(
                    commit_source_uri, file_path)

            if last_committed_file is None or last_committed_file[f.FILE_ACTION] == 'removed':
                committed_file[f.FILE_KEY] = str(uuid.uuid1())
            else:
                committed_file[f.FILE_KEY] = last_committed_file[f.FILE_KEY]

        return last_committed_file

    def add_counts_to_last(self, committed_file, last_committed_file):
        file_key = committed_file[f.FILE_KEY]
        committed_time = committed_file[f.COMMIT_COMMITTED_TIME]

        if last_committed_file is None:  # new file
            committed_file[f.FILE_TOTAL_COMMIT_COUNT] = 1
            committed_file[f.FILE_TOTAL_CHANGED_LINE_COUNT] = committed_file[
                f.FILE_CHANGED_LINE_COUNT]
        else:
            committed_file[f.FILE_TOTAL_COMMIT_COUNT] = last_committed_file[
                f.FILE_TOTAL_COMMIT_COUNT] + 1
            committed_file[f.FILE_TOTAL_CHANGED_LINE_COUNT] = last_committed_file[f.FILE_TOTAL_CHANGED_LINE_COUNT] +\
                committed_file[f.FILE_CHANGED_LINE_COUNT]

        author_key = committed_file.get(f.COMMIT_AUTHOR_KEY)
        if author_key is not None:
            committed_file[f.FILE_AUTHOR_COMMIT_COUNT] = self.count_by_file_and_user(
                file_key,
                f.COMMIT_AUTHOR_KEY, author_key,
                committed_time) + 1

            committed_file[f.FILE_AUTHOR_CHANGED_LINE_COUNT] = self.sum_by_file_and_user(
                file_key,
                f.COMMIT_AUTHOR_KEY, author_key,
                committed_time,
                f.FILE_CHANGED_LINE_COUNT) + committed_file[f.FILE_CHANGED_LINE_COUNT]
        else:
            committed_file[f.COMMIT_AUTHOR_KEY] = str(uuid.uuid1())
            committed_file[f.FILE_AUTHOR_COMMIT_COUNT] = long(1)
            committed_file[f.FILE_AUTHOR_CHANGED_LINE_COUNT] = long(
                committed_file[f.FILE_CHANGED_LINE_COUNT])

        committer_key = committed_file.get(f.COMMIT_COMMITTER_KEY)
        if committer_key is not None:
            committed_file[f.FILE_COMMITTER_COMMIT_COUNT] = self.count_by_file_and_user(
                file_key,
                f.COMMIT_COMMITTER_KEY, committer_key,
                committed_time) + 1

            committed_file[f.FILE_COMMITTER_CHANGED_LINE_COUNT] = self.sum_by_file_and_user(
                file_key,
                f.COMMIT_COMMITTER_KEY, committer_key,
                committed_time,
                f.FILE_CHANGED_LINE_COUNT) + committed_file[f.FILE_CHANGED_LINE_COUNT]
        else:
            committed_file[f.COMMIT_COMMITTER_KEY] = str(uuid.uuid1())
            committed_file[f.FILE_COMMITTER_COMMIT_COUNT] = long(1)
            committed_file[f.FILE_COMMITTER_CHANGED_LINE_COUNT] = long(
                committed_file[f.FILE_CHANGED_LINE_COUNT])

    def count_by_file_and_user(self, file_key, user_key, user_key_value, committed_time):
        count = self.resource_db.count(
            ResourceDB.COMMITTED_FILE_TYPE.name,
            {f.FILE_KEY: file_key, user_key: user_key_value},
            {f.COMMIT_COMMITTED_TIME: committed_time})
        return long(count)

    def sum_by_file_and_user(self, file_key, user_key, user_key_value, committed_time, counter_field_name):
        count, = self.resource_db.sum(
            ResourceDB.COMMITTED_FILE_TYPE.name,
            {f.FILE_KEY: file_key, user_key: user_key_value},
            {f.COMMIT_COMMITTED_TIME: committed_time},
            counter_field_name)
        return long(count)

    def get_last_committed_file(self, commit_source_uri, file_path):
        results = self.resource_db.find(
            ResourceDB.COMMITTED_FILE_TYPE.name,
            key_values={f.COMMIT_SOURCE_URI: commit_source_uri,
                        f.FILE_PATH: file_path},
            size=1, sort_key=f.FILE_TOTAL_COMMIT_COUNT, sort_order="desc")

        committed_file = None
        if results:
            committed_file = results[0]
            logger.debug("Last committed file: source_uri='%s', file_path='%s', total_commit_count=%d",
                         committed_file[f.COMMIT_SOURCE_URI], committed_file[
                             f.FILE_PATH],
                         committed_file[f.FILE_TOTAL_COMMIT_COUNT])
        else:
            logger.debug("No previous committed file for source_uri='%s', file_path='%s'",
                         commit_source_uri, file_path)

        return committed_file
