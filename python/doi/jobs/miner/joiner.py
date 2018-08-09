'''
Created on Jul 16, 2016

@author: alberto
'''

from doi.jobs.miner import fields as f
from doi.jobs.miner import counter
from doi.jobs.miner import pipeline
from doi.jobs.miner.resourcedb import ResourceDB, ResourceDBBulkReader
from doi.jobs.miner.resourcedb import ResourceDBBulkUpdater
from doi.util import log_util
from doi.util import uuid_util
from doi.common import StoppedException, TaskMethod
from operator import itemgetter


logger = log_util.get_logger("astro.jobs.miner.joiner")


class CommitIssueUserJoiner(pipeline.Updater):

    def __init__(self, issues_by_key, user_keys_by_ids, users_by_key):
        super(CommitIssueUserJoiner, self).__init__()
        self.issues_by_key = issues_by_key
        self.user_keys_by_ids = user_keys_by_ids
        self.users_by_key = users_by_key
        self.matched_issue_count = 0
        self.missing_issue_count = 0
        self.matched_user_count = 0
        self.missing_user_count = 0

    def update(self, commit):
        self.join_issues(commit)
        self.add_user_keys(commit)
        self.join_user(commit,
                       commit.get(f.COMMIT_AUTHOR_KEY),
                       'commit_author_')
        self.join_user(commit,
                       commit.get(f.COMMIT_COMMITTER_KEY),
                       'commit_committer_')

    def join_issues(self, commit):
        issues = []
        for issue_key in commit[f.COMMIT_ISSUE_KEYS]:
            issue = self.issues_by_key.get(issue_key)
            if issue is None:
                self.missing_issue_count += 1
                continue

            issues.append(issue)
            self.matched_issue_count += 1

        commit[f.COMMIT_ISSUES] = issues

    def add_user_keys(self, commit):
        author_ids = {
            "name": commit.get(f.COMMIT_AUTHOR_NAME),
            "email": commit.get(f.COMMIT_AUTHOR_EMAIL)
        }

        author_key = self.user_keys_by_ids.get(author_ids)
        if author_key is not None:
            commit[f.COMMIT_AUTHOR_KEY] = author_key

        committer_ids = {
            "name": commit.get(f.COMMIT_COMMITTER_NAME),
            "email": commit.get(f.COMMIT_COMMITTER_EMAIL)
        }

        committer_key = self.user_keys_by_ids.get(committer_ids)
        if committer_key is not None:
            commit[f.COMMIT_COMMITTER_KEY] = committer_key

    def join_user(self, commit, key, field_prefix):
        user = self.users_by_key.get(key)
        if user is None:
            self.missing_user_count += 1
            return

        self.matched_user_count += 1
        for field_name, field_value in user.iteritems():
            if field_name != f.TIMESTAMP:
                new_field_name = field_name.replace('user_', field_prefix)
                commit[new_field_name] = field_value


class CommitEventIssueUserJoiner(pipeline.Updater):

    def __init__(self, resource_db):
        super(CommitEventIssueUserJoiner, self).__init__()
        self.resource_db = resource_db

    def update(self, commit):
        self.join_issues(commit)
        # Note: commit_author_key will be added through this. It is then needed
        # for the counters
        self.join_user(commit,
                       commit.get(f.COMMIT_AUTHOR_ID),
                       commit.get(f.COMMIT_AUTHOR_NAME),
                       commit.get(f.COMMIT_AUTHOR_EMAIL),
                       'commit_author_')
        # Note: commit_committer_key will be added through this. It is then
        # needed for the counters
        self.join_user(commit,
                       commit.get(f.COMMIT_COMMITTER_ID),
                       commit.get(f.COMMIT_COMMITTER_NAME),
                       commit.get(f.COMMIT_COMMITTER_EMAIL),
                       'commit_committer_')

    def join_issues(self, commit):
        issues = []
        for issue_key in commit.get(f.COMMIT_ISSUE_KEYS):
            issue = self.resource_db.find(ResourceDB.ISSUE_TYPE.name,
                                          {f.ISSUE_KEY: issue_key}, 1)
            if issue:
                issues.append(issue[0])

        commit[f.COMMIT_ISSUES] = issues

    def join_user(self, commit, user_id, user_name, user_email, field_prefix):
        user = self.get_user(user_id, user_name, user_email)
        if user is None:
            return

        for field_name, field_value in user.iteritems():
            if field_name != f.TIMESTAMP:
                new_field_name = field_name.replace('user_', field_prefix)
                commit[new_field_name] = field_value

    def get_user(self, user_id, user_name, user_email):
        for field_name, field_value in [(f.USER_ID, user_id),
                                        (f.USER_NAME, user_name),
                                        (f.USER_EMAIL, user_email)]:
            if field_value:
                user = self.resource_db.find(ResourceDB.USER_TYPE.name,
                                             {field_name: field_value}, 1)
                if user:
                    return user[0]

        return None


def join_data(params, progress_callback, stop_event):
    job_name = params.job_name
    method = params.method

    if method == TaskMethod.DELETE:
        # Nothing to do when method is deleted. The preparation step has
        # already deleted the index
        return

    with ResourceDB(job_name) as resource_db:
        # Get all commits
        with ResourceDBBulkReader(
                resource_db, ResourceDB.COMMIT_TYPE.name) as reader:
            commits = [commit for commit in reader.iter()]
            commits.sort(key=itemgetter(f.SEQUENCE_NO))
            logger.info("Total number of commits: %d", len(commits))

        # Get all issues
        with ResourceDBBulkReader(
                resource_db, ResourceDB.ISSUE_TYPE.name) as reader:
            issues_by_key = {
                issue[f.ISSUE_KEY]: issue for issue in reader.iter()}
            logger.info("Total number of issues: %d", len(issues_by_key))

        # Get all users
        users_by_key = {}
        user_key_by_ids = uuid_util.UniqueIDByKeyValues()
        with ResourceDBBulkReader(
                resource_db, ResourceDB.USER_TYPE.name) as reader:
            contributor_count = 0
            for user in reader.iter():
                if f.USER_KEY in user:
                    users_by_key[user[f.USER_KEY]] = user

                    user_ids = {
                        "name": user.get(f.USER_NAME),
                        "email": user.get(f.USER_EMAIL)
                    }

                    user_key_by_ids.set(user_ids, user[f.USER_KEY])

                contributor_count += 1

            logger.info(
                "Total number of identifiable contributors: %d",
                len(users_by_key))

        # Join commits with issues and contributors
        with ResourceDBBulkUpdater(
                resource_db, ResourceDB.COMMIT_TYPE.name) as writer:
            # Prepare data pipeline
            commit_joiner = CommitIssueUserJoiner(
                issues_by_key, user_key_by_ids, users_by_key)
            commit_counter = counter.CommitCounter()
            committed_file_counter = counter.CommittedFileCounter()

            # Join commits
            for commit in commits:
                if stop_event.is_set():
                    raise StoppedException

                commit_joiner.update(commit)
                commit_counter.update(commit)
                writer.write(commit)

            logger.info("Total number of commits joined: %d", len(commits))
            logger.info("Matched issues: %d",
                        commit_joiner.matched_issue_count)
            logger.info("Missing issues: %d",
                        commit_joiner.missing_issue_count)
            logger.info("Matched contributors: %d",
                        commit_joiner.matched_user_count)
            logger.info("Missing contributors: %d",
                        commit_joiner.missing_user_count)

        # Join committed files with commits (already joined with issues and
        # contributors above)
        with ResourceDBBulkUpdater(
                resource_db, ResourceDB.COMMITTED_FILE_TYPE.name) as writer:
            total_committed_file_count = 0
            for commit in commits:
                # Do not fetch `file_changes`: they are big and we don't need
                # them
                exclude_fields = ["file_changes"]
                with ResourceDBBulkReader(
                        resource_db, ResourceDB.COMMITTED_FILE_TYPE.name,
                        key_values={f.COMMIT_ID: commit[f.COMMIT_ID]},
                        exclude_fields=exclude_fields) as reader:

                    committed_file_count = 0
                    for committed_file in reader.iter():
                        if stop_event.is_set():
                            raise StoppedException

                        committed_file.update(commit)
                        committed_file_counter.update(committed_file)
                        writer.write(committed_file)
                        committed_file_count += 1
                        total_committed_file_count += 1

                    logger.info("Commit '%s': %d files joined", commit[
                                f.COMMIT_ID], committed_file_count)
            logger.info("Total number of committed files joined: %d",
                        total_committed_file_count)

        return {
            "commit_count": len(commits),
            "committed_file_count": total_committed_file_count,
            "issue_count": len(issues_by_key),
            "contributor_count": contributor_count,
            "identifiable_contributor_count": len(users_by_key),
            "matched_issue_count": commit_joiner.matched_issue_count,
            "missing_issue_count": commit_joiner.missing_issue_count,
            "matched_contributor_count": commit_joiner.matched_user_count,
            "missing_contributor_count": commit_joiner.missing_user_count
        }
