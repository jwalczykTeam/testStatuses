'''
Created on Apr 9, 2016

@author: alberto
'''
#
# Common Fields
#

#: TIMESTAMP
TIMESTAMP = 'timestamp'

#: Sequence Number
SEQUENCE_NO = 'sequence_no'

#: The type of event
EVENT_TYPE = 'event_type'

#
# Commit Fields
#

#: The URI of this commit (string)
COMMIT_URI = 'commit_uri'

#: The ID of this commit (string)
COMMIT_ID = 'commit_id'

#: The message of this commit (string)
COMMIT_MESSAGE = 'commit_message'

#: The author's URI of this commit (string)
COMMIT_AUTHOR_URI = 'commit_author_uri'

#: The author's id of this commit (string)
COMMIT_AUTHOR_ID = 'commit_author_id'

#: The author's key of this commit. (string, auto-generated)
COMMIT_AUTHOR_KEY = 'commit_author_key'

#: The author's login name of this commit (string)
COMMIT_AUTHOR_LOGIN = 'commit_author_login'

#: The author's name of this commit (string)
COMMIT_AUTHOR_NAME = 'commit_author_name'

#: The author's email address of this commit (string)
COMMIT_AUTHOR_EMAIL = 'commit_author_email'

#: The time this commit was authored (long, # of ms from the epoch time)
COMMIT_AUTHORED_TIME = 'commit_author_time'

#: The commiter's URI of this commit (string)
COMMIT_COMMITTER_URI = 'commit_committer_uri'

#: The commiter's id of this commit (string)
COMMIT_COMMITTER_ID = 'commit_committer_id'

#: The commiter's key of this commit (string, auto-generated)
COMMIT_COMMITTER_KEY = 'commit_committer_key'

#: The commiter's login name of this commit (string)
COMMIT_COMMITTER_LOGIN = 'commit_committer_login'

#: The commiter's name of this commit (string)
COMMIT_COMMITTER_NAME = 'commit_committer_name'

#: The commiter's email address of this commit (string)
COMMIT_COMMITTER_EMAIL = 'commit_committer_email'

#: The time this commit was committed (long, # of ms from the epoch time)
COMMIT_COMMITTED_TIME = 'commit_committer_time'

#: The branch name of this commit
COMMIT_BRANCH_NAME = 'commit_branch_name'

#: The issues keys linked to this commit (array of strings)
COMMIT_ISSUE_KEYS = 'commit_issue_keys'

#: The issues  linked to this commit (array of objects)
COMMIT_ISSUES = 'commit_issues'

#: the container's URI of the commit (string)
COMMIT_SOURCE_URI = 'commit_source_uri'

#: the container's type of the commit (string)
COMMIT_SOURCE_TYPE = 'commit_source_type'


# Generated from commit history

#: Number of commits committed by the author before this commit (long)
COMMIT_AUTHOR_COMMIT_COUNT = 'commit_author_commit_count'

#: Number of commits committed by the commiter before this commit (long)
COMMIT_COMMITTER_COMMIT_COUNT = 'commit_committer_commit_count'


# Created from joining user data

#: The author's type of this commit (string)
COMMIT_AUTHOR_TYPE = 'commit_author_type'

#: The author's company of this commit (string)
COMMIT_AUTHOR_COMPANY = 'commit_author_company'

#: The number of public repos accessed by the author of this commit (long)
COMMIT_AUTHOR_PUBLIC_REPO_COUNT = 'commit_author_public_repo_count'

#: The number of public gists accessed by the author of this commit (long)
COMMIT_AUTHOR_PUBLIC_GIST_COUNT = 'commit_author_public_gist_count'

#: The number of users following the author of this commit (long)
COMMIT_AUTHOR_FOLLOWER_COUNT = 'commit_author_follower_count'

#: The number of users that the author of this commit is following (long)
COMMIT_AUTHOR_FOLLOWING_COUNT = 'commit_author_following_count'

#: The time the author's user record was created (long, # of ms from the epoch time)
COMMIT_AUTHOR_CREATED_TIME = 'commit_author_created_time'

#: The time the author's user record was last updated  (long, # of ms from the epoch time)
COMMIT_AUTHOR_UPDATED_TIME = 'commit_author_updated_time'

#: The committer's type of this commit (string)
COMMIT_COMMITTER_TYPE = 'commit_committer_type'

#: The committer's company of this commit (string)
COMMIT_COMMITTER_COMPANY = 'commit_committer_company'

#: The number of public repos accessed by the committer of this commit (long)
COMMIT_COMMITTER_PUBLIC_REPO_COUNT = 'commit_committer_public_repo_count'

#: The number of public gists accessed by the committer of this commit (long)
COMMIT_COMMITTER_PUBLIC_GIST_COUNT = 'commit_committer_public_gist_count'

#: The number of users that the committer of this commit is following (long)
COMMIT_COMMITTER_FOLLOWER_COUNT = 'commit_committer_follower_count'

#: The number of users that the committer of this commit is following (long)
COMMIT_COMMITTER_FOLLOWING_COUNT = 'commit_committer_following_count'

#: The time the committer's user record was created (long, # of ms from the epoch time)
COMMIT_COMMITTER_CREATED_TIME = 'commit_committer_created_time'

#: The time the committer's user record was last updated  (long, # of ms from the epoch time)
COMMIT_COMMITTER_UPDATED_TIME = 'commit_committer_updated_time'


#
# Committed File Fields
#

#: The URI of the committed file
FILE_URI = 'file_uri'

#: The ID of the committed file (one for revision)
FILE_ID = 'file_id'

#: Common file key beyond commits and renames
FILE_KEY = 'file_key'

#: The path in the repo of the committed file (string)
FILE_PATH = 'file_path'

#: The previous path in the repo of the committed file, when action='renamed' (string)
FILE_PREVIOUS_PATH = 'file_previous_path'

#: The action executed on the committed file ('added', 'removed', 'modified', 'renamed', 'moved')
FILE_ACTION = 'file_action'

#: The number of lines of this committed file (long)
FILE_LINE_COUNT = 'file_line_count'

#: The size of this committed file (long)
FILE_SIZE = 'file_size'

#: The changes in the committed file
FILE_CHANGES = 'file_changes'

#: The number of lines added to the committed file (long)
FILE_ADDED_LINE_COUNT = 'file_added_line_count'

#: The number of lines deleted from the committed file (long)
FILE_DELETED_LINE_COUNT = 'file_deleted_line_count'

#: The number of lines changed (added+deleted) in the committed file (long)
FILE_CHANGED_LINE_COUNT = 'file_changed_line_count'

#: The type of the committed file ('file', 'dir', 'link' - RTC only)
FILE_TYPE = 'file_type'


# Generated from committed file history

#: Total number of commits on this file before this commit (long)
FILE_TOTAL_COMMIT_COUNT = 'file_total_commit_count'

#: Total number of lines changed on this file  before this commit (long)
FILE_TOTAL_CHANGED_LINE_COUNT = 'file_total_changed_line_count'

#: Number of commits made by author on this file before this commit (long)
FILE_AUTHOR_COMMIT_COUNT = 'file_author_commit_count'

#: Number of lines changed by the author on this file before this commit (long)
FILE_AUTHOR_CHANGED_LINE_COUNT = 'file_author_changed_line_count'

#: Number of commits made by the committer on this file before this commit (long)
FILE_COMMITTER_COMMIT_COUNT = 'file_committer_commit_count'

#: Number of lines changed by the committer on this file before this commit (long)
FILE_COMMITTER_CHANGED_LINE_COUNT = 'file_committer_changed_line_count'


#
# Committed File Change Fields
#

#: The type of this change ('+' for additions, '-' for deletions)
CHANGE_TYPE = 'change_type'

#: The line number after this change for additions and before this change for deletions (long)
CHANGE_LINE_NUMBER = 'change_line_number'

#: The line number before this change (long)
CHANGE_PREVIOUS_LINE_NUMBER = 'change_previous_line_number'

#: The text of the added or deleted line in this change (string)
CHANGE_LINE = 'change_line'

#: The commit this line was introduced (string)
CHANGE_INTRODUCED_COMMIT = 'change_introduced_commit'


#
# Branch Fields
#

BRANCH_URI = 'branch_uri'
BRANCH_NAME = 'branch_name'
BRANCH_CREATED_TIME = 'branch_created_time'
BRANCH_COMMIT_URI = 'branch_commit_uri'
BRANCH_SOURCE_TYPE = 'branch_source_type'
BRANCH_COMMIT_SHA = 'branch_commit_sha'



#
# Pull Request Fields
#

PULL_URI = 'pull_uri'
PULL_ID = 'pull_id'
PULL_COMMIT_URIS = 'pull_commit_uris'
PULL_STATE = 'pull_state'
PULL_LOCKED = 'pull_locked'
PULL_TITLE = 'pull_title'
PULL_USER_URI = 'pull_user_uri'
PULL_CREATED_TIME = 'pull_created_time'
PULL_CLOSED_TIME = 'pull_closed_time'
PULL_UPDATED_TIME = 'pull_updated_time'
PULL_MERGED_TIME = 'pull_merged_time'
PULL_HEAD_ID = 'pull_head_id'
PULL_HEAD_URI = 'pull_head_uri'
PULL_HEAD_REPO_URI = 'pull_head_repo_uri'
PULL_BASE_ID = 'pull_base_id'
PULL_BASE_URI = 'pull_base_uri'
PULL_BASE_REPO_URI = 'pull_base_repo_uri'
PULL_MERGED = 'pull_merged'
PULL_MERGEABLE = 'pull_mergeable'
PULL_MERGEABLE_STATE = 'pull_mergeable_state'
PULL_COMMIT_COUNT = 'pull_commit_count'
PULL_ADDED_LINE_COUNT = 'pull_added_line_count'
PULL_DELETED_LINE_COUNT = 'pull_deleted_line_count'
PULL_CHANGED_FILE_COUNT = 'pull_changed_file_count'
PULL_SOURCE_TYPE = 'pull_source_type'

#
# Issue Fields
#

#: The URI of this issue (string)
ISSUE_URI = 'issue_uri'

#: The ID of the issue (string)
ISSUE_ID = 'issue_id'

#: The key of the issue. Used as tag in commit messages (string)
ISSUE_KEY = 'issue_key'

#: The project this issue was filed against (string)
ISSUE_PROJECT = 'issue_project'

#: The priority of this issue (string). Tags can be mapped to this field
ISSUE_PRIORITY = 'issue_priority'

#: The resolution of this issue (string). Tags can be mapped to this field
ISSUE_RESOLUTION = 'issue_resolution'

#: The severity of this issue (string). Tags can be mapped to this field
ISSUE_SEVERITY = 'issue_severity'

#: The status of this issue (string). Tags can be mapped to this field
ISSUE_STATUS = 'issue_status'

#: The time this issue was created (long, # of ms from the epoch time)
ISSUE_CREATED_TIME = 'issue_created_time'

#: The time this issue was updated (long, # of ms from the epoch time)
ISSUE_UPDATED_TIME = 'issue_updated_time'

#: The time this issue was closed (long, # of ms from the epoch time)
ISSUE_CLOSED_TIME = 'issue_closed_time'

#: The URI of this issue's creator (string)
ISSUE_CREATOR_URI = 'issue_creator_uri'

#: The URI of this issue's assignee (string)
ISSUE_ASSIGNEE_URI = 'issue_assignee_uri'

#: The URI of this issue's modifier (string)
ISSUE_MODIFIER_URI = 'issue_modifier_uri'

#: The URI of this issue's reporter (string)
ISSUE_REPORTER_URI = 'issue_reporter_uri'

#: The summary this issue (string)
ISSUE_SUMMARY = 'issue_summary'

#: The description this issue (string)
ISSUE_DESCRIPTION = 'issue_description'

#: The parent key of this issue (string)
ISSUE_PARENT_KEY = 'issue_parent_key'

#: The priority of this issue's parent (string)
ISSUE_PARENT_PRIORITY = 'issue_parent_priority'

#: The status of this issue's parent (string)
ISSUE_PARENT_STATUS = 'issue_parent_status'

#: The summary of this issue's parent (string)
ISSUE_PARENT_SUMMARY = 'issue_parent_summary'

#: The types of this issue's parent (string)
ISSUE_PARENT_TYPES = 'issue_parent_types'

#: The components this issue was filed against (array of string). Tags can be mapped to this field
ISSUE_COMPONENTS = 'issue_components'

#: The types of this issue (array of strings). Tags can be mapped to this field
ISSUE_TYPES = 'issue_types'

#: The resolved type of this issue (array of strings)
ISSUE_RESOLVED_TYPES = 'issue_resolved_types'

#: The versions this issue was filed against (array of string). Tags can be mapped to this field
ISSUE_VERSIONS = 'issue_versions'

#: The fix versions this issue was filed against (array of string). Tags can be mapped to this field
ISSUE_FIX_VERSIONS = 'issue_fix_versions'

#: The labels this issue was filed against (array of string)
ISSUE_TAGS = 'issue_tags'

#: The URI of the related Pull Request (when the issue is created for a pull request)
ISSUE_PULL_URI = 'issue_pull_uri'

#: The container's URI of the issue (string)
ISSUE_SOURCE_URI = 'issue_source_uri'

#: The container's type of the issue (string)
ISSUE_SOURCE_TYPE = 'issue_source_type'


# Experimental - RTC specific

ISSUE_DEFECT_TARGET = 'issue_defect_target'
ISSUE_FEATURE = 'issue_feature'
ISSUE_FILED_AGAINST = 'issue_filed_against'
ISSUE_FOUND_IN = 'issue_found_in'
ISSUE_PLANNED_FOR = 'issue_planned_for'
ISSUE_SUBFEATURE = 'issue_subfeature'


#
# User Fields
#

#: The URI of this user (string)
USER_URI = 'user_uri'

#: The login name of this user (string)
USER_LOGIN = 'user_login'

#: The id of this user (string)
USER_ID = 'user_id'

#: The key of this user (string, auto-generated)
USER_KEY = 'user_key'

#: The email address of this user (string)
USER_EMAIL = 'user_email'

#: The name of this user (string)
USER_NAME = 'user_name'

#: The type of this user (string)
USER_TYPE = 'user_type'

#: The company of this user (string)
USER_COMPANY = 'user_company'

#: The number of public repos accessed by this user (long)
USER_PUBLIC_REPO_COUNT = 'user_public_repo_count'

#: The number of public gists accessed by this user (long)
USER_PUBLIC_GIST_COUNT = 'user_public_gist_count'

#: The number of users following this user (long)
USER_FOLLOWER_COUNT = 'user_follower_count'

#: The number of users that this user is following (long)
USER_FOLLOWING_COUNT = 'user_following_count'

#: The time this user record was created (long, # of ms from the epoch time)
USER_CREATED_TIME = 'user_created_time'

#: The last time this user record was updated (long, # of ms from the epoch time)
USER_UPDATED_TIME = 'user_updated_time'

#: The container's URI of the user (string)
USER_SOURCE_URI = 'user_source_uri'

#: The container's type of the user (string)
USER_SOURCE_TYPE = 'user_source_type'

#: the container's type of the user (string)
USER_NUMBER_COMMITS = 'user_number_commits'

#: the container's type of the user (string)
USER_NUMBER_ADDITIONS = 'user_number_additions'

#: the container's type of the user (string)
USER_NUMBER_DELETIONS = 'user_number_deletions'


#
# Repo Comments
#

#: URI of the comment (string)
COMMENT_URI = 'comment_uri'

#: URI of the commit (string)
COMMENT_COMMIT_URI = 'comment_commit_uri'

#: Content of the comment (string)
COMMENT_BODY = 'comment_body'

#: Comment created time (long, # of ms from the epoch time)
COMMENT_CREATED_TIME = 'comment_created_time'

#: Comment updated time (long, # of ms from the epoch time)
COMMENT_UPDATED_TIME = 'comment_updated_time'

#: Author of the comment (string)
COMMENT_AUTHOR_URI = 'comment_user_uri'

#: Position of change mentioned in the commit comment(string)
COMMENT_POSITION = 'comment_position'

#: Line of change mentioned in the commit comment(string)
COMMENT_LINE = 'comment_line'

#: File Path of change mentioned in the commit comment(string)
COMMENT_PATH = 'comment_path'


#
# Issue Comments
#

#: URI of the comment (string)
ISSUE_COMMENT_URI = 'issue_comment_uri'

ISSUE_COMMENT_ISSUE_URI = 'issue_comment_issue_uri'

#: Content of the comment (string)
ISSUE_COMMENT_BODY = 'issue_comment_body'

#: Author of the comment (string)
ISSUE_COMMENT_AUTHOR_URI = 'issue_comment_author_uri'

#: Comment created time (long, # of ms from the epoch time)
ISSUE_COMMENT_CREATED_TIME = 'issue_comment_created_time'

#: Comment created time (long, # of ms from the epoch time)
ISSUE_COMMENT_UPDATED_TIME = 'issue_comment_updated_time'


#
# Issue Events
#

#: The URI of the event (string)
ISSUE_EVENT_URI = 'issue_event_uri'

#: The URI of the issue. Used as tag in commit messages (string)
ISSUE_EVENT_ISSUE_URI = 'issue_event_issue_uri'

#: Issue Event Type (string)
ISSUE_EVENT_TYPE = 'issue_event_type'

#: Issue event state (string)
ISSUE_EVENT_ISSUE_STATE  = 'issue_event_issue_state'

#: Issue Event commit Uri (string)
ISSUE_EVENT_COMMIT_URI = 'issue_event_commit_uri'

#: Issue Event create time (long, # of ms from the epoch time)
ISSUE_EVENT_CREATED_TIME = 'issue_event_created_time'


#
# Build Fields
#

#: The ID of the build, usually a number (string)
BUILD_ID = 'build_id'

#: Build Uri (string)
BUILD_URI = 'build_uri'

#: Uri of Repo associated to this build (string)
BUILD_REPO_URI = 'build_repo_uri'

#: Build status, whether it's running or not (true or false)
BUILD_STATUS = 'build_status'

#: Build result, whether failed or passed (string)
BUILD_RESULT = 'build_result'

#: Build duration (long)
BUILD_DURATION = 'build_duration'

#: Build estimated duration (long)
BUILD_ESTIMATED_DURATION = 'build_estimated_duration'

#: Build create time (long, # of ms from the epoch time)
BUILD_TIMESTAMP = 'build_timestamp'

#: Build full display name (string)
BUILD_NAME = 'build_name'

#: Build source code manager (git, svn, etc) (string)
BUILD_SCM = 'build_scm'

#: Build was built in this machine (string)
BUILD_BUILT_ON = 'build_built_on'

#: Build queue ID number (int)
BUILD_QUEUE_ID = 'build_queue_id'

#: List of users who committed changes to the build (list of dics [uri, name])
BUILD_CULPRITS = 'build_culprits'

#: Build short description (string)
BUILD_SHORT_DESCRIPTION  = 'build_short_description'

#: Commit id that triggered the build, might be found in many different places in the build page (string)
BUILD_COMMIT_ID = 'build_commit_id'

#: Author of the commit that triggered the build (string)
BUILD_COMMIT_AUTHOR_NAME = 'build_commit_author'

#: Email of the author (string)
BUILD_COMMIT_AUTHOR_EMAIL = 'build_commit_author_email'

#: Link of the pull request that triggered the commit (string)
BUILD_PULL_URI = 'build_pull_uri'

#: Branch of the git commit (string)
BUILD_GIT_BRANCH = 'build_git_branch'

#: Uri of the git (string)
BUILD_GIT_URI = 'build_git_uri'

#: Number of tests that failed (int)
BUILD_TESTS_FAILED_COUNT = 'build_tests_failed_count'

#: Number of tests that were skipped (int)
BUILD_TESTS_SKIPPED_COUNT = 'build_tests_skipped_count'

#: Total number of tests (int)
BUILD_TESTS_TOTAL_COUNT = 'build_tests_total_count'

#: Number of tests that passed (int)
BUILD_TESTS_PASSED_COUNT = 'build_tests_passed_count'

#: Name of the link to the test report (string)
BUILD_TESTS_URI_NAME = 'build_tests_uri_name'

#: Json containing the tests for this build (json)
BUILD_TESTS = 'build_tests'

#: Build source type
BUILD_SOURCE_TYPE = 'build_source_type'


#
# Build Tests Fields
#

#: Whether the build has children builds (True or False)
TESTS_URI = 'tests_uri'

#: Whether the build has children builds (True or False)
TESTS_BUILD_URI = 'tests_build_uri'

#: Whether the build has children builds (True or False)
TESTS_HAS_CHILDREN = 'tests_has_children'

#: The children of the build (list of children, with each containing a dict of all of the below)
TESTS_CHILDREN = 'tests_children'

#: Build Number of the father (int)
TESTS_CHILD_BUILD_NUMBER = 'tests_child_build_number'

#: Build Uri of the child (string)
TESTS_CHILD_BUILD_URI = 'tests_child_build_uri'

#: Total Duration of all the tests (int)
TESTS_TOTAL_DURATION = 'tests_total_duration'

#: If the test suite is empty (True or Flase)
TESTS_IS_EMPTY = 'tests_is_empty'

#: Total Count of skipped tests (int)
TESTS_SKIPPED_COUNT = 'tests_skipped_count'

#: Total Count of all the tests in all suites (int)
TESTS_TOTAL_COUNT = 'tests_total_count'

#: Total Count of failed tests (int)
TESTS_FAILED_COUNT = 'tests_failed_count'

#: Total Count of passed tests (int)
TESTS_PASSED_COUNT = 'tests_passed_count'


#
# Build Single Test Fields
#

#: Age of the test (int)
TEST_AGE = 'test_age'

#: The name of the class for the tests (string)
TEST_CLASS_NAME = 'test_class_name'

#: Duration of the test (long)
TEST_DURATION = 'test_duration'

#: How many times the test failed before (int)
TEST_FAILED_SINCE = 'test_failed_since'

#: Name of the test (string)
TEST_NAME = 'test_name'

#: If the test was skipped or executed (True or False)
TEST_SKIPPED = 'test_skipped'

#: Status of the test (string)
TEST_STATUS = 'test_status'

#: Combined list of all tests in all suites (list of dicts of tests)
ALL_TESTS = 'all_tests'


#
# Repo Mappings to Index Fields
#

# Git URI
GIT_URI = 'git_uri'

# Index Name
INDEX_NAME = 'index'
