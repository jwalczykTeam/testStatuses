from doi.util import log_util
import git
import pygit2
from git import exc
import fields as f
import pprint
import itertools

logger = log_util.get_logger("astro.jobs.miner.gitblame")


def set_change_introduced_commit(git_dir, committed_file):
    g = git.Git(git_dir)

    try:
        blame_doc = g.execute([
            'git',
            'blame',
            '-l',
            '--root',
            committed_file[f.COMMIT_ID] + "^",
            '--',
            committed_file[f.FILE_PATH],
        ], as_process=True)

    except Exception, e:
        logger.warning(
            "initial git blame failed for commit: {}".format(str(e)))
        try:
            blame_doc = g.execute([
                'git',
                'blame',
                '-l',
                '-root',
                committed_file[f.COMMIT_ID] + "^",
                '--',
                committed_file[f.FILE_PREVIOUS_PATH],
            ], as_process=True)
        except Exception, e:
            logger.warning("rename git blame failed: {}".format(str(e)))
            return

    blame_count = 0
    for change in committed_file[f.FILE_CHANGES]:
        if change['change_type'] == '-':
            change_line_index = change[f.CHANGE_PREVIOUS_LINE_NUMBER] - 1
            for line in blame_doc.stdout:
                if blame_count == change_line_index:
                    matched_commit = line.split(" ")[0]
                    if matched_commit[0] == '^':  # boundary commit
                        matched_commit = matched_commit[1:]

                    change[f.CHANGE_INTRODUCED_COMMIT] = matched_commit
                    blame_count += 1
                    break
                else:
                    blame_count += 1


class PyGitUtil(pygit2.Repository):

    def cache_file_mapping(self):
        """
        Initializes cached file mapping storing file_path oid
        introduced commit, use with update method
        """
        self.mapping = {}  # file_path => oid => first commit

    def update(self, cf):
        """
        cf  dictionary representing committed_file type

        updates cached file mapping
        """
        tree = self[cf['commit_id']].tree

        if cf['file_path'] in tree:
            if cf['file_path'] not in self.mapping:
                self.mapping[cf['file_path']] = {}
            oid = tree[cf['file_path']].oid.__str__()

            if oid in self.mapping[cf['file_path']]:
                return None
            else:
                self.mapping[cf['file_path']][oid] = cf['commit_id']

    def get_parent(self, file_path, c_id):
        """
        file_path   file path of file in repo
        c_id        commit id

        returns the parent commit id of file w.r.t. file path
        depends on cache_file_mapping and update
        """
        parents = self[c_id].parents
        if len(parents) == 0:
            return c_id

        tree = parents[0].tree
        if file_path not in tree:
            return c_id

        oid = tree[file_path].oid.__str__()

        return self.mapping[file_path].get(oid, c_id)

    def get_parents(self, file_path, c_id):
        """
        file_path   file path of file in repo
        c_id        commit id

        returns get_parent as list for merge commits
        """
        return [self.get_parent(file_path, c_id)]


class GitUtil(git.Git):

    def root_commit(self):
        """
        Returns /dev/null commit
        """
        return self.execute(
            ['git', 'hash-object', '-t', 'tree', '/dev/null']).strip()

    def get_parent(self, file_path, commit_id):
        """
        file_path   file path of file in repo
        c_id        commit id

        return parent w.r.t. file_path by executing git command
        """

        process = self.execute([
            'git',
            'log',
            '--follow',
            '--pretty=format:%H',
            commit_id,
            '--',
            file_path], as_process=True)

        for line in process.stdout:
            line = line.split('\n')[0]
            if line == commit_id:
                continue
            else:
                return line

    def get_file_content(self, file_path, commit_id):
        """
        file_path   file path of file in repo
        c_id        commit id

        return file contents of file_path at commit_id using git command
        """
        splitter = "\n"
        return [line + splitter for line in self.execute([
                'git',
                'show',
                commit_id + ":" + file_path
                ]).split(splitter)]


# Line tuple definition (change_line, commit_id, count)
# TODO: removed change_line when enough testing ensures correct blame
# but add ability to to turn back on


class LineTracker(object):

    def __init__(self, committed_file, with_line=False, git_util=None):
        """
        committed_file  added committed_file dict or Linetracker object
        with_line       flag to indicate whether to store line string
                        use for debugging or to extend feature set
        git_util        instance of GitUtil pointing to repo

        pass in git_util only if you want to debug and test against
        actual files
        """

        if type(committed_file) is LineTracker:
            # inherit from previous LineTracker
            self.with_line = committed_file.with_line
            self._set_add_line()
            self.file_lines = committed_file.file_lines
            self.git_util = committed_file.git_util
        else:
            self.with_line = with_line
            self._set_add_line()
            self.file_lines = self._first_commit(committed_file)
            self.git_util = git_util

    def _set_add_line(self):
        if self.with_line:
            self._add_line = self._add_line_with_line
        else:
            self._add_line = self._add_line_without_line

    def _to_list(self, index):
        """
        helper method to see current state of LineTracker file_lines
        for debugging

        index   index of tuple to reduce
        """
        return [line[index] for line in self.file_lines]

    def get_file(self):
        """
        Return representation of file contents
        """
        return self._to_list(0) if self.with_line else None

    def get_counts(self):
        """
        Return representation of counts at each line
        """
        return self._to_list(-1)

    def _first_commit(self, committed_file):
        """
        committed_file  dict of committed_file type
        private helper method for constructor to initialize file lines
        """

        # return [(line['change_line'], committed_file['commit_id'], 0)
        #         for line in committed_file['file_changes']]

        return [self._add_line(line, committed_file['commit_id'], 0)
                for line in committed_file['file_changes']]

    def update_blame(self, committed_file):
        """
        committed_file  dict of committed_file type

        given committed_file blame the minus diffs
        NOTE: this mutates the given committed_file
        """

        for line in committed_file['file_changes']:
            if line['change_type'] == '-':
                line['change_introduced_commit'] = self.file_lines[
                    line['change_previous_line_number'] - 1][-2]

    def _apply_minus_diff(self, minus_diffs, committed_file):
        """
        minus_diffs     only minus diff of committed_file
        committed_file  dict of committed_file type

        applies the minus diff to file lines
        retuns removed items with counts
        """

        removed = {}

        new_file_lines = []
        pointer = 0

        for minus_diff in minus_diffs:
            line_number = minus_diff['change_previous_line_number'] - 1
            new_file_lines.extend(self.file_lines[pointer:line_number])
            removed[line_number] = self.file_lines[line_number]

            if self.git_util is not None:
                # to test
                if (removed[line_number][0] != minus_diff['change_line']):
                    logger.warning(committed_file['sequence_no'])
                    logger.warning(committed_file['commit_message'])
                    logger.warning(committed_file['commit_id'])
                    logger.warning(committed_file['file_path'])
                    logger.warning(committed_file['file_key'])
                    logger.warning(minus_diff)
                    logger.warning(removed[line_number])
                    logger.warning("\n")
            pointer = line_number + 1

        if pointer < len(self.file_lines):
            new_file_lines.extend(self.file_lines[pointer:])
        self.file_lines = new_file_lines

        return removed

    def _apply_plus_diff(self, plus_diffs, committed_file, removed):
        """
        plus_diffs      only plus diffs of committed_file
        committed_file  dict of committed_file type
        removed         dicionary of change_line_number => tuple

        applies the plus diff to file_lines
        """

        new_lines = []
        pointer = 0
        lines_added_count = 0

        while (pointer < len(self.file_lines) or
               lines_added_count < len(plus_diffs)):

            if (lines_added_count < len(plus_diffs) and
                plus_diffs[lines_added_count]['change_line_number'] ==
                    pointer + lines_added_count + 1):

                changed_line_number = plus_diffs[lines_added_count][
                    'change_line_number'] - lines_added_count

                # new_lines.append((
                #     plus_diffs[lines_added_count][
                #         'change_line'],
                #     committed_file['commit_id'],
                #     0 if changed_line_number not in removed else
                #     removed[changed_line_number][2] + 1
                # ))

                new_lines.append(self._add_line(
                    plus_diffs[lines_added_count],
                    committed_file['commit_id'],
                    0 if changed_line_number not in removed else
                    removed[changed_line_number][-1] + 1
                ))

                lines_added_count += 1
            else:

                new_lines.append(self.file_lines[pointer])
                pointer += 1

        self.file_lines = new_lines

    def _add_line_with_line(self, plus_diff, commit_id, line_count):
        return (plus_diff['change_line'], commit_id, line_count)

    def _add_line_without_line(self, plus_diff, commit_id, line_count):
        return (commit_id, line_count)

    def _test_against_git(self, committed_file):
        """
        committed_file  dict of committed_file type

        compares file contents in git to the LineTracker state
        """

        if committed_file['file_action'] == 'removed':
            return None

        try:

            file_content = self.git_util.get_file_content(
                committed_file['file_path'],
                committed_file['commit_id']
            )

        except exc.GitCommandError:
            # bad object, usually a git submodule
            return None

        tracker_file_content = self.get_file()

        c = committed_file
        for a, b in zip(file_content, tracker_file_content):
            if a != b:
                if a[:-1] == b:
                    continue

                if a == b[:-1]:
                    continue

                logger.warning((c['commit_id'], c['file_path']))
                logger.warning("tracker content not equal")
                logger.warning("Real len: {}, Tracked Len: {}".format(
                    len(file_content),
                    len(tracker_file_content)))

                unequal_lines = [(n, t) for n, t in enumerate(
                    zip(file_content, tracker_file_content)) if t[0] != t[1]]
                pprint.pprint(unequal_lines)
                break

    def update_lines(self, committed_file):
        '''
        committed_file  dict of committed_file type

        metho to update the lines with diff in a given committed_file
        '''

        def split_diff(file_changes):

            minus_diffs = []
            plus_diffs = []
            for line in file_changes:
                if line['change_type'] == '-':
                    minus_diffs.append(line)
                else:
                    plus_diffs.append(line)

            return minus_diffs, plus_diffs

        minus_diffs, plus_diffs = split_diff(committed_file['file_changes'])

        removed = self._apply_minus_diff(minus_diffs, committed_file)
        self._apply_plus_diff(plus_diffs, committed_file, removed)

        if self.git_util is not None:
            self._test_against_git(committed_file)


class DiffBlamer(object):

    def __init__(self, pygit_util, git_util=None):
        """
        pygit_util      instance of git_util.PyGitUtil
        git_util        pass in if you want to debug and check
        """
        self.mapping = {}  # file_key => commit_id => LineTracker
        self.path_to_key = {}  # file_path => file_key
        self.pygit_util = pygit_util
        self.git_util = git_util

        self.pygit_util.cache_file_mapping()

        # used for sorting committed files
        self.file_action_priority = {
            'removed': 0,
            'copied': 1,
            'renamed': 2,
            'modified': 3,
            'added': 4
        }

    def committed_file_order_comp(self, d):
        """
        d   committed_file dict

        Method to determine sorting order of committed files
        """
        if (d['file_previous_path'] is not None and
                d['file_path'] == d['file_previous_path']):
            return 1
        else:
            return 0

    def find_previous_parent(self, file_path, commit_id):
        """
        file_path   file path
        commit_id   hash

        Finds the parent commit of committed_file
        """

        return self.pygit_util.get_parent(file_path, commit_id)

    def iterate_in_order(self, committed_file):
        """
        committed_file list of committed_file

        Method to iterate through groups of committed file
        committed_file list of committed_file
        """
        committed_file.sort(key=lambda d: d['sequence_no'])

        for _, cs in itertools.groupby(
                committed_file, key=lambda d: d['sequence_no']):

            cs = list(cs)
            cs.sort(
                key=lambda d:
                (d['sequence_no'],
                 self.file_action_priority[d['file_action']],
                 self.committed_file_order_comp(d)))
            yield cs

    def handle_copied(self, c):
        """
        c   committed_file dict

        handle file action copied of committed_file
        """

        file_path = c['file_path']
        commit_id = c['commit_id']
        previous_path = c['file_previous_path']
        parent_id = self.find_previous_parent(previous_path, commit_id)

        if file_path in self.mapping:
            self.mapping[file_path][commit_id] = \
                LineTracker(self.mapping[previous_path][parent_id])
        else:
            self.mapping[file_path] = {
                commit_id:
                LineTracker(self.mapping[previous_path][parent_id])
            }

        self.mapping[file_path][commit_id].update_blame(c)
        self.mapping[file_path][commit_id].update_lines(c)

    def handle_renamed(self, c):
        """
        c   committed_file dict

        handle file action renamed of committed_file
        """
        file_path = c['file_path']
        commit_id = c['commit_id']
        previous_path = c['file_previous_path']
        parent_id = self.find_previous_parent(previous_path, commit_id)


        if file_path in self.mapping:
            self.mapping[file_path][commit_id] = LineTracker(
                self.mapping[previous_path][parent_id])
        else:
            self.mapping[file_path] = {
                commit_id: LineTracker(
                    self.mapping[previous_path][parent_id])
            }

        self.mapping[file_path][commit_id].update_blame(c)
        self.mapping[file_path][commit_id].update_lines(c)

    def handle_modified(self, c):
        """
        c   committed_file dict

        handle file action modified of committed_file
        """

        file_path = c['file_path']
        commit_id = c['commit_id']

        parent_id = self.find_previous_parent(file_path, commit_id)
        previous_path = file_path

        self.mapping[file_path][commit_id] = LineTracker(self.mapping[
            previous_path][parent_id])
        self.mapping[file_path][commit_id].update_blame(c)
        self.mapping[file_path][commit_id].update_lines(c)

    def handle_removed(self, c):
        """
        c   committed_file dict

        handle file action removed of committed_file
        """

        file_path = c['file_path']
        commit_id = c['commit_id']
        parent_id = self.find_previous_parent(file_path, commit_id)

        self.mapping[file_path][commit_id] = LineTracker(
            self.mapping[file_path][parent_id])

        self.mapping[file_path][commit_id].update_blame(c)
        self.mapping[file_path][commit_id].update_lines(c)

    def handle_created(self, c):
        """
        c   committed_file dict

        handle file action created of committed_file
        """
        file_path = c['file_path']
        commit_id = c['commit_id']

        if file_path in self.mapping:
            self.mapping[file_path][commit_id] = LineTracker(c, self.git_util)
        else:
            self.mapping[file_path] = {
                commit_id: LineTracker(c, self.git_util)
            }

    def handle_merge(self, c):
        """
        c   committed_file dict

        handle file action merge of committed_file
        """

        file_path = c['file_path']
        commit_id = c['commit_id']
        file_previous_path = c['file_previous_path']
        file_action = c['file_action']

        try:

            if file_action != 'modified':  # only need to find a single parent

                if file_action == 'added':
                    parent_id = self.find_previous_parent(file_path, commit_id)
                elif file_action == 'removed':
                    parent_id = self.pygit_util.get_parents(
                        file_path, commit_id)[0]
                else:
                    # copied or renamed
                    parent_id = self.pygit_util.get_parents(
                        file_previous_path, commit_id)[0]

                if file_action == 'added' and parent_id == commit_id:
                    # file added and couldn't find parent in merge
                    self.handle_created(c)
                    return None

                if (file_path in self.mapping and
                        parent_id in self.mapping[file_path] and
                        file_action not in ['copied', 'renamed']):
                    # file has direct parent
                    self.mapping[file_path][commit_id] = LineTracker(
                        self.mapping[file_path][parent_id])
                elif file_path in self.mapping:
                    # file path already has exists but not for this parent
                    self.mapping[file_path][commit_id] = LineTracker(
                        self.mapping[file_previous_path][parent_id])
                else:
                    # no file path in mapping
                    self.mapping[file_path] = {
                        commit_id: LineTracker(
                            self.mapping[file_previous_path][parent_id])
                    }
            else:
                # multiple parents
                # TODO: support more than 2 parents

                parent_ids = self.pygit_util.get_parents(
                    file_path, commit_id)
                self.mapping[file_path][commit_id] = LineTracker(
                    self.mapping[file_path][parent_ids[0]])

                # TODO: add support for blaming to parent commit id
                # right now blames to merge commit - same behaviour as git
                # blame

            self.mapping[file_path][commit_id].update_blame(c)
            self.mapping[file_path][commit_id].update_lines(c)
        except BaseException:
            logger.error("Merge failed: {} {} {} ".format(
                file_path, commit_id, file_action))
            raise

    def run(self, committed_file):
        """
        committed_file list of committed_file
        """

        for cs in self.iterate_in_order(committed_file):

            # logger.info(set([_['sequence_no'] for _ in cs]))
            # logger.info(set([_['commit_id'] for _ in cs]))

            for c in cs:

                commit_id = c['commit_id']
                file_path = c['file_path']
                file_action = c['file_action']

                if len(self.pygit_util[commit_id].parents) > 1:
                    self.handle_merge(c)
                else:

                    try:
                        if file_action == 'copied':
                            self.handle_copied(c)
                        elif file_action == 'renamed':
                            self.handle_renamed(c)
                        elif file_action == 'modified':
                            self.handle_modified(c)
                        elif file_action == 'removed':
                            self.handle_removed(c)
                        else:
                            self.handle_created(c)
                    except BaseException:
                        logger.error("Failed at commit (non-merge)")
                        keys = set(['commit_id', 'file_path',
                                    'file_action', 'sequence_no', 'file_key',
                                    'file_previous_path', 'commit_message'])
                        pprint.pprint({key: c[key] for key in keys})
                        if file_path in self.mapping:
                            pprint.pprint(self.mapping[file_path])
                        else:
                            logger.error("File key not in mapping")
                        raise

                self.pygit_util.update(c)
