import sys
import functools
import time
import json
import os
import traceback
import random

from doi import common as cmn
from doi.scheduler import common
from doi.jobs.miner import stream_miner
from doi.jobs.analyzer import analyzer
from doi import constants
from doi.util.mb import mb_util
from doi.util import time_util, log_util, astro_util, file_util
from doi.jobs.miner.resourcedb import ResourceDB, ResourceDBBulkInserter


logger = log_util.get_logger("astro.jobs.miner.stream")


class StreamMiner(object):

    def __init__(self, hash_lo, hash_hi, stop_event):

        self.partitioner = functools.partial(
            self.hash_partitioner,
            lo=hash_lo,
            hi=hash_hi
        )

        self.stop_event = stop_event

        self.ac = astro_util.AstroClient()

    def run(self):

        projects = []

        while True:

            old_projects = projects

            projects = self.get_partitioned_projects()

            logger.info("Number of projects in this partition {}".format(
                len(projects)))

            remove_projects = self.get_removed_projects(projects, old_projects)

            self.clean_projects(remove_projects)

            random.shuffle(projects)

            for p in projects:

                try:
                    self.handle_project_mine(p)
                except Exception as e:
                    logger.error("Streaming job failed: {}".format(
                        p['full_name']))
                    logger.error(
                        ''.join(traceback.format_exception(*sys.exc_info())))
                    logger.error("error: {}".format(e))

                if self.stop_event.is_set():
                    raise cmn.StoppedException

            time.sleep(60)

            logger.info(
                "Refereshing streaming projects previous length: {}".format(
                    len(projects)))

            if self.stop_event.is_set():
                raise cmn.StoppedException

    def handle_project_mine(self, p):
        # Logic:
        logger.info("Handling streaming job for {}".format(p['full_name']))

        commit, committed_file = self.commit_mine(p)

        issues = self.issue_mine(p)

        self.handle_write(p, issues, 'issue')

        user = self.contributor_mine(p)

        self.handle_write(p, user, 'user')

        joined_commit, joined_committed_file = self.handle_join(
            p, commit, committed_file)

        self.handle_write(p, joined_commit, 'commit')
        self.handle_write(p, joined_committed_file, 'committed_file')

        if (joined_committed_file is not None and
                len(joined_committed_file) > 0):
            logger.info(
                "Streaming {} produced {} commit files send rudi task".format(
                    p['full_name'], len(joined_committed_file)))
            self.send_rudi_job(p)
        else:
            logger.info("No new commit files for {}".format(p['full_name']))

    def commit_mine(self, p):

        def extend(all_records, records):
            all_commit, all_committed_file = all_records[0], all_records[1]
            commit, committed_file = records[0], records[1]

            if commit is not None:
                all_commit.extend(commit)

            if committed_file is not None:
                all_committed_file.extend(committed_file)

        def returner(all_records):
            all_commit, all_committed_file = all_records[0], all_records[1]

            if len(all_commit) == 0:
                return None, None
            elif len(all_committed_file) == 0:
                return all_commit, None
            else:
                return all_commit, all_committed_file

        return self._base_data_mine(
            p,
            lambda: ([], []),
            extend,
            returner,
            stream_miner.commit_type_to_miner_class
        )

    def issue_mine(self, p):
        return self.data_mine(p, stream_miner.issue_type_to_miner_class)

    def contributor_mine(self, p):
        return self.data_mine(p, stream_miner.contributor_type_to_miner_class)

    def data_mine(self, p, mapper):

        return self._base_data_mine(
            p,
            lambda: [],
            lambda all_records, records:
                all_records.extend(records) if records is not None else None,
            lambda all_records: None if len(all_records) == 0 else all_records,
            mapper
        )

    def _base_data_mine(self, p, init, extend, returner, mapper):

        all_records = init()

        for source_type in set(
                source['source_type'] for source
                in p['input']['sources']):

            if source_type not in mapper:
                continue

            records = mapper[source_type](p).stream_data()

            extend(all_records, records)

        return returner(all_records)

    def handle_join(self, p, commit, committed_file):

        if commit is None:
            logger.info("No streaming commits to join")
            return None, None

        j = stream_miner.Joiner(p)

        if committed_file is None:
            logger.info("No streaming committed files to join")
            return j.join_data(commit, [])

        return j.join_data(commit, committed_file)

    def handle_write(self, p, docs, doc_type):

        if docs is None:
            return None

        with ResourceDB(p['full_name']) as resource_db:
            with ResourceDBBulkInserter(
                    resource_db,
                    doc_type) as writer:

                for d in docs:
                    writer.write(d)

    def send_rudi_job(self, p):

        mb_config = json.loads(
            os.environ[constants.ASTRO_MB_ENV_VAR])

        mb = mb_util.MessageBroker(mb_config)

        mb_manager = mb.new_manager()

        try:
            # logger.info("Ensuring topic '%s' exists",
            #             analyzer.Analyzer.REQUESTS_TOPIC_NAME)
            mb_manager.ensure_topic_exists(
                analyzer.Analyzer.REQUESTS_TOPIC_NAME)
        finally:
            mb_manager.close()

        task_id = 'streaming_analyze_task'

        timestamp = time_util.get_current_utc_millis()

        msg = {
            "task_msg": {
                "timestamp": timestamp,
                "type": "execute_task",
                "msg_id": "/".join([
                        p['current_method'],
                        p['id'],
                        task_id,
                        str(timestamp)]),
                "job_id": p['id'],
                "job_name": p['full_name'],
                "task_id": task_id,
                "method": p['current_method'],
                "rev": p['current_rev'],
                "input": p['input'],
                "previous_input": p['previous_input']
            }
        }

        logger.info(
            "Sending 'execute_task' message to rudi's requests topic: "
            "job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d",
            p['id'], p['name'], task_id, p['current_method'], p['current_rev'])

        mb_producer = mb.new_producer(self.create_temporal_id)
        mb_producer.send(analyzer.Analyzer.REQUESTS_TOPIC_NAME, msg)

    def get_partitioned_projects(self):

        ps = []

        for p in self.ac.get_projects():
            if self.partitioner(p['name']) and \
                self._is_batch_finished(p) and \
                p.get('mine_future_data',True):

                payload = self.ac.get_project(p['name'])
                if payload is not None:
                    payload.pop('tasks')
                    ps.append(payload)
        return ps

    @staticmethod
    def get_removed_projects(new_projects, old_projects):

        new_project_set = set([p['name'] for p in new_projects])

        return [p for p in old_projects if p['name'] not in new_project_set]

    def clean_projects(self, projects):

        logger.info("Removing {} streaming projects: {}".format(
            len(projects),
            [p['name'] for p in projects]))

        for p in projects:

            sources = p['input']['sources']
            job_name = p['name']

            for source in sources:

                work_dir = os.environ[constants.ASTRO_OUT_DIR_ENV_VAR]
                project_work_dir = os.path.join(work_dir, job_name)
                stream_in_path = os.path.join(project_work_dir, "stream/in")

                file_util.remove_dir_tree(stream_in_path)

    @staticmethod
    def _is_batch_finished(p):

        methods = ['create', 'update']

        return (p['completed'] and
                p['current_method'] in methods and
                p['desired_method'] in methods and
                p['current_state'] == 'successful')

    @staticmethod
    def hash_partitioner(item, hi, lo):

        h = hash(item)

        return h >= lo and h < hi

    @staticmethod
    def create_partitions(num_partitions):

        hi = sys.maxint
        lo = -sys.maxint - 1

        slices = [
            int(lo + x * (hi - lo) / num_partitions)
            for x in range(num_partitions + 1)
        ]

        partitions = []

        for i in range(len(slices) - 1):
            partitions.append((slices[i], slices[i + 1]))

        return partitions

    @staticmethod
    def get_partitioners(num_partitions):

        for partition in StreamMiner.create_partitions(num_partitions):
            yield functools.partial(
                StreamMiner.hash_partitioner,
                lo=partition[0],
                hi=partition[1])

    @staticmethod
    def create_job_payload(num_workers=None):

        if num_workers is None:
            num_partitions = 2
        else:
            if isinstance(num_workers, basestring):
                num_workers = int(num_workers)

            num_partitions = max([
                2,
                num_workers
            ])

        id_and_name = StreamMiner.create_temporal_id()

        d = {
            'job_msg': {
                'type': common.CreateJobMsg,
                'job': {
                    'id': id_and_name,
                    'tasks': [],
                    'name': id_and_name,
                    'current_rev': 1,
                    'desired_rev': 1,
                    'title': id_and_name
                },
                'timestamp': int(round(time.time()) * 1000)
            }
        }

        for partition in StreamMiner.create_partitions(num_partitions):
            d['job_msg']['job']['tasks'].append(
                {
                    "function": "doi.jobs.miner.stream.stream_mine",
                    "id": str(partition[0]) + "_" + str(partition[1])
                }
            )

        return common.CreateJobMsg(
            common.Job.decode(d['job_msg']['job'])).encode()

    @staticmethod
    def create_temporal_id():

        import time
        return "__astro_stream_job_" + time.strftime(
            "%Y_%m_%d_%H_%M_%S",
            time.gmtime())

    @staticmethod
    def delete_job_payload():

        ac = astro_util.AstroClient()
        for p in ac.get_projects():
            if p['id'].startswith('__astro_stream_job_'):
                if p['desired_method'] == 'delete':
                    yield p['id']

                yield common.DeleteJobMsg(p['id'], force=True)


def stream_mine(
    params,
    progress_callback,
    stop_event
):

    job_id = params.task_id
    hash_lo, hash_hi = [int(value) for value in job_id.split('_')]

    method = params.method

    if method in [cmn.TaskMethod.CREATE, cmn.TaskMethod.UPDATE]:
        sm = StreamMiner(hash_lo, hash_hi, stop_event)
        sm.run()
