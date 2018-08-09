'''
Created on Aug 27, 2016

@author: alberto
'''


import json
import os
import uuid
from doi import constants
from doi.common import MemberState
from doi.scheduler.common import Job, Scheduler, Worker
from doi.scheduler.common import HeartbeatMsg
from doi.util import log_util, time_util
from doi.util.db import model


logger = log_util.get_logger("astro.scheduler.db")


class JobDatabase(object):
    def __init__(self, db, group_id):
        super(JobDatabase, self).__init__()

        self.db = db
        self.name = constants.get_scheduler_db_name(group_id)

        logger.debug("Initializing job database: name='%s' ...", self.name)
        if not self.db.database_exists(self.name):
            model_path = os.path.dirname(model.__file__)
            db_config_path = os.path.join(model_path, "mapping.json")
            with open(db_config_path, 'r') as f:
                db_config = json.load(f)
            self.db.create_database(self.name, db_config)

        logger.debug("Job database initialized.")

    def close(self):
        logger.debug("Closing job database: name='%s' ...", self.name)
        self.db.close()
        logger.debug("Job database closed.")

#
#   Job Operations
#
    def get_job(self, job_id):
        resource = self.db.get_resource(self.name,
                                        constants.JOB_TYPE,
                                        job_id)
        return Job.decode(resource) if resource is not None else None

    def list_jobs(self, key_values=None, size=None):
        #logger.info("database.py list_jobs: name='%s', job_type='%s', key_values='%s', size='%s' ...",
        #            self.name, constants.JOB_TYPE, key_values, size)
        resources = self.db.list_resources(self.name,
                                           constants.JOB_TYPE,
                                           key_values,
                                           size)
        return [Job.decode(resource) for resource in resources]

    def list_metric_jobs(self, metrics_index=None, size=None, doc_type=None, sort_key=None, sort_order=None):
        resources = self.db.list_metric_resources(metrics_index,
                                                  size, doc_type, sort_key=sort_key, sort_order=sort_order)
        return resources

    def create_job(self, job):
        new_job = self.db.create_resource(
            self.name,
            constants.JOB_TYPE,
            job.encode(),
            job.id,
            job.created_at)
        self.db.flush(self.name)

        job.id = new_job["id"]
        job.created_at = new_job["created_at"]
        return job

    def update_job(self, job):
        new_job = self.db.update_resource(self.name,
                                          job.encode())
        self.db.flush(self.name)
        job.last_updated_at = new_job["last_updated_at"]
        return job

    def delete_job(self, job_id):
        result = self.db.delete_resource(self.name,
                                         constants.JOB_TYPE, job_id)
        self.db.flush(self.name)
        return result

#
#    Scheduler Operations
#

    def get_scheduler(self):
        resource = self.db.get_resource(self.name,
                                        constants.SCHEDULER_TYPE,
                                        Scheduler.ID)
        return Scheduler.decode(resource) if resource is not None else None

    def create_scheduler(self, scheduler):
        new_scheduler = self.db.create_resource(self.name,
                                                constants.SCHEDULER_TYPE,
                                                scheduler.encode(),
                                                scheduler.id)
        self.db.flush(self.name)
        scheduler.created_at = new_scheduler["created_at"]
        return scheduler

    def update_scheduler(self, scheduler):
        new_scheduler = self.db.update_resource(self.name,
                                                scheduler.encode())
        self.db.flush(self.name)
        scheduler.last_updated_at = new_scheduler["last_updated_at"]
        return scheduler

#
#    Worker Operations
#

    def get_worker(self, worker_id):
        resource = self.db.get_resource(self.name,
                                        constants.WORKER_TYPE,
                                        worker_id)
        return Worker.decode(resource) if resource is not None else None

    def list_workers(self, state, quiescent=None):
        if state is None:
            where = { 'state': MemberState.JOINED}
        elif state == 'all':
            where = None
        else:
            where = { 'state': state}

        if quiescent is not None:
            if not where:
                where = {}
            where['quiescent'] = 'true' if quiescent else 'false'

        resources = self.db.list_resources(self.name,
                                           constants.WORKER_TYPE,
                                           where)
        logger.debug("Got '%d' workers matching query '%s'", len(resources), where)
        return [Worker.decode(resource) for resource in resources]

    def create_worker(self, worker):
        new_worker = self.db.create_resource(self.name,
                                             constants.WORKER_TYPE,
                                             worker.encode(),
                                             worker.id)
        self.db.flush(self.name)
        worker.created_at = new_worker["created_at"]
        return worker

    def update_worker(self, worker):
        new_worker = self.db.update_resource(self.name,
                                             worker.encode())
        self.db.flush(self.name)
        worker.last_updated_at = new_worker["last_updated_at"]
        return worker

    def delete_worker(self, worker_id):
        result = self.db.delete_resource(self.name,
                                         constants.WORKER_TYPE, worker_id)
        self.db.flush(self.name)
        return result

#
#    History Operations
#

    def write_msg(self, msg):
        '''
        Write a history message to the job database
        '''

        if msg.Type == HeartbeatMsg.Type:
            return

        msg = msg.encode()

        '''
        Remove the envelope from the message (e.g. task_msg, job_msg).
        It does not add information and complicates searching
        '''
        envelope_name = msg.keys()[0]
        msg = msg[envelope_name]

        '''
        Convert timestamp to an ISO date so the DB can infer
        the type of the attribute allowing date searches.
        Moreover, a date formatted string is better for
        results readability.
        '''
        tms = msg.get('timestamp', None)
        msg['timestamp'] = time_util.utc_millis_to_iso8601_datetime(
            tms if tms else time_util.get_current_utc_millis())

        '''
        Flatten frequently searched attributes
        '''
        if 'job' in msg:
            thejob = msg['job']
            if 'full_name' in thejob:
                msg['job_name'] = thejob['full_name']
            if 'id' in thejob:
                msg['job_id'] = thejob['id']

        uid = str(uuid.uuid1())

        # Finally, write to the db
        self.db.insert(self.name, constants.HISTORY_TYPE, uid, msg)
        return msg

    def query_msgs(self, where=None, from_time=None, to_time=None,
                   sort_by='timestamp', sort_order='asc',
                   start=0, size=100, fields=None):
        '''
        Search over the history
        '''
        return self.db.query(self.name, constants.HISTORY_TYPE,
                             where, from_time, to_time,
                             sort_by, sort_order,
                             start, size, fields)

    def delete_old_msgs(self, where=None, older_than=0):
        to_time = time_util.get_current_utc_millis() - older_than
        return self.db.delete_by_query(self.name, constants.HISTORY_TYPE, where=where, to_time=to_time)

#
#   Backward compatibility fixes
#

    def fix_db(self):
        logger.debug("Fixing job database resources for backward compatibility ...")

        # fix for worker docs:
        workers = self.list_workers('all')
        for worker in workers:
            # fix for null quiescent field
            if worker.quiescent is None:
                worker.quiescent = False
            self.update_worker(worker)

        logger.debug("Job database resources fixed.")
