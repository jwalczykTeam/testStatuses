'''
Created on Aug 27, 2016

@author: alberto
'''


from doi import constants
from doi.common import TaskMethod
from doi.common import FailedException
from doi.scheduler.common import CreateJobMsg, UpdateJobMsg, DeleteJobMsg, \
        StopJobMsg, RestartJobMsg, DeleteJobHistoryMsg, UpdateWorkerMsg
from doi.scheduler.database import JobDatabase
from doi.util import log_util
from doi.util.log_util import mask_sensitive_info
from doi.util import time_util
from doi.util.db import db_util
from doi.util.mb import mb_util
import uuid


logger = log_util.get_logger("astro.scheduler.manager")


class Manager(object):
    def __init__(self, config):
        super(Manager, self).__init__()
        logger.info("Initializing job manager: %s ...", mask_sensitive_info(config))
        group_id = config['group_id']

        # Reset properties
        self.job_db = None
        self.mb_producer = None
        self.scheduler_messages_topic_name = constants.get_scheduler_messages_topic_name(group_id)

        # Initialize job database
        db = db_util.Database.factory(config["db"])
        self.job_db = JobDatabase(db, group_id)

        mb = mb_util.MessageBroker(config["mb"])
        mb_client_id = constants.get_api_client_id(group_id)
        self.mb_producer = mb.new_producer(mb_client_id)
        logger.info("Job manager initialized.")

    def close(self):
        logger.info("Closing job manager ...")
        if self.mb_producer is not None:
            self.mb_producer.close()
            self.mb_producer = None

        if self.job_db is not None:
            self.job_db.close()
            self.job_db = None
        logger.info("Job manager closed.")

    def list_jobs(self, key_values=None, size=None):
        return self.job_db.list_jobs(key_values, size)

    def list_metric_jobs(self, metrics_index=None, size=None):
        return self.job_db.list_metric_jobs(metrics_index, size, 'metrics_current')

    def get_job_by_id(self, job_id):
        return self.job_db.get_job(job_id)

    def get_job_by_name(self, tenant_id, job_name):
        job = self._get_job_by_name(tenant_id, job_name)
        if job is None:
            raise FailedException(
                "about:blank",
                "Not Found",
                404,
                "Job not found: job_name='{}'".format(job_name))
        return job

    def get_metric_job_by_name(self, tenant_id, job_name, metrics_index=None):
        job = self._get_metric_job_by_name(tenant_id, job_name, metrics_index)
        if job is None:
            raise FailedException(
                "about:blank",
                "Not Found",
                404,
                "Job not found: job_name='{}'".format(job_name))
        return job

    def get_metrics_job_by_name(self, tenant_id, job_name):
        job = self._get_metrics_job_by_name(tenant_id, job_name)
        if job is None:
            raise FailedException(
                "about:blank",
                "Not Found",
                404,
                "Job not found: job_name='{}'".format(job_name))
        return job

    def create_job(self, job):
        logger.info("Handling create job request: tenant_id='%s', job_name='%s' ...",
                    job.tenant_id, job.name)

        if not db_util.Database.is_db_name_valid(job.name):
            raise FailedException(
                "about:blank",
                "Forbidden",
                403,
                "Invalid characters in job name '{}'. "
                "Valid characters are: lowercase characters (a-z), digits (0-9), any of the characters _, $, (, ), +, and -".format(job.name))

        job_with_same_name = self._get_job_by_name(job.tenant_id, job.name)
        if job_with_same_name is not None:
            raise FailedException(
                "about:blank",
                "Conflict",
                409,
                "A job with the same name already exists: job_id='{}', job_name='{}'".format(
                    job_with_same_name.id, job_with_same_name.name))

        job.id = str(uuid.uuid1())
        job.created_at = time_util.get_current_utc_millis()
        job.current_rev = 1
        job.desired_rev = job.current_rev

        msg = CreateJobMsg(job)
        self.mb_producer.send(self.scheduler_messages_topic_name, msg.encode())
        logger.info("Create job request enqueued: job_id='%s', job_name='%s'",
                    job.id, job.name)
        return job

    def update_job(self, new_job):
        logger.info("Handling update job request: tenant_id='%s', job_name='%s' ...",
                    new_job.tenant_id, new_job.name)

        job = self.get_job_by_name(new_job.tenant_id, new_job.name)
        if not job.current_method in [TaskMethod.CREATE, TaskMethod.UPDATE]:
            raise FailedException(
                "about:blank",
                "Conflict",
                409,
                "Job current method is not create or update: job_id='{}', job_name='{}', current_method='{}'".format(
                    job.id, job.name, job.current_method))

        # only the following fields can be updated
        job.title = new_job.title
        job.description = new_job.description
        job.channels = new_job.channels
        job.input = new_job.input
        job.tasks = new_job.tasks
        job.last_updated_by = new_job.last_updated_by

        msg = UpdateJobMsg(job)
        self.mb_producer.send(self.scheduler_messages_topic_name, msg.encode())
        logger.info("Update job request enqueued: job_id='%s', job_name='%s' ...",
            job.id, job.name)
        return new_job

    def delete_job(self, tenant_id, job_name):
        logger.info("Handling delete job request: tenant_id='%s', name='%s' ...",
            tenant_id, job_name)

        job = self.get_job_by_name(tenant_id, job_name)
        if job.current_method not in [TaskMethod.CREATE, TaskMethod.UPDATE]:
            raise FailedException(
                "about:blank",
                "Conflict",
                409,
                "job current method is not create or update: job_id='{}', job_name='{}', current_method='{}'".format(
                    job.id, job.name, job.current_method))

        msg = DeleteJobMsg(job.id)
        self.mb_producer.send(self.scheduler_messages_topic_name, msg.encode())
        logger.info("Delete job request enqueued: job_id='%s', job_name='%s' ...",
            job.id, job.name)
        return job

    def stop_job(self, tenant_id, job_name):
        logger.info("Handling stop job request: tenant_id='%s', job_name='%s' ...",
            tenant_id, job_name)

        job = self.get_job_by_name(tenant_id, job_name)
        if job.stop_requested:
            raise FailedException(
                "about:blank",
                "Conflict",
                409,
                "Stop already requested: job_id='{}', job_name='{}'".format(
                    job.id, job.name))

        msg = StopJobMsg(job.id)
        self.mb_producer.send(self.scheduler_messages_topic_name, msg.encode())
        logger.info("Stop job request enqueued: job_id='%s', job_name='%s' ...",
            job.id, job.name)
        return job

    def restart_job(self, tenant_id, job_name):
        logger.info("Handling restart job request: tenant_id='%s', job_name='%s' ...",
            tenant_id, job_name)

        job = self.get_job_by_name(tenant_id, job_name)
        if not job.stop_requested:
            raise FailedException(
                "about:blank",
                "Conflict",
                409,
                "Restart already requested: job_id='{}', job_name='{}'".format(
                    job.id, job.name))

        msg = RestartJobMsg(job.id)
        self.mb_producer.send(self.scheduler_messages_topic_name, msg.encode())
        logger.info("Restart job request enqueued: id='%s', name='%s' ...",
            job.id, job.name)
        return job

    def get_scheduler(self):
        scheduler = self.job_db.get_scheduler()
        if scheduler is None:
            raise FailedException(
                "about:blank",
                "Not Found",
                404,
                "Scheduler not found")

        return scheduler

    def list_workers(self, state):
        return self.job_db.list_workers(state)

    def get_worker(self, worker_id):
        worker = self.job_db.get_worker(worker_id)
        if worker is None:
            raise FailedException(
                "about:blank",
                "Not Found",
                404,
                "Worker not found: id='{}'".format(worker_id))

        return worker

    def update_worker(self, worker_id, quiescent):
        worker = self.get_worker(worker_id)
        msg = UpdateWorkerMsg(worker_id)
        msg.quiescent = quiescent
        self.mb_producer.send(self.scheduler_messages_topic_name, msg.encode())
        logger.info("Update worker message enqueued: worker_id='%s', quiescent=%s ...",
            worker_id, quiescent)

        return worker

    def _get_job_by_name(self, tenant_id, job_name):
        job_key_values = {
            'name': job_name
        }
        if tenant_id:
            job_key_values['tenant_id'] = tenant_id
        jobs = self.job_db.list_jobs(job_key_values, 1)
        return jobs[0] if jobs else None

    def _get_metric_job_by_name(self, tenant_id, job_name, metrics_index=None):
        job_key_values = {
            'name': job_name
        }
        if tenant_id:
            job_key_values['tenant_id'] = tenant_id
        jobs = self.job_db.list_metric_jobs(metrics_index, 1, 'metrics_stats', 'timestamp', 'desc')
        return jobs[0] if jobs else None

    def _get_metrics_job_by_name(self, tenant_id, job_name):
        job_key_values = {
            'name': job_name
        }
        if tenant_id:
            job_key_values['tenant_id'] = tenant_id
        jobs = self.job_db.list_metrics(job_key_values, 1)
        return jobs[0] if jobs else None

    def query_msgs(self, where=None, from_time=None, to_time=None,
                   sort_by='timestamp', sort_order='asc',
                   start=0, size=100, fields=None):
        return self.job_db.query_msgs(where,
                                      from_time,
                                      to_time,
                                      sort_by,
                                      sort_order,
                                      start, size,
                                      fields)

    def delete_msgs(self, where=None, older_than_ms=None):
        '''
        To send or not to send!
        '''

        # To send
        msg = DeleteJobHistoryMsg(where, older_than_ms)
        self.mb_producer.send(self.scheduler_messages_topic_name, msg.encode())
        logger.info("Delete history message enqueued: where='%s', older_than_days='%d' ...",
            where, older_than_ms/(24*3600*1000))
        return {'message': 'Delete history messages accepted'}
