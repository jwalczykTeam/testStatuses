'''
Created on Dec 20, 2016
@author: alberto
'''

import os
import urlparse

from doi.common import FailedException
from doi.api.common import Api
from doi.scheduler.manager import Manager
from doi.util import log_util
from doi.util.log_util import mask_sensitive_info
from doi.util import time_util
from doi.util import uri_util

logger = log_util.get_logger("astro.api.job_api")


class JobApi(Api):
    def __init__(self, config):
        logger.info("Initializing job api: %s ...", mask_sensitive_info(config))
        super(Api, self).__init__()
        self.config = config['api']
        self.manager = Manager(config)
        logger.info("Job api initialized.")

    def close(self):
        logger.info("Closing job api ...")
        if self.manager is not None:
            self.manager.close()
            self.manager = None
        logger.info("Job api closed.")

#
#   Config Methods
#
    def get_config(self):
        env = {}
        for var in os.environ:
            env[var] = os.environ[var]
        return env

#
#   History Methods
#
    def get_history(self, where=None, from_time=None, to_time=None, start=0, size=100):
        try:
            from_time = uri_util.unquote_query_param(from_time)
            to_time = uri_util.unquote_query_param(to_time)

            results = self.manager.query_msgs(where=where, from_time=from_time, to_time=to_time, start=start, size=size)

            return self.build_data(200, results) # OK
        except FailedException as e:
            return self.build_error(e)

    def delete_history(self, where=None, older_than_ms=None):
        try:
            results = self.manager.delete_msgs(where, older_than_ms)

            return self.build_data(202, results) # OK
        except FailedException as e:
            return self.build_error(e)


#
#    Job methods
#
    def list_jobs(self, request_info, job_type, id_=None, name=None, state=None, completed=None):
        key_values = {}
        if id_:
            key_values['id'] = id_
        if name:
            key_values['name'] = name
        if state:
            key_values['current_state'] = state
        if completed:
            key_values['completed'] = completed

        jobs = []
        url = urlparse.urlparse(request_info['request_url'])
        base_url = url.scheme + "://" + url.netloc + url.path
        for job in self.manager.list_jobs(key_values):
            job.url = base_url + '/' + job.name if job.name is not None else 'None'
            json_job = self._externalize_job(job, False)
            json_job.pop("input", None)
            json_job.pop("previous_input", None)
            json_job.pop("channels", None)
            json_job.pop("tasks", None)
            jobs.append(json_job)
        return self.build_data(200, jobs)  # OK

    def list_metric_jobs(self,
                         request_info,
                         job_type,
                         id_=None,
                         name=None,
                         state=None,
                         completed=None,
                         metrics_index=None):
        key_values = {}
        if id_:
            key_values['id'] = id_
        if name:
            key_values['name'] = name
        if state:
            key_values['current_state'] = state
        if completed:
            key_values['completed'] = completed

        jobs = []
        url = urlparse.urlparse(request_info['request_url'])
        base_url = url.scheme + "://" + url.netloc + url.path
        return self.build_data(200, self.manager.list_metric_jobs(metrics_index))  # OK

    def get_job(self, request_info, job_name):
        try:
            job = self.manager.get_job_by_name(request_info['tenant_id'],
                                               job_name)
            job.url = request_info['request_url']
            return self.build_data(
                200,
                self._externalize_job(job))  # OK
        except FailedException as e:
            return self.build_error(e)

    def get_metric_job(self, request_info, job_name, metrics_index=None):
        try:
            job = self.manager.get_metric_job_by_name(request_info['tenant_id'],
                                               job_name, metrics_index)
            return self.build_data(
                200,
                self.manager.get_metric_job_by_name(request_info['tenant_id'],
                                                   job_name, metrics_index))  # OK
        except FailedException as e:
            return self.build_error(e)

    def get_metrics_job(self, request_info, job_name):
        try:
            job = self.manager.get_job_by_name(request_info['tenant_id'],
                                               job_name)
            job.url = request_info['request_url']
            return self.build_data(
                200,
                self._externalize_job(job))  # OK
        except FailedException as e:
            return self.build_error(e)

    def create_job(self, request_url, job):
        try:
            job = self.manager.create_job(job)
            job.url = request_url + '/' + job.name
            return self.build_data(
                202, self._externalize_job(job, False))  # Accepted
        except FailedException as e:
            return self.build_error(e)

    def update_job(self, request_info, job):
        try:
            job = self.manager.update_job(job)
            job.url = request_info['request_url']
            return self.build_data(
                202, self._externalize_job(job)) # Accepted
        except FailedException as e:
            return self.build_error(e)

    def patch_job(self, request_info, job_name, body):
        try:
            if 'description' in body:
                job = self.manager.get_job_by_name(request_info['tenant_id'], job_name)
                job.description = body['description']
                job = self.manager.update_job(request_info, job_name, job.input)
            elif 'stop_requested' in body:
                stop_requested = body['stop_requested']

                if stop_requested:
                    job = self.manager.stop_job(request_info['tenant_id'], job_name)
                else:
                    job = self.manager.restart_job(request_info['tenant_id'], job_name)
            else:
                raise FailedException(
                    422,
                    "Unprocessable Entity",
                    "Invalid job fields")

            job.url = request_info['request_url']
            return self.build_data(
                202, self._externalize_job(job)) # Accepted
        except FailedException as e:
            return self.build_error(e)

    def delete_job(self, request_info, job_name):
        try:
            job = self.manager.delete_job(request_info['tenant_id'], job_name)
            job.url = request_info['request_url']
            return self.build_data(
                202, self._externalize_job(job)) # Accepted
        except FailedException as e:
            return self.build_error(e)

    def _externalize_job(self, job, remove_sensitive_info=True):
        job_obj = job.encode()
        self._convert_to_iso_8601_datetime(job_obj, "created_at")
        self._convert_to_iso_8601_datetime(job_obj, "last_updated_at")
        self._convert_to_iso_8601_datetime(job_obj, "started_at")
        self._convert_to_iso_8601_datetime(job_obj, "ended_at")
        self._convert_to_iso_8601_datetime(job_obj, "last_msg_sent_at")
        self._convert_to_iso_8601_datetime(job_obj, "last_msg_received_at")
        if job.elapsed_time is not None:
            job_obj["elapsed_time"] = time_util.utc_millis_to_iso8601_duration(job.elapsed_time)
        if job.running_time is not None:
            job_obj["running_time"] = time_util.utc_millis_to_iso8601_duration(job.running_time)

        for task_obj in job_obj["tasks"]:
            self._convert_to_iso_8601_datetime(task_obj, "started_at")
            self._convert_to_iso_8601_datetime(task_obj, "ended_at")
            self._convert_to_iso_8601_datetime(task_obj, "last_msg_sent_at")
            self._convert_to_iso_8601_datetime(task_obj, "last_msg_received_at")
            task = job.get_task(task_obj["id"])
            if task.running_time is not None:
                task_obj["running_time"] = time_util.utc_millis_to_iso8601_duration(task.running_time)

        def mask_info(sources):
            if sources is not None:
                for source in sources:
                    if "access_token" in source:
                        source["access_token"] = "********"
                    if "password" in source:
                        source["password"] = "********"
                    if "username" in source:
                        source["username"] = "********"

        if remove_sensitive_info:
            input_ = job_obj.get("input")
            if input_ is not None:
                mask_info(input_.get("sources"))

            previous_input_ = job_obj.get("previous_input")
            if previous_input_ is not None:
                mask_info(previous_input_.get("sources"))

            channels = job_obj["channels"]
            for channel in channels:
                channel.pop("webhook_url", None)
                channel.pop("password", None)

        return job_obj

#
#   Workers Methods
#
    def get_scheduler(self, request_url):
        scheduler = self.manager.get_scheduler()
        scheduler.url = request_url
        return self.build_data(200,  self._externalize_scheduler(scheduler)) # OK

    def _externalize_scheduler(self, scheduler):
        obj = scheduler.encode()
        self._convert_to_iso_8601_datetime(obj, "created_at")
        self._convert_to_iso_8601_datetime(obj, "last_updated_at")
        return obj

    def list_workers(self, request_url, state):
        workers = []
        for worker in self.manager.list_workers(state):
            worker.url = request_url + '/' + worker.id
            workers.append(self._externalize_worker(worker))
        return self.build_data(200,  workers) # OK

    def get_worker(self, request_url, worker_id):
        worker = self.manager.get_worker(worker_id)
        worker.url = request_url
        return self.build_data(200,  self._externalize_worker(worker)) # OK

    def patch_worker(self, request_url, worker_id, body):
        try:
            if 'quiescent' in body:
                worker = self.manager.update_worker(worker_id, body['quiescent'])
                worker.url = request_url
                worker.quiescent = body['quiescent']
            else:
                raise FailedException(
                    422,
                    "Unprocessable Entity",
                    "Invalid worker fields")

            return self.build_data(
                202, self._externalize_worker(worker)) # Accepted
        except FailedException as e:
            return self.build_error(e)

    def _externalize_worker(self, worker):
        obj = worker.encode()
        self._convert_to_iso_8601_datetime(obj, "created_at")
        self._convert_to_iso_8601_datetime(obj, "last_updated_at")
        self._convert_to_iso_8601_datetime(obj, "last_heartbeat_sent_at")
        self._convert_to_iso_8601_datetime(obj, "last_heartbeat_received_at")
        self._convert_to_iso_8601_datetime(obj, "last_msg_sent_at")
        self._convert_to_iso_8601_datetime(obj, "last_msg_received_at")

        if worker.last_heartbeat_sent_at is not None:
            time_from_last_heartbeat_sent = time_util.get_current_utc_millis() - worker.last_heartbeat_sent_at
            obj["time_from_last_heartbeat_sent"] = time_from_last_heartbeat_sent

        if worker.last_heartbeat_received_at is not None:
            time_from_last_heartbeat_received = time_util.get_current_utc_millis() - worker.last_heartbeat_received_at
            obj["time_from_last_heartbeat_received"] = time_from_last_heartbeat_received

        return obj

    def _convert_to_iso_8601_datetime(self, obj, key):
        value = obj.get(key)
        if value is not None:
            obj[key] = time_util.utc_millis_to_iso8601_datetime(value)
        return value
