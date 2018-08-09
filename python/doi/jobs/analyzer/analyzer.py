'''
Created on Sep 25, 2016

@author: alberto
'''

from doi import constants
from doi.common import TaskMethod
from doi.common import TaskState
from doi.common import Error
from doi.common import StoppedException
from doi.common import FailedException
from doi.common import PermanentlyFailedException
from doi.util import log_util
from doi.util import time_util
from doi.util.mb import mb_util
import json
import os
import time
import datetime


logger = log_util.get_logger("astro.jobs.analyzer")


def analyze_current_data(params, progress_callback, stop_event):
    logger.info(
        "Starting analyzing current data: job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d ...",
        params.job_id,
        params.job_name,
        params.task_id,
        params.method,
        params.rev)

    if params.method == TaskMethod.DELETE:
        # Nothing to delete
        return

    analyzer = Analyzer(params, progress_callback, stop_event)

    # we want to wait until we're connected as a consumer before we send the msg.
    # otherwise there could be a race condition where we get the resp but we havent yet connected.
    # so passes the msg sending func as a callback to get_response() so get_response() can call it at the proper time.
    response = analyzer.get_response(analyzer.send_execute_task_msg)
    analyzer.handle_response(response)

    logger.info("Completed analyzing current data")


class Analyzer(object):
    REQUESTS_TOPIC_NAME = "rudi-requests"
    RESPONSES_TOPIC_NAME = "rudi-responses"

    def __init__(self, params, progress_callback, stop_event):
        self.job_id = params.job_id
        self.job_name = params.job_name
        self.task_id = params.task_id
        self.method = params.method
        self.rev = params.rev
        self.input = params.input
        self.previous_input = params.previous_input
        self.progress_callback = progress_callback
        self.stop_event = stop_event
        self.msg_id = "/".join([
            self.method,
            self.job_id,
            self.task_id,
            str(self.rev)])

        # Initialize message broker
        mb_config = json.loads(os.environ[constants.ASTRO_MB_ENV_VAR])
        self.mb = mb_util.MessageBroker(mb_config)

    @property
    def requests_topic_name(self):
        return Analyzer.REQUESTS_TOPIC_NAME

    @property
    def responses_topic_name(self):
        return Analyzer.RESPONSES_TOPIC_NAME

    @property
    def mb_client_id(self):
        return "task-{}-{}-client".format(self.job_id, self.task_id)

    @property
    def mb_group_id(self):
        # return "task-{}-{}-group".format(self.job_id, self.task_id)
        # create a unique name to to avoid stuck consumers
        ts = time.time()
        date_time = datetime.datetime.fromtimestamp(
            ts).strftime('%Y_%m_%d_%H_%M_%S')
        return hash("{}{}".format(self.task_id, date_time))

    def ensure_topics_exist(self):
        # Rudi should ensure that this topics exist ...
        mb_manager = self.mb.new_manager()
        try:
            logger.info("Ensuring topic '%s' exists", self.requests_topic_name)
            mb_manager.ensure_topic_exists(self.requests_topic_name)
            logger.info("Ensuring topic '%s' exists",
                        self.responses_topic_name)
            mb_manager.ensure_topic_exists(self.responses_topic_name)
        finally:
            mb_manager.close()

    def send_execute_task_msg(self):
        msg = {
            "task_msg": {
                "timestamp": time_util.get_current_utc_millis(),
                "type": "execute_task",
                "msg_id": self.msg_id,
                "job_id": self.job_id,
                "job_name": self.job_name,
                "task_id": self.task_id,
                "method": self.method,
                "rev": self.rev,
                "input": self.input,
                "previous_input": self.previous_input
            }
        }

        self.ensure_topics_exist()
        logger.info("Sending 'execute_task' message to rudi's requests topic: "
                    "job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d ...",
                    self.job_id, self.job_name, self.task_id, self.method, self.rev)
        mb_producer = self.mb.new_producer(self.mb_client_id)
        mb_producer.send(self.requests_topic_name, msg)

    def get_response(self, on_subscribed=None):
        mb_consumer = self.mb.new_consumer(self.mb_client_id, self.mb_group_id)

        try:
            mb_consumer.subscribe([self.responses_topic_name])
            logger.info("Subscribed to rudi's responses topic: %s",
                        self.responses_topic_name)

            # if we have a func to call when the subscribe is connected, runs it
            if on_subscribed is not None:
                on_subscribed()

            for msg in mb_consumer.messages(stop_event=self.stop_event):
                try:
                    if "task_msg" not in msg.value:
                        logger.warning(
                            "Unknown message received: %s", msg.value)
                        continue

                    response = msg.value["task_msg"]
                    type_ = response["type"]
                    if type_ != 'task_ended':
                        logger.warning(
                            "Expected 'task_ended' message type, found: %s",
                            type_)
                        continue

                    if "msg_id" in response:
                        msg_id = response["msg_id"]
                        if msg_id == self.msg_id:
                            logger.info(
                                "Response message received: msg_id='%s'",
                                msg_id)
                            return response

                    else:
                        # deprecate old logic

                        job_id = response["job_id"]
                        task_id = response["task_id"]
                        method = response["method"]

                        if (job_id, task_id, method) == (
                                self.job_id, self.task_id, self.method):
                            logger.info(
                                "Response message received: job_id='%s', task_id='%s', method='%s'",
                                job_id,
                                task_id,
                                method)
                            return response
                except:
                    logger.exception(
                        "Unexpected exception while processing a message: %s",
                        msg.value)

            if self.stop_event.is_set():
                # Stop execution, ignore results
                raise StoppedException()

        finally:
            mb_consumer.close()

        raise Exception("No response message received from rudi")

    def handle_response(self, response):
        state = response["state"]

        assert state in {TaskState.SUCCESSFUL, TaskState.FAILED, TaskState.PERMANENTLY_FAILED}, \
            "Invalid completion state: {}".format(state)

        if state == TaskState.SUCCESSFUL:
            # Just return when task is successful
            return

        error = Error.decode(response.get("error"))
        assert error is not None, "Error expected when state is failed or permanently failed"

        if state == TaskState.FAILED:
            raise FailedException(
                error.type,
                error.title,
                error.status,
                error.detail,
                **error.kwargs)
        elif state == TaskState.PERMANENTLY_FAILED:
            raise PermanentlyFailedException(
                error.type,
                error.title,
                error.status,
                error.detail,
                **error.kwargs)
