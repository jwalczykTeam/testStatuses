'''
Created on Aug 27, 2016

@author: alberto
'''


from doi import constants
from doi.common import MemberState, TaskMethod
from doi.scheduler.common import Scheduler
from doi.scheduler.common import Worker
from doi.scheduler.common import TaskState
from doi.scheduler.common import JobMsg, CreateJobMsg, UpdateJobMsg, DeleteJobMsg, StopJobMsg, RestartJobMsg
from doi.scheduler.common import TaskMsg, ExecuteTaskMsg, TaskStartedMsg, StopTaskMsg, TaskEndedMsg, TaskProgressChangedMsg
from doi.scheduler.common import MemberMsg, JoinGroupMsg, LeaveGroupMsg, HeartbeatMsg, DeleteJobHistoryMsg, UpdateWorkerMsg
from doi.scheduler.database import JobDatabase
from doi.util import log_util
from doi.util.log_util import mask_sensitive_info
from doi.util import stat_util
from doi.util import time_util
from doi.util import timer_util
from doi.util.db import db_util
from doi.util.mb import mb_util

from doi.jobs.miner import stream

from operator import attrgetter
import os
import time
import newrelic.agent


logger = log_util.get_logger("astro.scheduler.scheduler")
application = newrelic.agent.register_application(timeout=10.0)


class SchedulerService(object):
    WORKER_HEARTBEAT_TIMEOUT_DURATION_RATIO = 2
    
    # Designed not to raise exceptions
    def __init__(self, config):
        super(SchedulerService, self).__init__() 
        logger.info("Initializing scheduler: %s ...", mask_sensitive_info(config)) 
        self.group_id = config['group_id']
        self.error_retry_interval = config['error_retry_interval']
        self.error_notification_interval = config['error_notification_interval']
        self.slack_webhook_url = config['slack_webhook_url']                
        self.scheduler_update_interval = config["scheduler_update_interval"] * 1000
        self.rebalance_job_tasks_interval = config["rebalance_job_tasks_interval"] * 1000
        self.min_worker_heartbeat_timeout = config["min_worker_heartbeat_timeout"] * 1000
        self.history_retention_period = config["history_retention_period_days"] * 24 * 3600 * 1000 # Cleanup history messages older than history_retention_period
        self.cleanup_history_interval = config["cleanup_history_interval"] * 1000 # How frequently cleanup history messages older than history_retention_period
        self.scheduler_messages_topic_name = constants.get_scheduler_messages_topic_name(self.group_id)      
        self.workers_messages_topic_name = constants.get_workers_messages_topic_name(self.group_id)
        self.resource_db_events_topic_name = constants.get_resource_db_events_topic_name(self.group_id)      
        self.db_config = config["db"]
        self.mb_config = config["mb"]

        logger.info("group_id='%s'", self.group_id)        
        logger.info("error_retry_interval=%d", self.error_retry_interval)
        logger.info("error_notification_interval=%d", self.error_notification_interval)
        logger.info("slack_webhook_url=%s", "present" if self.slack_webhook_url else "absent")
        logger.info("min_worker_heartbeat_timeout=%d", self.min_worker_heartbeat_timeout)        
        logger.info("scheduler_messages_topic_name='%s'", self.scheduler_messages_topic_name)        
        logger.info("workers_messages_topic_name='%s'", self.workers_messages_topic_name)
        logger.info("resource_db_events_topic_name=%s", self.resource_db_events_topic_name)         

        self.job_db = None
        self.mb_producer = None
        self.mb_consumer = None
        self.cleanedup_history_at = self.rebalanced_job_tasks_at = time_util.get_current_utc_millis()
        self.scheduler = Scheduler()
        self.scheduler.scheduler_update_interval =  self.scheduler_update_interval   
        self.scheduler.rebalance_job_tasks_interval = self.rebalance_job_tasks_interval   
        self.scheduler.min_worker_heartbeat_timeout = self.min_worker_heartbeat_timeout   
        self.msg_received_throughput = stat_util.Throughput(60)
        self.msg_sent_throughput = stat_util.Throughput(60)
        self.updated_scheduler_at = time_util.get_current_utc_millis()        


    def open(self):                            
        # Enable notifications
        logger.enable_slack_notification(self.slack_webhook_url)
                
        # Initialize database and message producer/consumer
        db = db_util.Database.factory(self.db_config)        
        self.job_db = JobDatabase(db, self.group_id)
        self.job_db.fix_db()
        
        mb = mb_util.MessageBroker(self.mb_config)
        mb_manager = mb.new_manager()
        try:                          
            logger.info("Ensuring topic '%s' exists", self.scheduler_messages_topic_name)      
            mb_manager.ensure_topic_exists(self.scheduler_messages_topic_name)
            logger.info("Ensuring topic '%s' exists", self.workers_messages_topic_name)      
            mb_manager.ensure_topic_exists(self.workers_messages_topic_name)
            logger.info("Ensuring topic '%s' exists", self.resource_db_events_topic_name)      
            mb_manager.ensure_topic_exists(self.resource_db_events_topic_name)
        finally:
            mb_manager.close()

        mb_client_id = constants.get_scheduler_client_id(self.group_id)        
        mb_group_id = constants.get_scheduler_group_id(self.group_id)
        logger.info("mb_client_id='%s'", mb_client_id)        
        logger.info("mb_group_id='%s'", mb_group_id)        

        self.mb_producer = mb.new_producer(mb_client_id)
        self.mb_consumer = mb.new_consumer(mb_client_id, mb_group_id)
        self._create_scheduler()
        logger.info("Scheduler initialized: group_id='%s'", self.group_id)
            
    def close(self):
        logger.info("Closing scheduler: group_id='%s' ...", self.group_id)       
        
        if self.mb_consumer is not None:
            try:
                self.mb_consumer.close()
            except:
                logger.exception("Unexpected exception while closing message consumer.")
            self.mb_consumer = None

        if self.mb_producer is not None:
            try:
                self.mb_producer.close()
            except:
                logger.exception("Unexpected exception while closing message producer.")
            self.mb_producer = None

        if self.job_db is not None:
            try:
                self.job_db.close()
            except:
                logger.exception("Unexpected exception while closing job database.")
            self.job_db = None
        logger.info("Scheduler closed: group_id='%s' ...", self.group_id)

    @staticmethod
    def start(config):
        scheduler = SchedulerService(config)
        timer = timer_util.Timer(scheduler.error_notification_interval)
        
        while True: 
            try:
                scheduler.open()
                logger.info("Astro scheduler successfully started: group_id='%s'", 
                            scheduler.group_id, notify=True)                
                timer.reset()
                scheduler.run()
            except KeyboardInterrupt:
                logger.info("Astro scheduler interrupted. Gracefully exiting: group_id='%s'",
                            scheduler.group_id, notify=True)                
                break                
            except:
                timer.activate()
                logger.exception("Unexpected exception in astro scheduler: group_id='%s'", 
                                 scheduler.group_id, notify=timer.active)
            finally:
                scheduler.close()            
                logger.info("Astro scheduler stopped: group_id='%s'",
                            scheduler.group_id, notify=timer.active)                
    
            logger.info("Restarting astro scheduler in %d seconds: group_id='%s'", 
                        scheduler.error_retry_interval, scheduler.group_id, notify=timer.active)
            time.sleep(scheduler.error_retry_interval)    

    def run(self):
        self.mb_consumer.subscribe([self.scheduler_messages_topic_name])
        logger.info("Subscribed to scheduler messages topic: %s",
            self.scheduler_messages_topic_name)

        for clear_stream in stream.StreamMiner.delete_job_payload():
            logger.info("Clearing streaming {}".format(clear_stream))
            if isinstance(clear_stream, basestring):
                self.job_db.delete_job(clear_stream)
            else:
                self._handle_delete_job_msg(clear_stream)
                self.job_db.write_msg(clear_stream)

        start_stream = stream.StreamMiner.create_job_payload(
            os.environ.get('NUMBER_OF_WORKERS'))
        logger.info("Starting stream {}".format(start_stream))
        job_msg = JobMsg.factory(start_stream)
        self._handle_create_job_msg(job_msg)
        self.job_db.write_msg(job_msg)

        for msg in self.mb_consumer.messages():
            current_time = time_util.get_current_utc_millis()
            try:
                if "job_msg" in msg.value:
                    #logger.debug("Job message received from client: %s", msg.value["job_msg"])
                    job_msg = JobMsg.factory(msg.value)
                    if job_msg.Type == CreateJobMsg.Type:
                        self._handle_create_job_msg(job_msg)
                    elif job_msg.Type == UpdateJobMsg.Type:
                        self._handle_update_job_msg(job_msg)
                    elif job_msg.Type == DeleteJobMsg.Type:
                        self._handle_delete_job_msg(job_msg)
                    elif job_msg.Type == StopJobMsg.Type:
                        self._handle_stop_job_msg(job_msg)
                    elif job_msg.Type == RestartJobMsg.Type:
                        self._handle_restart_job_msg(job_msg)
                    elif job_msg.Type == DeleteJobHistoryMsg.Type:
                        self._handle_delete_job_history_msg(job_msg)
                    else:
                        logger.warning("Unknown job message received: %s", msg.value)

                    self.job_db.write_msg(job_msg)
                    self.mb_consumer.commit(msg)
                elif "task_msg" in msg.value:
                    #logger.debug("Task message received from worker: %s", msg.value["task_msg"])
                    task_msg = TaskMsg.factory(msg.value)
                    if task_msg.Type == TaskStartedMsg.Type:
                        self._handle_task_started_msg(task_msg)
                    elif task_msg.Type == TaskEndedMsg.Type:
                        self._handle_task_ended_msg(task_msg)
                    elif task_msg.Type == TaskProgressChangedMsg.Type:
                        self._handle_task_progress_changed_msg(task_msg)
                    else:
                        logger.warning("Unknown task message received: %s", msg.value)

                    self.job_db.write_msg(task_msg)
                    self.mb_consumer.commit(msg)
                elif "member_msg" in msg.value:
                    member_msg = MemberMsg.factory(msg.value)
                    if member_msg.Type == JoinGroupMsg.Type:
                        self._handle_join_group_msg(member_msg)
                    elif member_msg.Type == LeaveGroupMsg.Type:
                        self._handle_group_left_msg(member_msg)
                    elif member_msg.Type == HeartbeatMsg.Type:
                        self._handle_heartbeat_msg(member_msg)
                    elif member_msg.Type == UpdateWorkerMsg.Type:
                        self._handle_update_worker_msg(member_msg)
                    else:
                        logger.warning("Unknown member message received: %s", msg.value)

                    self.job_db.write_msg(member_msg)
                    self.mb_consumer.commit(msg)
                else:
                    logger.warning("Unknown message received: %s", msg.value)

            except:
                logger.exception("Unexpected exception while processing a message: %s", msg.value)
            
            self.msg_received_throughput.increase()
            if current_time - self.updated_scheduler_at > self.scheduler_update_interval:
                self._update_scheduler()
                self.updated_scheduler_at = current_time
            # logger.info("Waiting for next message ...")

#
#   Job message handlers
#
    def _handle_create_job_msg(self, msg):
        logger.info("Handling '%s' message: job_id='%s', job_name='%s' ...", 
            msg.Type, msg.job.id, msg.job.full_name)
        
        job = self.job_db.get_job(msg.job.id)
        if job is not None:
            logger.warning("Cannot create job, a job with the same id already exists: "
                           "job_id='%s'. Ignored.", msg.job.id)
            return

        job = msg.job
        
        job_with_same_name = self._get_job_by_name(job.tenant_id, job.name)
        if job_with_same_name is not None:
            logger.warning("Cannot create job, a job with the same name already exists: "
                           "job_id='%s', job_name='%s'. Ignored.",
                           job_with_same_name.id, job_with_same_name.name)
            return
        
        job.current_method = TaskMethod.CREATE
        job.desired_method = TaskMethod.CREATE
        self.scheduler.create_job_msgs_received += 1
                
        self.job_db.create_job(job)
        logger.info("Job object created: job_id='%s', job_name='%s'", job.id, job.full_name)                
        self._coordinate_job_tasks(job)

    def _handle_update_job_msg(self, msg):
        logger.info("Handling '%s' message: job_id='%s', job_name='%s' current_rev=%d, desired_rev=%d...", 
            msg.Type, msg.job.id, msg.job.full_name, msg.job.current_rev, msg.job.desired_rev)

        job = self.job_db.get_job(msg.job.id)
        if job is None:
            logger.warning("Cannot update job, job id not found: job_id='%s'. Ignored.", msg.job.id)
            return
        
        if job.current_method not in [TaskMethod.CREATE, TaskMethod.UPDATE]:
            logger.warning("Cannot update job, job current method is not create or update: "
                           "job_id='%s', job_name='%s', current_method='%s'. Ignored.",
                           job.id, job.name, job.current_method)
            return

        new_job = msg.job
        job.desired_method = TaskMethod.UPDATE
        job.title = new_job.title
        job.description = new_job.description
        job.channels = new_job.channels
        job.previous_input = job.input
        job.input = new_job.input
        self._merge_job_tasks(job, new_job)
        job.last_updated_by = new_job.last_updated_by
        job.desired_rev = job.current_rev +1
        
        self.scheduler.update_job_msgs_received += 1
                
        self.job_db.update_job(job)
        logger.info("Job object updated: job_id='%s', job_name='%s'", 
            job.id, job.full_name)        
        self._coordinate_job_tasks(job)

    def _handle_delete_job_msg(self, msg):
        logger.info("Handling '%s' message: job_id='%s ...", 
            msg.Type, msg.job_id)

        job = self.job_db.get_job(msg.job_id)
        if job is None:
            logger.warning("Cannot delete job, job id not found: job_id='%s'. Ignored.", msg.job_id)
            return
        
        if job.current_method not in [TaskMethod.CREATE, TaskMethod.UPDATE]:
            logger.warning("Cannot delete job, job current method is not create or update: "
                           "job_id='%s', job_name='%s', current_method='%s'. Ignored.",
                           job.id, job.name, job.current_method)
            return
        
        job.desired_method = TaskMethod.DELETE
        self.scheduler.delete_job_msgs_received += 1
                
        self.job_db.update_job(job)
        logger.info("Job object updated: job_id='%s', job_name='%s'", 
                    job.id, job.full_name)        
        self._coordinate_job_tasks(job)

    def _handle_stop_job_msg(self, msg):
        logger.info("Handling '%s' message: job_id='%s' ...", msg.Type, msg.job_id)
        
        job = self.job_db.get_job(msg.job_id)
        if job is None:
            logger.warning("Cannot stop job, job id not found: job_id='%s'. Ignored.",
                           msg.job_id)
            return
        
        if job.stop_requested:
            logger.warning("Stop already requested: job_id='%s', job_name='%s'. Ignored.",
                           job.id, job.name)
            return
        
        job.stop_requested = True
        self.scheduler.stop_job_msgs_received += 1
                                
        self.job_db.update_job(job)
        logger.info("Job object updated: job_id='%s', job_name='%s'", 
                    job.id, job.full_name)        
        self._coordinate_job_tasks(job)

    def _handle_restart_job_msg(self, msg):
        logger.info("Handling '%s' message: job_id='%s' ...", 
                    msg.Type, msg.job_id)
        
        job = self.job_db.get_job(msg.job_id)
        if job is None:
            logger.warning("Cannot restart job, jod id not found: job_id='%s'. Ignored.",
                           msg.Type, msg.job_id)
            return

        if not job.stop_requested:
            logger.warning("Restart already requested: job_id='%s', job_name='%s'. Ignored.",
                           job.id, job.name)
            return
        
        job.stop_requested = False
        self.scheduler.restart_job_msgs_received += 1
                                
        self.job_db.update_job(job)
        logger.warning("Job object updated: job_id='%s', job_name='%s'", 
                       job.id, job.full_name)        
        self._coordinate_job_tasks(job)

    def _handle_delete_job_history_msg(self, msg):
        logger.info("Handling '%s' message: ...", msg.Type)

        results = self.job_db.delete_old_msgs(msg.where, msg.older_than_ms)
        logger.info("Deleted '%d' history messages older than '%d' days matching query '%s'",
                    results['deleted'], msg.older_than_ms/(24*3600*1000), msg.where)

    def _get_job_by_name(self, tenant_id, job_name):
        job_key_values = {
            'name': job_name
        }
        
        if tenant_id:
            job_key_values['tenant_id'] = tenant_id
            
        jobs = self.job_db.list_jobs(job_key_values, 1)
        return jobs[0] if jobs else None        
        
    def _merge_job_tasks(self, job, new_job):
        # add new tasks
        for new_task in new_job.get_tasks():
            if job.has_task(new_task.id):
                task = job.get_task(new_task.id)
                task.condition = new_task.condition
            else:
                job.add_task(new_task)
                
        # mark for deletion tasks that are no more needed
        for task in job.get_tasks():
            if not new_job.has_task(task.id):
                task.marked_for_deletion = True

#
#   Task message handlers
#
    def _handle_task_started_msg(self, msg):
        logger.info("Handling '%s' message: job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d, worker_id='%s' ...", 
                    msg.Type, msg.job_id, msg.job_name, msg.task_id, msg.method, msg.rev, msg.worker_id)

        job = self.job_db.get_job(msg.job_id)
        if job is None:
            logger.warning("Received '%s' message for unknown job: job_id='%s', job_name='%s', worker_id='%s'. Ignored.",
                           msg.Type, msg.job_id, msg.job_name, msg.worker_id)
            return

        task = job.get_task(msg.task_id)
        logger.info("Changing task current state: job_id='%s', job_name='%s', task_id='%s', "
                    "method='%s', rev=%d, state='%s'->'%s', worker_id='%s' ...", 
                    msg.job_id, msg.job_name, msg.task_id, msg.method, msg.rev,
                    task.current_state, TaskState.RUNNING, msg.worker_id)

        task.current_state = TaskState.RUNNING
        task.started_at = msg.started_at        
        task.last_msg_received = "{}({})".format(msg.Type, msg.id)
        task.last_msg_received_at = time_util.get_current_utc_millis()                            
        if job.started_at is None:
            job.started_at = task.started_at
        task.started_count += 1
        self.scheduler.task_started_msgs_received += 1
            
        self.job_db.update_job(job)
        logger.info("Job object updated: job_id='%s', job_name='%s'", 
                    job.id, job.full_name)        

        worker = self.job_db.get_worker(msg.worker_id)
        if worker is None:
            logger.warning("Received '%s' message from unknown worker: worker_id='%s'. Ignored.", 
                           msg.Type, msg.worker_id)
            return

        worker.add_executing_task_id(task.full_id) 
        self.job_db.update_worker(worker)
        logger.info("Worker object updated: worker_id='%s'", worker.id)   
        
    def _handle_task_ended_msg(self, msg):
        logger.info("Handling '%s' message: job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d, worker_id='%s' ...", 
                    msg.Type, msg.job_id, msg.job_name, msg.task_id, msg.method, msg.rev, msg.worker_id)

        job = self.job_db.get_job(msg.job_id)
        if job is None:
            logger.warning("Received '%s' message for unknown job: job_id='%s', job_name='%s', worker_id='%s'. Ignored.",
                           msg.Type, msg.job_id, msg.job_name, msg.worker_id)
            return
        
        task = job.get_task(msg.task_id)
        logger.info("Changing task current state: job_id='%s', job_name='%s', task_id='%s', "
                    "method='%s', rev=%d, state='%s'->'%s', worker_id='%s' ...", 
                    msg.job_id, msg.job_name, msg.task_id, msg.method, msg.rev, 
                    task.current_state, msg.state, msg.worker_id)
        
        task.current_state = msg.state
        task.output = msg.output
        task.last_error = msg.error
        if task.last_error is not None:
            # Add context for the error
            error_kwargs = task.last_error.kwargs
            if error_kwargs is None:
                error_kwargs = {}
            error_kwargs['progress'] = task.progress

        task.ended_at = msg.ended_at                
        job.ended_at = task.ended_at                
        task.last_msg_received = "{}({})".format(msg.Type, msg.id)
        task.last_msg_received_at = time_util.get_current_utc_millis()

        task.ended_count += 1
        if task.current_state == TaskState.SUCCESSFUL:
            task.successful_count += 1
            self.scheduler.successful_tasks += 1            
        elif task.current_state == TaskState.FAILED:
            task.failed_count += 1
            self.scheduler.failed_tasks += 1            
        elif task.current_state == TaskState.PERMANENTLY_FAILED:
            task.permanently_failed_count += 1
            self.scheduler.permanently_failed_tasks += 1            
        elif task.current_state == TaskState.STOPPED:
            task.stopped_count += 1
            self.scheduler.stopped_tasks += 1            
        self.scheduler.task_ended_msgs_received += 1
                               
        self.job_db.update_job(job)
        logger.info("Job object updated: job_id='%s', job_name='%s'", job.id, job.full_name)        

        worker = self.job_db.get_worker(msg.worker_id)
        if worker is None:
            logger.warning("Received '%s' message from unknown worker: worker_id='%s'. Ignored.", 
                           msg.Type, msg.worker_id)
            return

        worker.remove_executing_task_id(task.full_id) 
        self.job_db.update_worker(worker)
        logger.info("Worker object updated: worker_id='%s'", worker.id)                        
        self._coordinate_job_tasks(job)

    def _handle_task_progress_changed_msg(self, msg):
        logger.info("Handling '%s' message: job_id='%s', task_id='%s', method='%s', rev=%d, worker_id='%s' ...", 
                    msg.Type, msg.job_id, msg.task_id, msg.method, msg.rev, msg.worker_id)
        
        job = self.job_db.get_job(msg.job_id)
        if job is None:
            logger.warning("Received '%s' message for unknown job: job_id='%s', worker_id='%s'. Ignored.",
                           msg.Type, msg.job_id, msg.worker_id)
            return
        
        task = job.get_task(msg.task_id)
        logger.info("Changing task progress: job_id='%s', job_name='%s', task_id='%s', "
                    "method='%s', progress='%s'->'%s', worker_id='%s' ...", 
                    msg.job_id, job.full_name, msg.task_id, job.current_method,
                    task.progress, msg.progress, msg.worker_id)
        
        task.progress = msg.progress
        task.last_msg_received = "{}({})".format(msg.Type, msg.id)
        task.last_msg_received_at = time_util.get_current_utc_millis()
        self.scheduler.task_progress_changed_msgs_received += 1
                                
        self.job_db.update_job(job)
        logger.info("Job object updated: job_id='%s', job_name='%s'", job.id, job.full_name)        
    
#
#   Member message handlers
#
    def _handle_join_group_msg(self, msg):
        logger.info("Handling '%s' message: worker_id='%s' ...", msg.Type, msg.worker_id)

        current_time = time_util.get_current_utc_millis()
        worker = self.job_db.get_worker(msg.worker_id)
        exists = worker is not None
        if exists:
            logger.warning("Received '%s' message from an known worker: worker_id='%s', state='%s'. Rejoining it ...", 
                           msg.Type, worker.id, worker.state)
            # reset the executing task ids
            worker.executing_task_ids = []
        else:
            logger.info("Received '%s' message from an unknown worker: worker_id='%s'. Joining it ...", 
                        msg.Type, msg.worker_id)
            worker = Worker(msg.worker_id)
        
        worker.state = MemberState.JOINED
        worker.max_running_task_count = msg.max_running_task_count
        worker.last_heartbeat_sent_at = msg.timestamp
        worker.last_heartbeat_received_at = current_time
        worker.heartbeat_timeout = self.min_worker_heartbeat_timeout
        self._update_heartbeat_period(worker)
        worker.joined_count += 1
        self.scheduler.join_group_msgs_received += 1
        
        if exists:
            self.job_db.update_worker(worker)            
        else:
            self.job_db.create_worker(worker)            
        
        logger.info("Worker joined: worker_id='%s'", worker.id)
        self._rebalance_job_tasks()

    def _handle_group_left_msg(self, msg):
        logger.info("Handling '%s' message: worker_id='%s' ...", msg.Type, msg.worker_id)
        
        worker = self.job_db.get_worker(msg.worker_id)
        if worker is None:
            logger.warning("Received '%s' message from an unknown worker: worker_id='%s'. Ignored.", 
                           msg.Type, msg.worker_id)
            return
        
        worker.state = MemberState.LEFT
        worker.executing_task_ids = []
        worker.left_count += 1
        self.scheduler.group_left_msgs_received += 1
        
        self.job_db.update_worker(worker)            
                        
        logger.info("Worker left: worker_id='%s'", worker.id)
        self._rebalance_job_tasks()

    def _handle_heartbeat_msg(self, msg):
        '''
        Heartbeats can be received out of sync of task_started and task_ended messages.
        Never change the worker executing task ids here!
        '''
        
        logger.debug("Handling '%s' message: worker_id='%s', timestamp=%d ...", 
                     msg.Type, msg.worker_id, msg.timestamp)            
        
        worker = self.job_db.get_worker(msg.worker_id)
        exists = worker is not None
        if exists:
            if worker.state == MemberState.DEAD:
                logger.warning("Received '%s' message from a dead worker: worker_id='%s'. Rejoining it ...",
                               msg.Type, msg.worker_id)
                worker.resurrected_count += 1
                self.scheduler.resurrected_workers += 1
            elif worker.state == MemberState.LEFT:
                logger.warning("Received '%s' message from a disjoined worker: worker_id='%s'. Rejoining it ...",
                               msg.Type, msg.worker_id)                
        else:
            logger.warning("Received '%s' message from an unknown worker: worker_id='%s'. Joining it.",
                           msg.Type, msg.worker_id)
            worker = Worker(msg.worker_id)
            worker.last_heartbeat_received_at = time_util.get_current_utc_millis()
            worker.heartbeat_timeout = self.min_worker_heartbeat_timeout

        worker.state = MemberState.JOINED
        worker.max_running_task_count = msg.max_running_task_count
        worker.running_task_ids = msg.running_task_ids        
        worker.last_heartbeat_sent_at = msg.timestamp
        self._update_heartbeat_period(worker)
        
        worker.last_msg_received = msg.last_msg_received
        worker.last_msg_received_at = msg.last_msg_received_at
        worker.last_msg_sent = msg.last_msg_sent
        worker.last_msg_sent_at = msg.last_msg_sent_at
        
        worker.total_msgs_received = msg.total_msgs_received
        worker.min_msgs_received_per_min = msg.min_msgs_received_per_min
        worker.max_msgs_received_per_min = msg.max_msgs_received_per_min
        worker.avg_msgs_received_per_min = msg.avg_msgs_received_per_min
        
        worker.total_msgs_sent = msg.total_msgs_sent        
        worker.min_msgs_sent_per_min = msg.min_msgs_sent_per_min
        worker.max_msgs_sent_per_min = msg.max_msgs_sent_per_min
        worker.avg_msgs_sent_per_min = msg.avg_msgs_sent_per_min
        
        worker.process_cpu = msg.process_cpu
        worker.virtual_mem = msg.virtual_mem
        worker.swap_mem = msg.swap_mem
        worker.process_mem = msg.process_mem
        self.scheduler.heartbeat_msgs_received += 1

        if exists:
            self.job_db.update_worker(worker)
        else:
            self.job_db.create_worker(worker)

        current_time = time_util.get_current_utc_millis()
        if current_time - self.rebalanced_job_tasks_at >= self.rebalance_job_tasks_interval:
            self._rebalance_job_tasks()
            self.rebalanced_job_tasks_at = current_time

        if current_time - self.cleanedup_history_at >= self.cleanup_history_interval:
            results = self.job_db.delete_old_msgs(self.history_retention_period)
            logger.info("Deleted %d history messages older than '%d' days ...", 
                        results['deleted'], 
                        self.history_retention_period / (24 * 3600 * 1000))
            self.cleanedup_history_at = current_time

    def _update_heartbeat_period(self, worker):
        # set last_heartbeat_period and min/max/avg stats  
        worker.total_heartbeats_received += 1
        current_time = time_util.get_current_utc_millis()        
        worker.last_heartbeat_period = current_time - worker.last_heartbeat_received_at
        worker.last_heartbeat_received_at = current_time
        
        if worker.last_heartbeat_period < worker.min_heartbeat_period:
            worker.min_heartbeat_period = worker.last_heartbeat_period

        if worker.last_heartbeat_period > worker.max_heartbeat_period:
            worker.max_heartbeat_period = worker.last_heartbeat_period
             
        worker.avg_heartbeat_period += int(round(((worker.last_heartbeat_period - worker.avg_heartbeat_period) / 
                                                  float(worker.total_heartbeats_received))))

        # adjust the heartbeat timeout of this worker
        if worker.last_heartbeat_period > worker.heartbeat_timeout:
            worker.heartbeat_timeout = (worker.last_heartbeat_period * 
                                        SchedulerService.WORKER_HEARTBEAT_TIMEOUT_DURATION_RATIO)
        elif worker.last_heartbeat_period < self.min_worker_heartbeat_timeout:
            worker.heartbeat_timeout = self.min_worker_heartbeat_timeout

    def _handle_update_worker_msg(self, msg):
        logger.info("Handling '%s' message: ...", msg.Type)

        worker = self.job_db.get_worker(msg.worker_id)
        if worker.quiescent != msg.quiescent:
            worker.quiescent = msg.quiescent
            self.job_db.update_worker(worker)

            logger.info("Worker quiescent state changed to %s: worker_id='%s'",
                    msg.quiescent, msg.worker_id)
        else:
            logger.info("Worker quiescent state is already set to %s: worker_id='%s'",
                    msg.quiescent, msg.worker_id)        

    def _create_scheduler(self):
        logger.info("Creating scheduler object ...")
        self.scheduler.state = MemberState.JOINED
        self.job_db.create_scheduler(self.scheduler)            
        logger.info("Scheduler object created")

    def _update_scheduler(self):
        logger.info("Updating scheduler object ...")
        self.scheduler.total_msgs_received = self.msg_received_throughput.total_count
        self.scheduler.min_msgs_received_per_min = self.msg_received_throughput.min_count_per_interval
        self.scheduler.max_msgs_received_per_min = self.msg_received_throughput.max_count_per_interval
        self.scheduler.avg_msgs_received_per_min = int(round(self.msg_received_throughput.avg_count_per_interval))
        self.scheduler.total_msgs_sent = self.msg_sent_throughput.total_count
        self.scheduler.min_msgs_sent_per_min = self.msg_sent_throughput.min_count_per_interval
        self.scheduler.max_msgs_sent_per_min = self.msg_sent_throughput.max_count_per_interval
        self.scheduler.avg_msgs_sent_per_min = int(round(self.msg_sent_throughput.avg_count_per_interval))
        self.job_db.update_scheduler(self.scheduler)
        logger.info("Scheduler object updated")
#
#   Task message senders
#
    def _send_execute_task_msg(self, job, task):
        # Get the most free worker and ask it to execute the task
        workers = self.job_db.list_workers(MemberState.JOINED, quiescent=False)

        if not workers:
            logger.info("Cannot execute task, no workers: "
                        "job_id='%s', job_name='%s', task_id='%s', "
                        "method='%s', rev=%d ...",
                        job.id, job.full_name, task.id,
                        job.current_method, job.current_rev)
            return

        if task.is_mining_task():
            workers.sort(key=attrgetter('process_mem'))
            worker = workers[0]
            for worker in workers:
                if worker.executing_task_count < worker.max_running_task_count:
                    break
        else:
            worker = min(workers, key=attrgetter('executing_task_count'))

        if worker.executing_task_count >= worker.max_running_task_count:        
            logger.info("Cannot execute task, all workers running at maximum capacity: "
                        "job_id='%s', job_name='%s', task_id='%s', "
                        "method='%s', rev=%d ...",
                        job.id, job.full_name, task.id,
                        job.current_method, job.current_rev)
            return

        task.worker_id = worker.id
        msg = ExecuteTaskMsg(job.id, job.full_name, task.id, job.current_method, job.current_rev,
                             task.function, job.input, job.previous_input, task.worker_id)        
        logger.info("Sending '%s' message: job_id='%s', job_name='%s', task_id='%s', "
                    "method='%s', rev=%d, worker_id='%s' ...",
                    msg.Type, msg.job_id, msg.job_name, msg.task_id,
                    msg.method, msg.rev, msg.worker_id)
        self.job_db.write_msg(msg)
        self.mb_producer.send(self.workers_messages_topic_name, msg.encode())
        task.last_msg_sent = "{}({})".format(msg.Type, msg.id)           
        task.last_msg_sent_at = time_util.get_current_utc_millis()            
        task.execute_req_count += 1
        self.scheduler.execute_task_msgs_sent += 1
        self.msg_sent_throughput.increase()

        # Change task state to ready
        task.current_state = TaskState.READY        
        self.job_db.update_job(job)
        logger.info("Job object updated: job_id='%s', job_name='%s'", 
                    job.id, job.full_name)        

        # Add this task to the tasks executing on worker
        # If we do not do it here, the worker could execute more tasks than 'max_running_task_count' 
        worker.add_executing_task_id(task.full_id) 
        self.job_db.update_worker(worker)
        logger.info("Worker object updated: worker_id='%s'", worker.id)                

    def _send_stop_task_msg(self, job, task):
        # Ask a specific worker to stop the execution of a task 
        msg = StopTaskMsg(job.id, job.full_name, task.id, job.current_method, job.current_rev, task.worker_id)
        logger.info("Sending '%s' message: job_id='%s', job_name='%s', task_id='%s', "
                    "method='%s', rev=%d, worker_id='%s' ...", 
                    msg.Type, msg.job_id, job.full_name, msg.task_id, 
                    msg.method, msg.rev, msg.worker_id)
        self.job_db.write_msg(msg)
        self.mb_producer.send(self.workers_messages_topic_name, msg.encode())
        task.last_msg_sent = "{}({})".format(msg.Type, msg.id)           
        task.last_msg_sent_at = time_util.get_current_utc_millis()            
        task.stop_req_count += 1
        self.scheduler.stop_task_msgs_sent += 1
        self.msg_sent_throughput.increase()

#
#   Job processing methods
#
    def _coordinate_job_tasks(self, job):
        logger.info("Coordinating job tasks: job_id='%s', job_name='%s', method='%s', rev=%d ...",
                    job.id, job.full_name, 
                    job.current_method, job.current_rev)
        
        job_state_modified = False
                
        # 1st step: handle current/desired method changes        
        if ((job.current_method != job.desired_method) 
            or (job.current_method == TaskMethod.UPDATE 
                and job.current_rev != job.desired_rev)):            
            logger.info("Job method/rev changed: job_id='%s', job_name='%s', method='%s'->'%s', rev=%d->%d",
                        job.id, job.full_name, 
                        job.current_method, job.desired_method,
                        job.current_rev, job.desired_rev)
            # We can change method only when there are no executing tasks
            executing_tasks = job.get_tasks_by_state(TaskState.READY, TaskState.RUNNING)
            if executing_tasks:
                job.stop_requested = True
                job_state_modified = True                
                executing_task_ids = [t.id for t in executing_tasks]
                logger.info("Job method/rev changed and executing tasks detected, stop requested: "
                            "job_id='%s', job_name='%s', method='%s'->'%s', rev=%d->%d, "
                            "executing_task_ids=%s",
                            job.id, job.full_name, 
                            job.current_method, job.desired_method,
                            job.current_rev, job.desired_rev,
                            executing_task_ids)
            else:
                job.stop_requested = False
                job.current_method = job.desired_method
                job.current_rev = job.desired_rev
                self._delete_tasks_marked_for_deletion(job)
                job.reset()
                job_state_modified = True
                logger.info("Job method/rev changed and no executing tasks detected, job reset: "
                            "job_id='%s', job_name='%s', method='%s'->'%s', rev=%d->%d",
                            job.id, job.full_name, 
                            job.current_method, job.desired_method,
                            job.current_rev, job.desired_rev)

        # 2nd step: handle execute/stop changes
        if job.stop_requested:
            for task in job.get_tasks():
                if task.current_state in [TaskState.WAITING, TaskState.FAILED]:
                    task.current_state = TaskState.STOPPED
                    task.worker_id = None
                    job_state_modified = True        
                elif task.current_state in [TaskState.READY, TaskState.RUNNING]:
                    self._send_stop_task_msg(job, task)
                    job_state_modified = True        
        else:
            for task in job.get_tasks():
                if task.current_state in [TaskState.WAITING, TaskState.STOPPED]:
                    if task.can_execute(job):
                        self._send_execute_task_msg(job, task)
                        job_state_modified = True
                elif task.current_state == TaskState.FAILED:
                    if task.retry_count < task.max_retry_count:
                        task.retry_count += 1
                        logger.info("Retrying failed task: job_id='%s', job_name='%s', task_id='%s', "
                                    "method='%s', rev=%d, retry_number=%d, max_retry_number=%d",
                                    job.id, job.full_name, task.id, job.current_method, job.current_rev, 
                                    task.retry_count, task.max_retry_count)
                        self._send_execute_task_msg(job, task)
                        job_state_modified = True
                    else:
                        logger.info("Max retry count reached for failed task: job_id='%s', job_name='%s', "
                                    "task_id='%s', method='%s', rev=%d, retry_count=%d, max_retry_count=%d", 
                                    job.id, job.full_name, task.id, job.current_method, job.current_rev,
                                    task.retry_count, task.max_retry_count)
                        # If one task is permanently failed, then the job is completed
                        task.current_state = TaskState.PERMANENTLY_FAILED
                        job_state_modified = True
        
        if job_state_modified:
            self.job_db.update_job(job)
            logger.info("Job object updated: job_id='%s', job_name='%s'", 
                        job.id, job.full_name)
            
        if job.completed:
            logger.info("Job completed: job_id='%s', job_name='%s'", job.id, job.name)
            if job.current_method == TaskMethod.DELETE:                
                logger.info("Deleting job: id='%s', name='%s' ...", job.id, job.full_name)
                self.job_db.delete_job(job.id)
            self.scheduler.completed_jobs += 1
            if job.current_state == TaskState.SUCCESSFUL:
                self.scheduler.successful_jobs += 1
            elif job.current_state == TaskState.FAILED:
                self.scheduler.failed_jobs += 1
            elif job.current_state == TaskState.PERMANENTLY_FAILED:
                self.scheduler.permanently_failed_jobs += 1            
            self.notify_completion(job)
 
        self.scheduler.coordinate_job_tasks_runs += 1
        logger.info("Completed coordinating job tasks: job_id='%s', job_name='%s', method='%s', rev=%d ...",
                    job.id, job.full_name, 
                    job.current_method, job.current_rev)

    def _rebalance_job_tasks(self):
        logger.debug("Rebalancing all job tasks ...")

        current_time = time_util.get_current_utc_millis()        
        incompleted_jobs = self.job_db.list_jobs({"completed": "false"})
        workers = self.job_db.list_workers(MemberState.JOINED, quiescent=False)

        # Determine the dead workers and get the alive workers
        alive_worker_ids = []
        for worker in workers:
            heartbeat_period = current_time - worker.last_heartbeat_received_at
            if heartbeat_period > worker.heartbeat_timeout:
                logger.info("Detected dead worker: worker_id='%s', heartbeat_period=%d, heartbeat_timeout=%d", 
                            worker.id, heartbeat_period, worker.heartbeat_timeout)
                worker.state = MemberState.DEAD
                worker.executing_task_ids = []                
                worker.died_count += 1
                self.job_db.update_worker(worker)
                logger.info("Worker object updated: worker_id='%s'", worker.id)        
                self.scheduler.dead_workers += 1
            else:
                alive_worker_ids.append(worker.id)

        # Get all the tasks executing on workers
        executing_task_ids = []
        for worker in workers:
            executing_task_ids.extend(worker.executing_task_ids)

        # Restart all tasks that are blocked for one reason or another
        for job in incompleted_jobs:
            job_state_modified = False                                
            for task in job.get_tasks():
                if task.completed:
                    continue

                if task.current_state in [TaskState.WAITING]:
                    logger.info("Detected task waiting too long, current state reset: "
                                "job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d", 
                                job.id, job.full_name, task.id, job.current_method, job.current_rev)
                    task.reset()
                    job_state_modified = True
                    self.scheduler.tasks_waiting_too_long += 1
                elif task.current_state in [TaskState.READY, TaskState.RUNNING, TaskState.FAILED] and task.worker_id not in alive_worker_ids: 
                    logger.info("Detected task assigned to a dead worker, current state reset: "
                                "job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d, worker_id='%s'", 
                                job.id, job.full_name, task.id, job.current_method, job.current_rev, task.worker_id)
                    if job.stop_requested:
                        # If task is dead and stop was requested, the task is STOPPED
                        task.current_state = TaskState.STOPPED
                        task.worker_id = None
                        job_state_modified = True
                    else:        
                        # If task is dead and stop not was requested, the task is WAITING
                        task.reset()
                        job_state_modified = True
                    self.scheduler.tasks_assigned_to_dead_worker += 1
                elif task.current_state in [TaskState.READY, TaskState.RUNNING] and task.full_id not in executing_task_ids:
                    logger.info("Detected missing running task in worker, current state reset: "
                                "job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d, worker_id='%s'", 
                                job.id, job.full_name, task.id, job.current_method, job.current_rev, task.worker_id)
                    task.reset()
                    job_state_modified = True
                    self.scheduler.missing_running_tasks_in_worker += 1

            if job_state_modified:                                
                self.job_db.update_job(job)
                logger.info("Job object updated: job_id='%s', job_name='%s'", job.id, job.full_name)        
                self._coordinate_job_tasks(job)

        self.scheduler.rebalance_job_tasks_runs += 1
        logger.debug("Completed rebalancing all job tasks.")

    def _delete_tasks_marked_for_deletion(self, job):
        for task in job.get_tasks():
            if task.marked_for_deletion:
                job.del_task(task.id)
        
    def notify_completion(self, job):
        if job.current_method == TaskMethod.DELETE:
            return
        
        try:
            job.channels.notify_job_completion(job)
        except:
            logger.exception("Unexpected exception while notifying job completion")
