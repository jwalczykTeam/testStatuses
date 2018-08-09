'''
Created on Aug 23, 2016

@author: alberto
'''


from doi import constants 
from doi.common import TaskParams
from doi.scheduler.common import JoinGroupMsg, LeaveGroupMsg
from doi.scheduler.common import TaskMsg, ExecuteTaskMsg
from doi.scheduler.common import TaskStartedMsg, StopTaskMsg, TaskEndedMsg
from doi.worker.executor import TaskExecutor
from doi.worker.heartbeat_sender import HeartbeatSender
from doi.util import log_util
from doi.util.log_util import mask_sensitive_info
from doi.util import stat_util
from doi.util import time_util
from doi.util import timer_util
from doi.util.mb import mb_util
import threading
import time
import newrelic.agent
#from mem_top import mem_top
#from pympler.tracker import SummaryTracker


logger = log_util.get_logger("astro.worker.worker")
application = newrelic.agent.register_application(timeout=10.0)

class WorkerService(object):
    # Designed not to raise exceptions
    def __init__(self, config):
        super(WorkerService, self).__init__()
        logger.info("Initializing worker: %s ...", mask_sensitive_info(config))
        self.id = config['id']
        self.group_id = config['group_id']
        self.error_retry_interval = config['error_retry_interval']
        self.error_notification_interval = config['error_notification_interval']
        self.slack_webhook_url = config['slack_webhook_url']                
        self.max_running_task_count = config['max_running_tasks']
        self.heartbeat_interval = config['heartbeat_interval']
        self.scheduler_messages_topic_name = constants.get_scheduler_messages_topic_name(self.group_id)      
        self.workers_messages_topic_name = constants.get_workers_messages_topic_name(self.group_id)
        self.resource_db_events_topic_name = constants.get_resource_db_events_topic_name(self.group_id)                      
        self.mb_config = config["mb"]
        
        logger.info("worker_id='%s'", self.id)        
        logger.info("group_id='%s'", self.group_id)        
        logger.info("error_retry_interval=%d", self.error_retry_interval)
        logger.info("error_notification_interval=%d", self.error_notification_interval)
        logger.info("slack_webhook_url=%s", "present" if self.slack_webhook_url else "absent")
        logger.info("max_running_tasks=%d", self.max_running_task_count)        
        logger.info("heartbeat_interval=%d", self.heartbeat_interval)        
        logger.info("scheduler_messages_topic_name='%s'", self.scheduler_messages_topic_name)        
        logger.info("workers_messages_topic_name='%s'", self.workers_messages_topic_name) 
        logger.info("resource_db_events_topic_name=%s", self.resource_db_events_topic_name)                        

        self.mb_producer = None
        self.mb_consumer = None
        self.heartbeat_sender = None
        self.task_executors = []
        self.task_executors_lock = threading.Lock()
        self.last_msg_received = None
        self.last_msg_received_at = None
        self.last_msg_sent = None
        self.last_msg_sent_at = None
        self.msg_received_throughput = stat_util.Throughput(60)
        self.msg_sent_throughput = stat_util.Throughput(60)

    def open(self):
        # Enable notifications
        logger.enable_slack_notification(self.slack_webhook_url)

        # Initializer message broker and message consumer/producer
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
        
        mb_client_id = constants.get_worker_client_id(self.group_id, self.id)
        mb_group_id = constants.get_worker_group_id(self.group_id, self.id)
        logger.info("mb_client_id='%s'", mb_client_id)        
        logger.info("mb_group_id='%s'", mb_group_id)        

        self.mb_producer = mb.new_producer(mb_client_id)
        self.mb_consumer = mb.new_consumer(mb_client_id, mb_group_id)
        logger.info("Worker initialized: group_id='%s', worker_id='%s'", self.group_id, self.id)                              
                
    def close(self):
        logger.info("Closing worker: group_id='%s', worker_id='%s'", 
                    self.group_id, self.id)

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

        logger.info("Worker closed: group_id='%s', worker_id='%s'", 
                    self.group_id, self.id)        

    @staticmethod
    def start(config):
        worker = WorkerService(config)
        timer = timer_util.Timer(worker.error_notification_interval)
        
        while True:
            try:
                worker.open()
                logger.info("Astro worker successfully started: group_id='%s', worker_id='%s'", 
                            worker.group_id, worker.id, notify=True)
                timer.reset()                
                worker.run()
            except KeyboardInterrupt:
                logger.info("Astro worker interrupted. Gracefully exiting: group_id='%s', worker_id='%s'",
                            worker.group_id, worker.id, notify=True)                
                break                
            except:
                timer.activate()
                logger.exception("Unexpected exception in astro worker: group_id='%s', worker_id='%s'", 
                                 worker.group_id, worker.id, notify=timer.active)
            finally:
                worker.close()            
                logger.info("Astro worker stopped: group_id='%s', worker_id='%s'", 
                            worker.group_id, worker.id, notify=timer.active)
    
            logger.info("Restarting astro worker in %d seconds: group_id='%s', worker_id='%s'", 
                        worker.error_retry_interval, worker.group_id, worker.id, notify=timer.active)
            time.sleep(worker.error_retry_interval)    

    def run(self):
        try: 
            self._send_join_group_msg()
            self._start_heartbeat_sender()
            self.mb_consumer.subscribe([self.workers_messages_topic_name])        
            logger.info("Subscribed to workers messages topic: %s", self.workers_messages_topic_name)
            logger.info("Waiting for next message ...")

            for msg in self.mb_consumer.messages():
                try:
                    if 'task_msg' in msg.value:
                        #logger.debug("Task message received: %s", msg.value['task_msg'])
                        task_msg = TaskMsg.factory(msg.value)
                        if task_msg.Type == ExecuteTaskMsg.Type:
                            self._handle_execute_task_msg(task_msg)
                        elif task_msg.Type == StopTaskMsg.Type:
                            self._handle_stop_task_msg(task_msg)
                        else:
                            logger.warning("Unknown task message received: %s", msg.value)
                                                        
                        self.mb_consumer.commit(msg)
                    else:
                        logger.warning("Unknown message received: %s", msg.value)
                except:
                    logger.exception("Unexpected exception while processing message: %s", msg.value)                
          
                self.msg_received_throughput.increase()

                logger.info("Waiting for next message ...")

        finally:
            logger.debug("DONE - _stop_heartbeat_sender & _send_leave_group_msg...")
            self._stop_heartbeat_sender()
            self._send_leave_group_msg()

#
#    Member message senders
#
    @newrelic.agent.background_task()
    def _send_join_group_msg(self):
        current_time = time_util.get_current_utc_millis()            
        msg = JoinGroupMsg(self.id, current_time)
        logger.info("Sending '%s' message: worker_id='%s' ...", msg.Type, msg.worker_id)
        msg.max_running_task_count = self.max_running_task_count
        self.mb_producer.send(self.scheduler_messages_topic_name, msg.encode())
        self.last_msg_sent = msg.Type           
        self.last_msg_sent_at = current_time            
        self.msg_sent_throughput.increase()

    @newrelic.agent.background_task()
    def _send_leave_group_msg(self):
        current_time = time_util.get_current_utc_millis()            
        msg = LeaveGroupMsg(self.id, current_time)
        logger.info("Sending '%s' message: worker_id='%s' ...", msg.Type, msg.worker_id)
        self.mb_producer.send(self.scheduler_messages_topic_name, msg.encode())
        self.last_msg_sent = msg.Type           
        self.last_msg_sent_at = current_time            
        self.msg_sent_throughput.increase()

#
#    Task message handlers
#
    @newrelic.agent.background_task() 
    def _handle_execute_task_msg(self, msg):
        with self.task_executors_lock:
            logger.info("Handling '%s' message: job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d, worker_id='%s' ...", 
                        msg.Type, msg.job_id, msg.job_name, msg.task_id, msg.method, msg.rev, msg.worker_id)
           
            # if message is not for this worker, ignore it,
            if msg.worker_id != self.id:
                logger.info("Received '%s' message addressed to different worker: job_id='%s', job_name='%s', task_id='%s', "
                            "method='%s', rev=%d, worker_id='%s'. Ignored.",
                            msg.Type, msg.job_id, msg.job_name, msg.task_id, msg.method, msg.rev, msg.worker_id)
                return
            
            # if job is already running (it could be a previous method or rev), ignore the message
            task_executor = self._find_task_executor(msg.job_id, msg.task_id)
            if task_executor is not None:
                logger.info("Received '%s' message for a task already running: job_id='%s', job_name='%s', task_id='%s', "
                            "method='%s', rev=%d, worker_id='%s'. Ignored.", 
                            msg.Type, msg.job_id, msg.job_name, msg.task_id, msg.method, msg.rev, msg.worker_id)
                return
                
            self.last_msg_received = "{}({})".format(msg.Type, msg.id)            
            self.last_msg_received_at = time_util.get_current_utc_millis()            
            logger.info("Starting task: job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d, worker_id='%s' ...", 
                        msg.job_id, msg.job_name, msg.task_id, msg.method, msg.rev, msg.worker_id)
            task_params = TaskParams(msg.job_id, 
                                     msg.job_name, 
                                     msg.task_id, 
                                     msg.method, 
                                     msg.rev,
                                     msg.input,
                                     msg.previous_input)
            self._start_task(msg.function, task_params)

    @newrelic.agent.background_task()
    def _handle_stop_task_msg(self, msg):
        with self.task_executors_lock:
            logger.info("Handling '%s' message: job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d, worker_id='%s' ...", 
                        msg.Type, msg.job_id, msg.job_name, msg.task_id, msg.method, msg.rev, msg.worker_id)
            
            # if message is not for this worker, ignore it,
            if msg.worker_id != self.id:
                logger.info("Received '%s' message addressed to different worker: job_id='%s', job_name='%s', task_id='%s', "
                            "method='%s', rev=%d, worker_id='%s'. Ignored.",
                            msg.Type, msg.job_id, msg.job_name, msg.task_id, msg.method, msg.rev, msg.worker_id)
                return

            task_executor = self._find_task_executor(msg.job_id, msg.task_id)
            if task_executor is None:
                logger.info("Received '%s' message for unknown task: job_id='%s', job_name='%s', task_id='%s', rev=%d, worker_id='%s'. Ignored.", 
                            msg.Type, msg.job_id, msg.job_name, msg.task_id, msg.rev, msg.worker_id)
                return
            
            self.last_msg_received = "{}({})".format(msg.Type, msg.id)            
            self.last_msg_received_at = time_util.get_current_utc_millis()            
            logger.info("Sending stop signal to task thread: job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d, worker_id='%s' ...", 
                        msg.job_id, msg.job_name, msg.task_id, msg.method, msg.rev, msg.worker_id)
            task_executor.stop_event.set()

    @newrelic.agent.background_task()
    def _start_heartbeat_sender(self):
        logger.info("Starting heartbeat sender ...")
        self.heartbeat_sender = HeartbeatSender(self)
        self.heartbeat_sender.start()

    @newrelic.agent.background_task()
    def _stop_heartbeat_sender(self):
        if self.heartbeat_sender is not None:
            logger.info("Stopping heartbeat sender ...")
            try:
                self.heartbeat_sender.stop()
            except:
                logger.exception("Unexpected exception while stopping heartbeat sender.")

    @newrelic.agent.background_task()
    def _start_task(self, function, task_params):
        task_executor = TaskExecutor(self, function, task_params)
        task_executor.start()
        self.task_executors.append(task_executor)
        self._send_task_started_msg(task_executor)
    
    @newrelic.agent.background_task()
    def _task_ended(self, task_executor):
        with self.task_executors_lock:

            logger.info("Before thread pop, Running threads {}".format(
                threading.enumerate()))
            logger.info("Before thread pop, Thread count {}".format(
                threading.active_count()))
            logger.info("Task ended: job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d, state='%s'", 
                        task_executor.job_id, task_executor.job_name, task_executor.task_id, 
                        task_executor.method, task_executor.rev, task_executor.state)
            self.task_executors.remove(task_executor)
            logger.info("Task ended, Running threads {}".format(
                threading.enumerate()))
            logger.info("Task ended, Thread count {}".format(
                threading.active_count()))
            self._send_task_ended_msg(task_executor)

    @newrelic.agent.background_task()
    def _find_task_executor(self, job_id, task_id):
        for task_executor in self.task_executors:
            if task_executor.job_id == job_id and task_executor.task_id == task_id:
                return task_executor
        
        return None

    @newrelic.agent.background_task()
    def _send_task_started_msg(self, executor):
        msg = TaskStartedMsg(executor.job_id, executor.job_name, executor.task_id, 
                             executor.method, executor.rev, 
                             self.id, time_util.get_current_utc_millis())        
        logger.info("Sending '%s' message: job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d ...",
                    msg.Type, msg.job_id, msg.job_name, msg.task_id, msg.method, msg.rev)
        self.mb_producer.send(self.scheduler_messages_topic_name, msg.encode())
        self.last_msg_sent = "{}({})".format(msg.Type, msg.id)            
        self.last_msg_sent_at = time_util.get_current_utc_millis()            
        self.msg_sent_throughput.increase()
        

    @newrelic.agent.background_task()
    def _send_task_ended_msg(self, executor):
        msg = TaskEndedMsg(executor.job_id, executor.job_name, 
                           executor.task_id, executor.method, executor.rev,
                           executor.state, executor.output, executor.error, 
                           self.id, time_util.get_current_utc_millis())        
        logger.info("Sending '%s' message: job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d, state='%s' ...", 
                    msg.Type, msg.job_id, msg.job_name, msg.task_id, msg.method, msg.rev, msg.state)
        self.mb_producer.send(self.scheduler_messages_topic_name, msg.encode())
        self.last_msg_sent = "{}({})".format(msg.Type, msg.id)            
        self.last_msg_sent_at = time_util.get_current_utc_millis()            
        self.msg_sent_throughput.increase()
