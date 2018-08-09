'''
Created on Sep 6, 2016

@author: alberto
'''

from doi import constants
from doi.common import Resource, TaskState, Error
from doi.scheduler.channel import Channels
from doi.util import time_util

import sys


#
#    Job Classes
#
  
class Job(Resource):
    def __init__(self, id_=None):
        super(Job, self).__init__(constants.JOB_TYPE, id_)
        self.schema_rev = 1
        self.created_at = None
        self.last_updated_at = None
        self.created_by = None
        self.last_updated_by = None
        self.url = None
        self.job_type = None
        self.name = None
        self.full_name = None
        self.title = None
        self.description = None
        self.html_url = None
        self.tenant_id = None
        self.input = None
        self.previous_input = None
        self.current_method = None
        self.desired_method = None
        self.current_rev = None
        self.desired_rev = None
        self.stop_requested = False
        self.started_at = None
        self.ended_at = None
        self.tasks = {}
        self.channels = Channels()
        
    def add_task(self, task):
        self.tasks[task.id] = task
        task.job = self
        
    def get_tasks(self):
        return self.tasks.values()
        
    def get_task(self, task_id):
        return self.tasks[task_id]

    def del_task(self, task_id):
        del self.tasks[task_id]

    def has_task(self, task_id):
        return task_id in self.tasks

    def get_tasks_by_state(self, *states):
        return [t for t in self.get_tasks() if t.current_state in states]
            
    def reset(self):
        self.started_at = None
        self.ended_at = None
        for task in self.get_tasks():
            task.reset()

    def add_channel(self, channel):
        self.channels.add_channel(channel)

    @property                
    def current_state(self):
        states = { t.current_state for t in self.get_tasks() }
        
        if TaskState.SUCCESSFUL in states and len(states) == 1:
            return TaskState.SUCCESSFUL  
        elif TaskState.PERMANENTLY_FAILED in states:
            return TaskState.PERMANENTLY_FAILED  
        elif TaskState.FAILED in states:
            return TaskState.RUNNING
        elif TaskState.RUNNING in states:
            return TaskState.RUNNING
        elif TaskState.STOPPED in states:
            return TaskState.STOPPED
        elif TaskState.READY in states:
            return TaskState.READY
        else:
            return TaskState.WAITING
            
    @property                
    def completed(self):
        # A job if completed if one or more tasks are permanently failed or all tasks are successful 
        return (self.current_state == TaskState.SUCCESSFUL or 
                self.current_state == TaskState.PERMANENTLY_FAILED)
    
    @property                
    def elapsed_time(self):
        if self.started_at is None:
            return None
        
        ended_at = self.ended_at if self.ended_at is not None else time_util.get_current_utc_millis()            
        return ended_at - self.started_at

    @property                
    def running_time(self):
        job_running_time = 0
        for task in self.get_tasks():
            task_running_time = task.running_time
            if task_running_time is not None:
                job_running_time += task_running_time
                
        return job_running_time
                
    @staticmethod
    def decode(obj):
        job = Job(obj["id"])
        job.schema_rev = obj.get("schema_rev", 1)
        job.job_type = obj.get("job_type")
        job.created_at = obj.get("created_at")
        job.last_updated_at = obj.get("last_updated_at")
        job.created_by = obj.get("created_by")
        job.last_updated_by = obj.get("last_updated_by")
        job.name = obj.get("name")
        job.full_name = obj.get("full_name")
        job.title = obj.get("title")
        job.description = obj.get("description")
        job.html_url = obj.get("html_url")
        job.tenant_id = obj.get("tenant_id")
        job.input = obj.get("input")
        job.previous_input = obj.get("previous_input")
        job.current_method = obj.get("current_method")
        job.desired_method = obj.get("desired_method")
        job.current_rev = obj.get("current_rev")
        job.desired_rev = obj.get("desired_rev")
        job.stop_requested = obj.get("stop_requested", False)
        job.started_at = obj.get("started_at")
        job.ended_at = obj.get("ended_at")
        for task in obj["tasks"]: job.add_task(Task.decode(task))        
        job.channels.decode(obj.get("channels"))
        return job

    def encode(self):
        return {
            "type": self.type,
            "id": self.id,
            "schema_rev": self.schema_rev,
            "created_at": self.created_at,
            "last_updated_at": self.last_updated_at,
            "created_by": self.created_by,
            "last_updated_by": self.last_updated_by,
            "url": self.url,
            "job_type": self.job_type,
            "name": self.name,
            "full_name": self.full_name,
            "title": self.title,
            "description": self.description,
            "html_url": self.html_url,
            "tenant_id": self.tenant_id,
            "input": self.input,
            "previous_input": self.previous_input,
            "current_method": self.current_method,
            "desired_method": self.desired_method,
            "current_rev": self.current_rev,
            "desired_rev": self.desired_rev,
            "current_state": self.current_state,
            "stop_requested": self.stop_requested,
            "completed": self.completed,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "tasks": [t.encode() for t in self.get_tasks()],
            "channels": self.channels.encode()
        }


class Task(object):
    def __init__(self, id_, function, condition=None):
        super(Task, self).__init__()
        self.id = id_
        self.function = function
        self.condition = condition
        self.job = None
        self.current_state = TaskState.WAITING
        self.progress = None
        self.output = None
        self.marked_for_deletion = False
        self.execute_req_count = 0
        self.stop_req_count = 0
        self.started_count = 0
        self.ended_count = 0
        self.successful_count = 0
        self.failed_count = 0
        self.permanently_failed_count = 0
        self.stopped_count = 0
        self.last_error = None
        self.retry_count = 0       
        self.max_retry_count = constants.MAX_RETRY_NUMBER       
        self.worker_id = None        
        self.last_msg_sent = None
        self.last_msg_sent_at = None
        self.last_msg_received = None
        self.last_msg_received_at = None
        self.started_at = None
        self.ended_at = None

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.id == other.id
        else:
            return False    

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return self.id != other.id
        else:
            return True

    def __hash__(self):
        return hash(self.id)
    
    def reset(self):        
        self.current_state = TaskState.WAITING
        self.progress = None
        self.output = None
        self.last_error = None
        self.retry_count = 0
        self.max_retry_count = constants.MAX_RETRY_NUMBER       
        self.worker_id = None
        self.last_msg_sent = None
        self.last_msg_sent_at = None
        self.last_msg_received = None
        self.last_msg_received_at = None
        self.started_at = None
        self.ended_at = None

    def can_execute(self, job):
        return True if self.condition is None else self.condition.evaluate(job)

    def is_mining_task(self):
        # allow only one github mine per worker
        mine_commits = ['mine', 'commits']
        methods = ['create', 'update']

        if (all(m in self.id for m in mine_commits) and
                self.job.current_method in methods):
            return True
        else:
            return False

    @property
    def full_id(self):
        return self.job.current_method + '/' + self.job.id + '/' + self.id + '/' + str(self.job.current_rev)
    
    @property
    def completed(self):
        # A task if completed if it is successful or permanently failed        
        return (self.current_state == TaskState.SUCCESSFUL or 
                self.current_state == TaskState.PERMANENTLY_FAILED)

    @property                
    def running_time(self):
        if self.started_at is None:
            return None
        
        ended_at = self.ended_at if self.ended_at is not None else time_util.get_current_utc_millis()        
        return ended_at - self.started_at

    @staticmethod
    def decode(obj):
        task = Task(
            obj["id"],
            obj["function"],
            Condition.decode(obj.get("condition")))
        task.current_state = obj.get("current_state", TaskState.WAITING)
        task.progress = obj.get("progress")
        task.output = obj.get("output")
        task.marked_for_deletion = obj.get("marked_for_deletion", False)
        task.execute_req_count = obj.get("execute_req_count", 0)       
        task.stop_req_count = obj.get("stop_req_count", 0)       
        task.started_count = obj.get("started_count", 0)       
        task.ended_count = obj.get("ended_count", 0)       
        task.successful_count = obj.get("successful_count", 0)       
        task.failed_count = obj.get("failed_count", 0)       
        task.permanently_failed_count = obj.get("permanently_failed_count", 0)       
        task.stopped_count = obj.get("stopped_count", 0)               
        task.last_error = Error.decode(obj.get("last_error"))        
        task.retry_count = obj.get("retry_count", 0)       
        task.max_retry_count = obj.get("max_retry_count", constants.MAX_RETRY_NUMBER)       
        task.worker_id = obj.get("worker_id")
        task.last_msg_sent = obj.get("last_msg_sent")                                       
        task.last_msg_sent_at = obj.get("last_msg_sent_at")                                       
        task.last_msg_received = obj.get("last_msg_received")                                       
        task.last_msg_received_at = obj.get("last_msg_received_at")                                       
        task.started_at = obj.get('started_at')
        task.ended_at = obj.get('ended_at')
        return task                

    def encode(self):
        return {
            "id": self.id,
            "function": self.function,
            "condition": None if self.condition is None else self.condition.encode(),
            "completed": self.completed,            
            "current_state": self.current_state,
            "progress": self.progress,
            "output": self.output,
            "marked_for_deletion": self.marked_for_deletion,
            "execute_req_count": self.execute_req_count,
            "stop_req_count": self.stop_req_count,
            "started_count": self.started_count,
            "ended_count": self.ended_count,
            "successful_count": self.successful_count,
            "failed_count": self.failed_count,
            "permanently_failed_count": self.permanently_failed_count,
            "stopped_count": self.stopped_count,
            "last_error": None if self.last_error is None else self.last_error.encode(),            
            "retry_count": self.retry_count,
            "max_retry_count": self.max_retry_count,
            "worker_id": self.worker_id,            
            "last_msg_sent": self.last_msg_sent,                        
            "last_msg_sent_at": self.last_msg_sent_at,                        
            "last_msg_received": self.last_msg_received,                        
            "last_msg_received_at": self.last_msg_received_at,                        
            "started_at": self.started_at,
            "ended_at": self.ended_at
        }


class Condition(object):
    def __init__(self, task_ids):
        super(Condition, self).__init__()
        self.task_ids = task_ids
    
    def evaluate(self, job):
        for task_id in self.task_ids:
            task = job.get_task(task_id)
            if task.current_state != TaskState.SUCCESSFUL:
                return False
            
        return True

    @staticmethod
    def decode(obj):
        if obj is None:
            return None
        
        return Condition(obj["depends_on_tasks"])
    
    def encode(self):
        return {
            "depends_on_tasks": self.task_ids
        }


#
#    Worker/Scheduler Classes
#

class Worker(Resource):
    def __init__(self, id_):
        super(Worker, self).__init__(constants.WORKER_TYPE, id_)
        self.schema_rev = 1
        self.created_at = None
        self.last_updated_at = None
        self.url = None
        self.state = None
        self.quiescent = False
        self.joined_count = 0
        self.left_count = 0
        self.died_count = 0
        self.resurrected_count = 0
        self.last_heartbeat_sent_at = None
        self.last_heartbeat_received_at = None
        self.last_heartbeat_period = 0
        self.heartbeat_timeout = 0
        self.last_msg_received = None
        self.last_msg_received_at = None
        self.last_msg_sent = None
        self.last_msg_sent_at = None
        self.executing_task_ids = []
        self.running_task_ids = []
        self.max_running_task_count = constants.MAX_RUNNING_TASK_COUNT
        self.total_heartbeats_received = 0
        self.min_heartbeat_period = sys.maxint
        self.max_heartbeat_period = 0
        self.avg_heartbeat_period = 0
        self.total_msgs_received = 0
        self.min_msgs_received_per_min = 0
        self.max_msgs_received_per_min = 0
        self.avg_msgs_received_per_min = 0
        self.total_msgs_sent = 0
        self.min_msgs_sent_per_min = 0
        self.max_msgs_sent_per_min = 0
        self.avg_msgs_sent_per_min = 0
        self.process_cpu = 0
        self.virtual_mem = 0
        self.swap_mem = 0
        self.process_mem = 0
        
    @property
    def executing_task_count(self):
        return len(self.executing_task_ids)

    def add_executing_task_id(self, task_id):
        if task_id not in self.executing_task_ids:
            self.executing_task_ids.append(task_id) 
    
    def remove_executing_task_id(self, task_id):
        if task_id in self.executing_task_ids:
            self.executing_task_ids.remove(task_id) 
    
    @property
    def running_task_count(self):
        return len(self.running_task_ids)
    
    @staticmethod
    def decode(obj):
        worker = Worker(obj["id"])
        worker.schema_rev = obj.get("schema_rev", 1)
        worker.created_at = obj.get("created_at")
        worker.last_updated_at = obj.get("last_updated_at")
        worker.state = obj.get("state")
        worker.quiescent = obj.get("quiescent", False)
        worker.joined_count = obj.get("joined_count", 0)
        worker.left_count = obj.get("left_count", 0)
        worker.died_count = obj.get("died_count", 0)
        worker.resurrected_count = obj.get("resurrected_count", 0)
        worker.last_heartbeat_sent_at = obj.get("last_heartbeat_sent_at")
        worker.last_heartbeat_received_at = obj.get("last_heartbeat_received_at")
        worker.last_heartbeat_period = obj.get("last_heartbeat_period")
        worker.heartbeat_timeout = obj.get("heartbeat_timeout", 0)
        worker.last_msg_received = obj.get("last_msg_received", 0)
        worker.last_msg_received_at = obj.get("last_msg_received_at")
        worker.last_msg_sent = obj.get("last_msg_sent")
        worker.last_msg_sent_at = obj.get("last_msg_sent_at")
        worker.executing_task_ids = obj.get("executing_task_ids", [])
        worker.running_task_ids = obj.get("running_task_ids", [])
        worker.max_running_task_count = obj.get("max_running_task_count", constants.MAX_RUNNING_TASK_COUNT)
        worker.total_heartbeats_received = obj.get("total_heartbeats_received", 0)
        worker.min_heartbeat_period = obj.get("min_heartbeat_period", sys.maxint)
        worker.max_heartbeat_period = obj.get("max_heartbeat_period", 0)
        worker.avg_heartbeat_period = obj.get("avg_heartbeat_period", 0)
        worker.total_msgs_received = obj.get("total_msgs_received", 0)
        worker.min_msgs_received_per_min = obj.get("min_msgs_received_per_min", 0)
        worker.max_msgs_received_per_min = obj.get("max_msgs_received_per_min", 0)
        worker.avg_msgs_received_per_min = obj.get("avg_msgs_received_per_min", 0)
        worker.total_msgs_sent = obj.get("total_msgs_sent", 0)
        worker.min_msgs_sent_per_min = obj.get("min_msgs_sent_per_min", 0)
        worker.max_msgs_sent_per_min = obj.get("max_msgs_sent_per_min", 0)
        worker.avg_msgs_sent_per_min = obj.get("avg_msgs_sent_per_min", 0)
        worker.process_cpu = obj.get("process_cpu", 0)
        worker.virtual_mem = obj.get("virtual_mem", 0)
        worker.swap_mem = obj.get("swap_mem", 0)
        worker.process_mem = obj.get("process_mem", 0)
        return worker

    def encode(self):
        return {
            "type": self.type,
            "id": self.id,
            "schema_rev": self.schema_rev,
            "created_at": self.created_at,
            "last_updated_at": self.last_updated_at,
            "url": self.url,
            "state": self.state,
            "quiescent": self.quiescent,
            "joined_count": self.joined_count,
            "left_count": self.left_count,
            "died_count": self.died_count,
            "resurrected_count": self.resurrected_count,
            "last_heartbeat_sent_at": self.last_heartbeat_sent_at,
            "last_heartbeat_received_at": self.last_heartbeat_received_at,
            "last_heartbeat_period": self.last_heartbeat_period,
            "heartbeat_timeout": self.heartbeat_timeout,
            "last_msg_received": self.last_msg_received,
            "last_msg_received_at": self.last_msg_received_at,
            "last_msg_sent": self.last_msg_sent,
            "last_msg_sent_at": self.last_msg_sent_at,
            "executing_task_count": self.executing_task_count,
            "executing_task_ids": self.executing_task_ids,
            "running_task_count": self.running_task_count,
            "running_task_ids": self.running_task_ids,
            "max_running_task_count": self.max_running_task_count,
            "total_heartbeats_received": self.total_heartbeats_received,
            "min_heartbeat_period": self.min_heartbeat_period,
            "max_heartbeat_period": self.max_heartbeat_period,
            "avg_heartbeat_period": self.avg_heartbeat_period,
            "total_msgs_received": self.total_msgs_received,
            "min_msgs_received_per_min": self.min_msgs_received_per_min,
            "max_msgs_received_per_min": self.max_msgs_received_per_min,
            "avg_msgs_received_per_min": self.avg_msgs_received_per_min,
            "total_msgs_sent": self.total_msgs_sent,
            "min_msgs_sent_per_min": self.min_msgs_sent_per_min,
            "max_msgs_sent_per_min": self.max_msgs_sent_per_min,
            "avg_msgs_sent_per_min": self.avg_msgs_sent_per_min,
            "process_cpu": self.process_cpu,
            "virtual_mem": self.virtual_mem,
            "swap_mem": self.swap_mem,
            "process_mem": self.process_mem
        }


class Scheduler(Resource):
    ID = "Scheduler"
    
    def __init__(self):
        super(Scheduler, self).__init__(constants.SCHEDULER_TYPE, 
                                        Scheduler.ID)
        self.schema_rev = 1
        self.created_at = None
        self.last_updated_at = None
        self.url = None
        self.state = None
        self.scheduler_update_interval = None
        self.rebalance_job_tasks_interval = None
        self.min_worker_heartbeat_timeout = None        
        self.create_job_msgs_received = 0
        self.update_job_msgs_received = 0
        self.delete_job_msgs_received = 0
        self.stop_job_msgs_received = 0
        self.restart_job_msgs_received = 0
        self.task_started_msgs_received = 0
        self.task_ended_msgs_received = 0
        self.task_progress_changed_msgs_received = 0
        self.join_group_msgs_received = 0
        self.group_left_msgs_received = 0
        self.heartbeat_msgs_received = 0
        self.execute_task_msgs_sent = 0
        self.stop_task_msgs_sent = 0      
        self.completed_jobs = 0
        self.successful_jobs = 0
        self.failed_jobs = 0
        self.permanently_failed_jobs = 0
        self.successful_tasks = 0
        self.failed_tasks = 0
        self.permanently_failed_tasks = 0
        self.stopped_tasks = 0
        self.dead_workers = 0
        self.resurrected_workers = 0
        self.tasks_waiting_too_long = 0
        self.tasks_assigned_to_dead_worker = 0
        self.missing_running_tasks_in_worker = 0
        self.coordinate_job_tasks_runs = 0                 
        self.rebalance_job_tasks_runs = 0        
        self.total_msgs_received = 0
        self.min_msgs_received_per_min = 0
        self.max_msgs_received_per_min = 0
        self.avg_msgs_received_per_min = 0
        self.total_msgs_sent = 0
        self.min_msgs_sent_per_min = 0
        self.max_msgs_sent_per_min = 0
        self.avg_msgs_sent_per_min = 0
        
    @staticmethod
    def decode(obj):
        scheduler = Scheduler()
        scheduler.schema_rev = obj.get("schema_rev", 1)
        scheduler.created_at = obj.get("created_at")
        scheduler.last_updated_at = obj.get("last_updated_at")
        scheduler.state = obj.get("state")
        scheduler.scheduler_update_interval = obj.get("scheduler_update_interval", 0)
        scheduler.rebalance_job_tasks_interval = obj.get("rebalance_job_tasks_interval", 0)
        scheduler.min_worker_heartbeat_timeout = obj.get("min_worker_heartbeat_timeout", 0)
        scheduler.create_job_msgs_received = obj.get("create_job_msgs_received", 0)
        scheduler.update_job_msgs_received = obj.get("update_job_msgs_received", 0)
        scheduler.delete_job_msgs_received = obj.get("delete_job_msgs_received", 0)
        scheduler.stop_job_msgs_received = obj.get("stop_job_msgs_received", 0)
        scheduler.restart_job_msgs_received = obj.get("restart_job_msgs_received", 0)
        scheduler.task_started_msgs_received = obj.get("task_started_msgs_received", 0)
        scheduler.task_ended_msgs_received = obj.get("task_ended_msgs_received", 0)
        scheduler.task_progress_changed_msgs_received = obj.get("task_progress_changed_msgs_received", 0)
        scheduler.join_group_msgs_received = obj.get("join_group_msgs_received", 0)
        scheduler.group_left_msgs_received = obj.get("group_left_msgs_received", 0)
        scheduler.heartbeat_msgs_received = obj.get("heartbeat_msgs_received", 0)
        scheduler.execute_task_msgs_sent = obj.get("execute_task_msgs_sent", 0)
        scheduler.stop_task_msgs_sent = obj.get("stop_task_msgs_sent", 0)
        scheduler.completed_jobs = obj.get("completed_jobs", 0)
        scheduler.successful_jobs = obj.get("successful_jobs", 0)
        scheduler.failed_jobs = obj.get("failed_jobs", 0)
        scheduler.permanently_failed_jobs = obj.get("permanently_failed_jobs", 0)
        scheduler.successful_tasks = obj.get("successful_tasks", 0)
        scheduler.failed_tasks = obj.get("failed_tasks", 0)
        scheduler.permanently_failed_tasks = obj.get("permanently_failed_tasks", 0)
        scheduler.stopped_tasks = obj.get("stopped_tasks", 0)
        scheduler.dead_workers = obj.get("dead_workers", 0)
        scheduler.resurrected_workers = obj.get("resurrected_workers", 0)
        scheduler.tasks_waiting_too_long = obj.get("tasks_waiting_too_long", 0)
        scheduler.tasks_assigned_to_dead_worker = obj.get("tasks_assigned_to_dead_worker", 0)
        scheduler.missing_running_tasks_in_worker = obj.get("missing_running_tasks_in_worker", 0)
        scheduler.coordinate_job_tasks_runs = obj.get("coordinate_job_tasks_runs", 0)  
        scheduler.rebalance_job_tasks_runs = obj.get("rebalance_job_tasks_runs", 0)
        scheduler.total_msgs_received = obj.get("total_msgs_received", 0)
        scheduler.min_msgs_received_per_min = obj.get("min_msgs_received_per_min", 0)
        scheduler.max_msgs_received_per_min = obj.get("max_msgs_received_per_min", 0)
        scheduler.avg_msgs_received_per_min = obj.get("avg_msgs_received_per_min", 0)
        scheduler.total_msgs_sent = obj.get("total_msgs_sent", 0)
        scheduler.min_msgs_sent_per_min = obj.get("min_msgs_sent_per_min", 0)
        scheduler.max_msgs_sent_per_min = obj.get("max_msgs_sent_per_min", 0)
        scheduler.avg_msgs_sent_per_min = obj.get("avg_msgs_sent_per_min", 0)
        return scheduler

    def encode(self):
        return {
            "type": self.type,
            "id": self.id,
            "schema_rev": self.schema_rev,
            "created_at": self.created_at,
            "last_updated_at": self.last_updated_at,
            "url": self.url,
            "state": self.state,
            "scheduler_update_interval": self.scheduler_update_interval,
            "rebalance_job_tasks_interval": self.rebalance_job_tasks_interval,
            "min_worker_heartbeat_timeout": self.min_worker_heartbeat_timeout,
            "create_job_msgs_received": self.create_job_msgs_received,
            "update_job_msgs_received": self.update_job_msgs_received,
            "delete_job_msgs_received": self.delete_job_msgs_received,
            "stop_job_msgs_received": self.stop_job_msgs_received,
            "restart_job_msgs_received": self.restart_job_msgs_received,
            "task_started_msgs_received": self.task_started_msgs_received,
            "task_ended_msgs_received": self.task_ended_msgs_received,
            "task_progress_changed_msgs_received": self.task_progress_changed_msgs_received,
            "join_group_msgs_received": self.join_group_msgs_received,
            "group_left_msgs_received": self.group_left_msgs_received,
            "heartbeat_msgs_received": self.heartbeat_msgs_received,
            "execute_task_msgs_sent": self.execute_task_msgs_sent,
            "stop_task_msgs_sent": self.stop_task_msgs_sent,
            "completed_jobs": self.completed_jobs,
            "successful_jobs": self.successful_jobs,
            "failed_jobs": self.failed_jobs,
            "permanently_failed_jobs": self.permanently_failed_jobs,
            "successful_tasks": self.successful_tasks,
            "failed_tasks": self.failed_tasks,
            "permanently_failed_tasks": self.permanently_failed_tasks,
            "stopped_tasks": self.stopped_tasks,
            "dead_workers": self.dead_workers,
            "resurrected_workers": self.resurrected_workers,
            "tasks_waiting_too_long": self.tasks_waiting_too_long,
            "tasks_assigned_to_dead_worker": self.tasks_assigned_to_dead_worker,
            "missing_running_tasks_in_worker": self.missing_running_tasks_in_worker,
            "coordinate_job_tasks_runs": self.coordinate_job_tasks_runs,      
            "rebalance_job_tasks_runs": self.rebalance_job_tasks_runs,
            "total_msgs_received": self.total_msgs_received,
            "min_msgs_received_per_min": self.min_msgs_received_per_min,
            "max_msgs_received_per_min": self.max_msgs_received_per_min,
            "avg_msgs_received_per_min": self.avg_msgs_received_per_min,
            "total_msgs_sent": self.total_msgs_sent,
            "min_msgs_sent_per_min": self.min_msgs_sent_per_min,
            "max_msgs_sent_per_min": self.max_msgs_sent_per_min,
            "avg_msgs_sent_per_min": self.avg_msgs_sent_per_min
        }


#
#    Job Messages
#

class JobMsg(object):
    def __init__(self, timestamp=None):
        super(JobMsg, self).__init__()
        self.timestamp = timestamp if timestamp is not None else time_util.get_current_utc_millis()       

    @staticmethod
    def factory(envelope):
        content = envelope["job_msg"]
        msg_type = content["type"]
        if msg_type == CreateJobMsg.Type:
            return CreateJobMsg.decode(envelope)
        elif msg_type == UpdateJobMsg.Type:
            return UpdateJobMsg.decode(envelope)
        elif msg_type == DeleteJobMsg.Type:
            return DeleteJobMsg.decode(envelope)
        elif msg_type == StopJobMsg.Type:
            return StopJobMsg.decode(envelope)
        elif msg_type == RestartJobMsg.Type:
            return RestartJobMsg.decode(envelope)
        elif msg_type == DeleteJobHistoryMsg.Type:
            return DeleteJobHistoryMsg.decode(envelope)
        else:
            raise ValueError("Invalid job msg type: {}".format(msg_type))

    def encode(self):
        raise NotImplementedError


class CreateJobMsg(JobMsg):
    '''
    Sent by a client to the controller to create a new job
    '''
    
    Type = 'create_job'

    def __init__(self, job, timestamp=None):
        super(CreateJobMsg, self).__init__(timestamp)
        self.job = job

    @staticmethod
    def decode(obj):
        content = obj["job_msg"]
        return CreateJobMsg(        
            Job.decode(content["job"]),
            content['timestamp'])

    def encode(self):
        return {
            "job_msg": {
                "type": CreateJobMsg.Type,
                "job": self.job.encode(),
                "timestamp": self.timestamp                 
            }
        }


class UpdateJobMsg(JobMsg):
    '''
    Sent by a client to the controller to update a job
    '''
    
    Type = 'update_job'

    def __init__(self, job, timestamp=None):
        super(UpdateJobMsg, self).__init__(timestamp)
        self.job = job

    @staticmethod
    def decode(obj):
        content = obj["job_msg"]
        return UpdateJobMsg(        
            Job.decode(content["job"]),
            content['timestamp'])

    def encode(self):
        return {
            "job_msg": {
                "type": UpdateJobMsg.Type,
                "job": self.job.encode(),
                "timestamp": self.timestamp                 
            }
        }


class DeleteJobMsg(JobMsg):
    '''
    Sent by a client to the controller to delete a job
    '''
    
    Type = 'delete_job'

    def __init__(self, job_id, force=False, timestamp=None):
        super(DeleteJobMsg, self).__init__(timestamp)
        self.job_id = job_id
        self.force = force

    @staticmethod
    def decode(obj):
        content = obj["job_msg"]
        return DeleteJobMsg(        
            content["job_id"],
            content['force'],
            content['timestamp'])

    def encode(self):
        return {
            "job_msg": {
                "type": DeleteJobMsg.Type,
                "job_id": self.job_id,
                "force": self.force,
                "timestamp": self.timestamp                 
            }
        }


class StopJobMsg(JobMsg):
    '''
    Sent by a client to the controller to stop a job
    '''
    
    Type = 'stop_job'

    def __init__(self, job_id, force=False, timestamp=None):
        super(StopJobMsg, self).__init__(timestamp)
        self.job_id = job_id
        self.force = force

    @staticmethod
    def decode(obj):
        content = obj["job_msg"]
        return StopJobMsg(        
            content["job_id"],
            content['force'],
            content['timestamp'])

    def encode(self):
        return {
            "job_msg": {
                "type": StopJobMsg.Type, 
                "job_id": self.job_id,
                "force": self.force,
                "timestamp": self.timestamp
            }
        }


class RestartJobMsg(JobMsg):
    '''
    Sent by a client to the controller to restart a stopped job
    '''
    
    Type = 'restart_job'

    def __init__(self, job_id, timestamp=None):
        super(RestartJobMsg, self).__init__(timestamp)
        self.job_id = job_id

    @staticmethod
    def decode(obj):
        content = obj["job_msg"]
        return RestartJobMsg(
            content["job_id"],
            content['timestamp'])

    def encode(self):
        return {
            "job_msg": {
                "type": RestartJobMsg.Type, 
                "job_id": self.job_id,
                "timestamp": self.timestamp
            }
        }

class DeleteJobHistoryMsg(JobMsg):
    '''
    Sent by the api server to cleanup job history messages older than a given period of time
    '''

    Type = 'delete_job_history'

    def __init__(self, where=None, older_than_ms=None, timestamp=None):
        '''
        delete messages matching a `where` query older than
        `older_than_ms` milliseconds
        '''
        super(DeleteJobHistoryMsg, self).__init__(timestamp)
        self.where = where
        self.older_than_ms = older_than_ms

    @staticmethod
    def decode(obj):
        content = obj["job_msg"]
        return DeleteJobHistoryMsg(
            content["where"],
            content['older_than_ms'],
            content['timestamp'])

    def encode(self):
        return {
            "job_msg": {
                "type": DeleteJobHistoryMsg.Type,
                "where": self.where,
                "older_than_ms": self.older_than_ms,
                "timestamp": self.timestamp
            }
        }


#
#    Task Messages
#

class TaskMsg(object):
    def __init__(self, job_id, job_name, task_id, method, rev, timestamp=None):
        super(TaskMsg, self).__init__()
        self.job_id = job_id
        self.job_name = job_name
        self.task_id = task_id
        self.method = method
        self.rev = rev
        self.timestamp = timestamp if timestamp is not None else time_util.get_current_utc_millis()       
        
    @property
    def id(self):
        return self.method + '/' + self.job_id + '/' + self.task_id + '/' + str(self.rev)
    
    @staticmethod
    def factory(obj):
        content = obj["task_msg"]
        msg_type = content["type"]
        if msg_type == ExecuteTaskMsg.Type:
            return ExecuteTaskMsg.decode(obj)
        elif msg_type == TaskStartedMsg.Type:
            return TaskStartedMsg.decode(obj)
        elif msg_type == StopTaskMsg.Type:
            return StopTaskMsg.decode(obj)
        elif msg_type == TaskEndedMsg.Type:
            return TaskEndedMsg.decode(obj)
        elif msg_type == TaskProgressChangedMsg.Type:
            return TaskProgressChangedMsg.decode(obj)
        else:
            raise ValueError("Invalid task msg type: {}".format(msg_type))

    def encode(self):
        raise NotImplementedError


class ExecuteTaskMsg(TaskMsg):
    '''
    Sent by the controller to a worker to execute a task
    '''
    
    Type = 'execute_task'

    def __init__(self, job_id, job_name, task_id, method, rev,
                 function, input_, previous_input, worker_id, timestamp=None):
        super(ExecuteTaskMsg, self).__init__(job_id, job_name, task_id, method, rev, timestamp)
        self.function = function
        self.input = input_
        self.previous_input = previous_input
        self.worker_id = worker_id

    @staticmethod
    def decode(obj):
        content = obj["task_msg"]
        return ExecuteTaskMsg(        
            content["job_id"],
            content["job_name"],
            content["task_id"],
            content["method"],
            content["rev"],                        
            content["function"],                                
            content["input"],                                
            content["previous_input"],
            content["worker_id"],
            content['timestamp'])

    def encode(self):
        return {
            "task_msg": {
                "type": ExecuteTaskMsg.Type,
                "job_id": self.job_id, 
                "job_name": self.job_name, 
                "task_id": self.task_id,
                "method": self.method, 
                "rev": self.rev,
                "function": self.function, 
                "input": self.input, 
                "previous_input": self.previous_input,
                "worker_id": self.worker_id,
                "timestamp": self.timestamp
            }
        }


class TaskStartedMsg(TaskMsg):
    '''
    Sent by a worker to notify the controller a task started
    '''
    
    Type = 'task_started'
    
    def __init__(self, job_id, job_name, task_id, method, rev,
                 worker_id, started_at, timestamp=None):
        super(TaskStartedMsg, self).__init__(job_id, job_name, task_id, method, rev, timestamp)
        self.worker_id = worker_id
        self.started_at = started_at

    @staticmethod
    def decode(obj):
        content = obj["task_msg"]
        return TaskStartedMsg(        
            content["job_id"],
            content["job_name"],
            content["task_id"],
            content["method"],            
            content["rev"],            
            content["worker_id"],
            content["started_at"],
            content['timestamp'])

    def encode(self):
        return {
            "task_msg": {
                "type": TaskStartedMsg.Type, 
                "job_id": self.job_id,
                "job_name": self.job_name,
                "task_id": self.task_id,
                "method": self.method, 
                "rev": self.rev,
                "worker_id": self.worker_id,
                "started_at": self.started_at,
                "timestamp": self.timestamp
            }
        }


class StopTaskMsg(TaskMsg):
    '''
    Sent by the controller to a worker to stop a task
    '''
    
    Type = 'stop_task'
    
    def __init__(self, job_id, job_name, task_id, method, rev, worker_id, timestamp=None):
        super(StopTaskMsg, self).__init__(job_id, job_name, task_id, method, rev, timestamp)
        self.worker_id = worker_id

    @staticmethod
    def decode(obj):
        content = obj["task_msg"]
        return StopTaskMsg(        
            content["job_id"],
            content["job_name"],
            content["task_id"],
            content["method"],            
            content["rev"],            
            content["worker_id"],
            content['timestamp'])

    def encode(self):
        return {
            "task_msg": {
                "type": StopTaskMsg.Type, 
                "job_id": self.job_id,
                "job_name": self.job_name,
                "task_id": self.task_id,
                "method": self.method, 
                "rev": self.rev,
                "worker_id": self.worker_id,
                "timestamp": self.timestamp
            }
        }


class TaskEndedMsg(TaskMsg):
    '''
    Sent by a worker to notify the controller a task ended
    '''
    
    Type = 'task_ended'
    
    def __init__(self, job_id, job_name, task_id, method, rev,
                 state, output, error, worker_id, ended_at, timestamp=None):
        super(TaskEndedMsg, self).__init__(job_id, job_name, task_id, method, rev, timestamp)
        self.state = state
        self.output = output
        self.error = error
        self.worker_id = worker_id
        self.ended_at = ended_at

    @staticmethod
    def decode(obj):
        content = obj["task_msg"]
        return TaskEndedMsg(        
            content["job_id"],
            content["job_name"],
            content["task_id"],
            content["method"],            
            content["rev"],            
            content["state"],
            content["output"],
            Error.decode(content.get("error")),
            content["worker_id"],
            content["ended_at"],
            content['timestamp'])

    def encode(self):
        return {
            "task_msg": {
                "type": TaskEndedMsg.Type, 
                "job_id": self.job_id,
                "job_name": self.job_name,
                "task_id": self.task_id,
                "method": self.method, 
                "rev": self.rev,
                "state": self.state,
                "output": self.output,
                "error": None if self.error is None else self.error.encode(),                
                "worker_id": self.worker_id,
                "ended_at": self.ended_at,
                "timestamp": self.timestamp
            }
        }


class TaskProgressChangedMsg(TaskMsg):
    '''
    Sent by a worker to notify the controller the progress of a task changed
    '''
    
    Type = 'task_progress_changed'
    
    def __init__(self, job_id, job_name, task_id, method, rev,
                 progress, worker_id, timestamp=None):
        super(TaskProgressChangedMsg, self).__init__(job_id, job_name, task_id, method, rev, timestamp)
        self.progress = progress
        self.worker_id = worker_id

    @staticmethod
    def decode(obj):
        content = obj["task_msg"]
        return TaskProgressChangedMsg(        
            content["job_id"],
            content["job_name"],
            content["task_id"],
            content["method"],            
            content["rev"],            
            content["progress"],
            content["worker_id"],
            content['timestamp'])

    def encode(self):
        return {
            "task_msg": {
                "type": TaskProgressChangedMsg.Type, 
                "job_id": self.job_id,
                "job_name": self.job_name,
                "task_id": self.task_id,
                "method": self.method, 
                "rev": self.rev,
                "progress": self.progress,
                "worker_id": self.worker_id,
                "timestamp": self.timestamp
            }
        }


#
#    Group Membership Messages
#

class MemberMsg(object):
    def __init__(self, worker_id, timestamp=None):
        super(MemberMsg, self).__init__()
        self.worker_id = worker_id
        self.timestamp = timestamp if timestamp is not None else time_util.get_current_utc_millis()       
    
    @staticmethod
    def factory(obj):
        content = obj["member_msg"]
        msg_type = content["type"]
        if msg_type == JoinGroupMsg.Type:
            return JoinGroupMsg.decode(obj)
        elif msg_type == LeaveGroupMsg.Type:
            return LeaveGroupMsg.decode(obj)
        elif msg_type == HeartbeatMsg.Type:
            return HeartbeatMsg.decode(obj)
        elif msg_type == UpdateWorkerMsg.Type:
            return UpdateWorkerMsg.decode(obj)
        else:
            raise ValueError("Invalid member msg type: {}".format(msg_type))

    def encode(self):
        raise NotImplementedError

    
class JoinGroupMsg(MemberMsg):
    '''
    Sent by a member to the controller to join a group
    '''
    
    Type = 'join_group'

    def __init__(self, worker_id, timestamp):
        super(JoinGroupMsg, self).__init__(worker_id, timestamp)
        self.max_running_task_count = constants.MAX_RUNNING_TASK_COUNT
        
    @staticmethod
    def decode(obj):
        content = obj["member_msg"]
        msg = JoinGroupMsg(content["worker_id"], content["timestamp"])
        msg.max_running_task_count = content.get("max_running_task_count", constants.MAX_RUNNING_TASK_COUNT)
        return msg
    
    def encode(self):
        return {
            "member_msg": {
                "type": JoinGroupMsg.Type, 
                "worker_id": self.worker_id,
                "max_running_task_count": self.max_running_task_count,
                "timestamp": self.timestamp
            }
        }


class LeaveGroupMsg(MemberMsg):
    '''
    Sent by a member to notify the controller it left a group
    '''
    
    Type = 'leave_group'

    def __init__(self, worker_id, timestamp):
        super(LeaveGroupMsg, self).__init__(worker_id, timestamp)
        
    @staticmethod
    def decode(obj):
        content = obj["member_msg"]
        msg = LeaveGroupMsg(content["worker_id"], content["timestamp"])
        return msg
    
    def encode(self):
        return {
            "member_msg": {
                "type": LeaveGroupMsg.Type, 
                "worker_id": self.worker_id,
                "timestamp": self.timestamp
            }
        }


class HeartbeatMsg(JoinGroupMsg):
    Type = 'heartbeat'

    def __init__(self, worker_id, timestamp):
        super(HeartbeatMsg, self).__init__(worker_id, timestamp)
        self.last_msg_received = None
        self.last_msg_received_at = None
        self.last_msg_sent = None
        self.last_msg_sent_at = None
        self.total_msgs_received = 0
        self.min_msgs_received_per_min = 0
        self.max_msgs_received_per_min = 0
        self.avg_msgs_received_per_min = 0
        self.total_msgs_sent = 0
        self.min_msgs_sent_per_min = 0
        self.max_msgs_sent_per_min = 0
        self.avg_msgs_sent_per_min = 0
        self.process_cpu = None                
        self.virtual_mem = None
        self.swap_mem = None
        self.process_mem = None                
        
    @staticmethod
    def decode(obj):
        content = obj["member_msg"]
        msg = HeartbeatMsg(content["worker_id"], content["timestamp"])
        msg.max_running_task_count = content.get("max_running_task_count", constants.MAX_RUNNING_TASK_COUNT)
        msg.running_task_ids = content.get("running_task_ids", [])
        msg.last_msg_received = content.get("last_msg_received")
        msg.last_msg_received_at = content.get("last_msg_received_at")
        msg.last_msg_sent = content.get("last_msg_sent", 0)
        msg.last_msg_sent_at = content.get("last_msg_sent_at")
        msg.total_msgs_received = content.get("total_msgs_received", 0)
        msg.min_msgs_received_per_min = content.get("min_msgs_received_per_min", 0)
        msg.max_msgs_received_per_min = content.get("max_msgs_received_per_min", 0)
        msg.avg_msgs_received_per_min = content.get("avg_msgs_received_per_min", 0)
        msg.total_msgs_sent = content.get("total_msgs_sent", 0)
        msg.min_msgs_sent_per_min = content.get("min_msgs_sent_per_min", 0)
        msg.max_msgs_sent_per_min = content.get("max_msgs_sent_per_min", 0)
        msg.avg_msgs_sent_per_min = content.get("avg_msgs_sent_per_min", 0)
        msg.process_cpu = content.get("process_cpu", 0)
        msg.virtual_mem = content.get("virtual_mem", 0)
        msg.swap_mem = content.get("swap_mem", 0)
        msg.process_mem = content.get("process_mem", 0)
        return msg
    
    def encode(self):
        return {
            "member_msg": {
                "type": HeartbeatMsg.Type, 
                "worker_id": self.worker_id,
                "max_running_task_count": self.max_running_task_count,
                "running_task_ids": self.running_task_ids,
                "last_msg_received": self.last_msg_received,
                "last_msg_received_at": self.last_msg_received_at,
                "last_msg_sent": self.last_msg_sent,
                "last_msg_sent_at": self.last_msg_sent_at,
                "total_msgs_received": self.total_msgs_received,
                "min_msgs_received_per_min": self.min_msgs_received_per_min,
                "max_msgs_received_per_min": self.max_msgs_received_per_min,
                "avg_msgs_received_per_min": self.avg_msgs_received_per_min,                
                "total_msgs_sent": self.total_msgs_sent,
                "min_msgs_sent_per_min": self.min_msgs_sent_per_min,
                "max_msgs_sent_per_min": self.max_msgs_sent_per_min,
                "avg_msgs_sent_per_min": self.avg_msgs_sent_per_min,                
                "process_cpu": self.process_cpu,                
                "virtual_mem": self.virtual_mem,
                "swap_mem": self.swap_mem,
                "process_mem": self.process_mem,                
                "timestamp": self.timestamp
            }
        }

        
class UpdateWorkerMsg(MemberMsg):
    '''
    Sent by the api server to update the state of the worker
    '''

    Type = 'update_worker'

    def __init__(self, worker_id, timestamp=None):
        '''
        Set the quiescent state of the worker to true|false
        '''
        super(UpdateWorkerMsg, self).__init__(worker_id, timestamp)
        self.quiescent = False

    @staticmethod
    def decode(obj):
        content = obj["member_msg"]
        msg = UpdateWorkerMsg(content["worker_id"], content['timestamp'])
        msg.quiescent = content.get("quiescent", False)
        return msg

    def encode(self):
        return {
            "member_msg": {
                "type": UpdateWorkerMsg.Type,
                "worker_id": self.worker_id,
                "quiescent": self.quiescent,
                "timestamp": self.timestamp
            }
        }


        
