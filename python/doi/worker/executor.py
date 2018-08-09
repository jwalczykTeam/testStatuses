'''
Created on Aug 23, 2016

@author: alberto
'''


from doi.common import TaskState
from doi.common import Error
from doi.common import StoppedException, FailedException, PermanentlyFailedException
from doi.scheduler.common import TaskProgressChangedMsg
from doi.util import log_util
import importlib
import threading
import traceback
#from pympler.tracker import SummaryTracker
#from mem_top import mem_top


logger = log_util.get_logger("astro.worker.executor")


class TaskExecutor(threading.Thread):
    def __init__(self, worker, function, task_params):
        super(TaskExecutor, self).__init__()
        self.worker = worker
        self.function = function
        self.task_params = task_params
        self.job_id = task_params.job_id
        self.job_name = task_params.job_name
        self.task_id = task_params.task_id
        self.method = task_params.method
        self.rev = task_params.rev
        self.stop_event = threading.Event()
        self.state = None
        self.output = None
        self.error = None
        self.running_time = None
        
    @property
    def id(self):
        return self.method + '/' + self.job_id + '/' + self.task_id + '/' + str(self.rev)

    def run(self):
        def set_progress(value):
            try:
                with self.worker.task_executors_lock:
                    msg = TaskProgressChangedMsg(self.job_id, self.job_name, self.task_id, self.method, self.rev, 
                                                 value, self.worker.id)
                    self.worker.mb_producer.send(self.worker.scheduler_messages_topic_name, msg.encode())
            except:
                logger.exception("Unexpected exception while setting progress")
        
        
        #tracker = SummaryTracker()

        #logger.info("==================  EXECUTOR.PY  START OF TASK  ==========================")
        #tracker.print_diff() 
        #tracker.print_diff() 
        #tracker.print_diff() 

        try:

            module_and_function = self.function.rsplit('.', 1)
            module = importlib.import_module(module_and_function[0])
            function = getattr(module, module_and_function[1])
            logger.info("Executing task: job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d",
                self.job_id, self.job_name, self.task_id, self.method, self.rev)
            self.output = function(self.task_params, set_progress, self.stop_event)
            logger.info("Task completed successfully: job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d", 
                self.job_id, self.job_name, self.task_id, self.method, self.rev)
            self.state = TaskState.SUCCESSFUL
        except StoppedException:
            logger.info("Task stopped: job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d", 
                self.job_id, self.job_name, self.task_id, self.method, self.rev)
            self.state = TaskState.STOPPED
        except PermanentlyFailedException as e:
            logger.info("Task permanently failed: job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d", 
                self.job_id, self.job_name, self.task_id, self.method, self.rev)
            logger.exception(e)            
            self.state = TaskState.PERMANENTLY_FAILED
            self.error = self._build_error(e)
        except FailedException as e:
            logger.info("Task failed: job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d", 
                self.job_id, self.job_name, self.task_id, self.method, self.rev)
            logger.exception(e)            
            self.state = TaskState.FAILED
            self.error = self._build_error(e)
        except Exception as e:
            logger.error("Task unexpectedly failed: job_id='%s', job_name='%s', task_id='%s', method='%s', rev=%d",                         
                self.job_id, self.job_name, self.task_id, self.method, self.rev)
            logger.exception(e)            
            self.state = TaskState.PERMANENTLY_FAILED
            self.error = self._build_unexpected_error(e)
        
        self.worker._task_ended(self)

        #logger.info("==================   EXECUTOR.PY  END OF TASK  ==========================")
        #tracker.print_diff()

        #logger.info("====####################   EXECUTOR.PY  MEM_TOP   #########################===")
        #logger.info("mem_top {}".format(mem_top()))
    
    def _build_error(self, ex):
        stack_trace = traceback.format_exc().split('\n')
        return Error(ex.type, ex.title, ex.status, ex.message, 
                     stack_trace=stack_trace)

    def _build_unexpected_error(self, e):
        stack_trace = traceback.format_exc().split('\n')
        return Error("about:blank", "Unexpected Task Error", 500, e.message, 
                     stack_trace=stack_trace)
