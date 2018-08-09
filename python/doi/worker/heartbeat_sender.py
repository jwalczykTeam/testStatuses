'''
Created on Aug 23, 2016

@author: alberto
'''


from doi.scheduler.common import HeartbeatMsg
from doi.util import log_util
from doi.util import sys_util
from doi.util import time_util
from doi.util import thread_util
import time


logger = log_util.get_logger("astro.worker.heartbeat")


class HeartbeatSender(thread_util.StoppableThread):
    # Designed not to raise exceptions
    def __init__(self, worker):
        super(HeartbeatSender, self).__init__()
        self.worker = worker
        self.daemon = True
        
    def run(self):
        try:
            while not self.stop_requested():
                with self.worker.task_executors_lock:
                    self.send_heartbeat()
                time.sleep(self.worker.heartbeat_interval)
            
            logger.info("Heartbeat sender stopped: worker_id=%s", self.worker.id)
            self.set_stopped()
        except:
            logger.exception("Unexpected exception in heartbeat sender: worker_id=%s", 
                             self.worker.id)
            
    def send_heartbeat(self):
        try:
            worker = self.worker
            msg_received_throughput = worker.msg_received_throughput
            msg_sent_throughput = worker.msg_sent_throughput
            
            current_time = time_util.get_current_utc_millis()
            msg = HeartbeatMsg(worker.id, current_time)
            logger.debug("Sending heartbeat message: worker_id='%s', timestamp=%d", 
                         msg.worker_id, msg.timestamp)
            msg.max_running_task_count = worker.max_running_task_count
            msg.running_task_ids = [executor.id for executor in worker.task_executors]            
            msg.last_msg_received = worker.last_msg_received
            msg.last_msg_received_at = worker.last_msg_received_at
            msg.last_msg_sent = worker.last_msg_sent
            msg.last_msg_sent_at = worker.last_msg_sent_at
            msg.total_msgs_received = msg_received_throughput.total_count
            msg.min_msgs_received_per_min = msg_received_throughput.min_count_per_interval
            msg.max_msgs_received_per_min = msg_received_throughput.max_count_per_interval
            msg.avg_msgs_received_per_min = int(round(msg_received_throughput.avg_count_per_interval))
            msg.total_msgs_sent = msg_sent_throughput.total_count
            msg.min_msgs_sent_per_min = msg_sent_throughput.min_count_per_interval
            msg.max_msgs_sent_per_min = msg_sent_throughput.max_count_per_interval
            msg.avg_msgs_sent_per_min = int(round(msg_sent_throughput.avg_count_per_interval))
            msg.process_cpu = sys_util.current_process_cpu_percent()
            msg.virtual_mem = sys_util.used_virtual_memory()
            msg.swap_mem = sys_util.used_swap_memory()
            msg.process_mem = sys_util.used_current_process_memory()
            worker.mb_producer.send(worker.scheduler_messages_topic_name, msg.encode())
            worker.msg_sent_throughput.increase()            
        except:
            logger.exception("Unexpected exception while sending heartbeat message: "
                             "worker_id='%s'", self.worker.id)
        
