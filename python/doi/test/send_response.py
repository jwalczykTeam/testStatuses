'''
Created on Jan 16, 2017

@author: alberto
'''

from doi import constants
from doi.common import TaskMethod
from doi.common import TaskState
from doi.jobs.analyzer.analyzer import Analyzer
from doi.util import cf_util
from doi.util.mb import mb_util
import json
import os

def send_response(job_id, task_id, method, rev, state, error):

    response = {
        "type": "task_ended",
        "job_id": job_id,
        "task_id": task_id,
        "method": method,
        "rev": rev,
        "state": state,
        "error": error
    }
    
    # initialize message broker
    os.environ[constants.ASTRO_MB_ENV_VAR] = json.dumps(cf_util.get_mb_credentials())                    
    mb_config = json.loads(os.environ[constants.ASTRO_MB_ENV_VAR])        
    mb = mb_util.MessageBroker(mb_config)
    
    mb_client_id = "rudi-analyzer"
    responses_topic_name = Analyzer.RESPONSES_TOPIC_NAME
    #mb_manager = mb.new_manager()
    #mb_manager.delete_topic(responses_topic_name)      
    mb_producer = mb.new_producer(mb_client_id)    
    mb_producer.send(responses_topic_name, { "task_msg": response })        


error = {
    "status": 500,
    "title": "Error title",
    "type": "about:blank",
    "detail": "detail"
}

send_response("3139d542-e8de-11e6-8c7e-28d2445d1645", "analyze_current_data", TaskMethod.CREATE, 1, TaskState.PERMANENTLY_FAILED, error)
