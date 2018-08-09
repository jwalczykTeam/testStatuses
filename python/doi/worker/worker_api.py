'''
Created on Feb 28, 2017

@author: adminibm
'''

import os
from flask import Flask, jsonify
from doi import constants
from doi.util import time_util
from doi.util import log_util
#from mem_top import mem_top

logger = log_util.get_logger("astro.jobs.worker_api")

class WorkerApi(Flask):
    '''
    Implement the worker api
    /info at the moment
    '''
    def __init__(self):
        super(WorkerApi, self).__init__('astro-worker')
        self._started_at = time_util.get_current_utc_millis()
        self._info = {
            'astro_worker_id': os.environ.get(constants.ASTRO_WORKER_ID_ENV_VAR),
            'astro_group_id': os.environ.get(constants.ASTRO_GROUP_ID_ENV_VAR),
            'astro_last_commit_sha': os.environ.get('ASTRO_LAST_COMMIT_SHA'),
            'started_at': time_util.utc_millis_to_iso8601_datetime(self._started_at)
        }

        self.add_url_rule('/info', 'info', self.get_info)

    def get_info(self):
        self._info['up_time'] = time_util.utc_millis_to_iso8601_duration(
            time_util.get_current_utc_millis() - self._started_at)
        #logger.info("  ====####################   WORKER_API.PY  MEM   TOP   #########################===  ")
        #logger.debug(mem_top())
        return jsonify(self._info)
