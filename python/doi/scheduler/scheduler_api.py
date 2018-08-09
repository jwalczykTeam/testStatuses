'''
Created on Feb 28, 2017

@author: adminibm
'''

import os
from flask import Flask, jsonify
from doi import constants
from doi.util import time_util


class SchedulerApi(Flask):
    '''
    Implement the worker api
    /info at the moment
    '''
    def __init__(self):
        super(SchedulerApi, self).__init__('astro-scheduler')
        self._started_at = time_util.get_current_utc_millis()
        self._info = {
            'astro_group_id': os.environ.get(constants.ASTRO_GROUP_ID_ENV_VAR),
            'astro_last_commit_sha': os.environ.get('ASTRO_LAST_COMMIT_SHA'),
            'started_at': time_util.utc_millis_to_iso8601_datetime(self._started_at)
        }

        self.add_url_rule('/info', 'info', self.get_info)

    def get_info(self):
        self._info['up_time'] = time_util.utc_millis_to_iso8601_duration(
            time_util.get_current_utc_millis() - self._started_at)
        return jsonify(self._info)
