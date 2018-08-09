'''
Created on Mar 10, 2016

@author: alberto
'''

from doi import constants
from doi.common import TaskMethod
from doi.util import log_util
import os


logger = log_util.get_logger("astro.jobs.miner")


class Miner(object):

    def __init__(self, params, progress_callback, stop_event):
        self.job_name = params.job_name
        self.method = params.method
        self.rev = params.rev
        self.input = params.input
        self.previous_input = params.previous_input
        self.progress_callback = progress_callback
        self.stop_event = stop_event

        self.sources = self.input['sources']
        self.max_mining_time = self.input.get('max_mining_time')
        self.work_dir = os.environ[constants.ASTRO_OUT_DIR_ENV_VAR]

    def mine_data(self):
        raise NotImplementedError("mine_data")

    def delete_data(self):
        raise NotImplementedError("delete_data")


def mine_data(cls, params, progress_callback, stop_event):
    obj = cls(params, progress_callback, stop_event)

    method = params.method
    if method in [TaskMethod.CREATE, TaskMethod.UPDATE]:
        return obj.mine_data()
    elif method == TaskMethod.DELETE:
        return obj.delete_data()
    else:
        assert method in [TaskMethod.CREATE, TaskMethod.UPDATE, TaskMethod.DELETE], \
            "Invalid job method: {}".format(method)
