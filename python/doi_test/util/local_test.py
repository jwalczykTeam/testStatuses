'''
Created on Nov 7, 2016

@author: adminibm
'''

import os
import subprocess
import time
import json
from requests import ConnectionError
import doi.constants as constants
from doi_test.util.common import AstroInstance, \
    repeat_request_until, ACCESS_TOKEN_ENV_VAR, TENANT_ID_ENV_VAR


_ASTRO_API = 'http://localhost:{}/v1'

def _change_ext(pathname):
    return os.path.splitext(pathname)[0]+'.py'


def _build_work_dir(wd, test_module, test_class):
    if test_module:
        wd = os.path.join(wd, test_module)
    if test_class:
        wd = os.path.join(wd, test_class)
    return wd

class LocalAstroInstance(AstroInstance):
    @staticmethod
    def get_api_app():
        return _change_ext(os.path.abspath(__import__('doi.api.api_app', fromlist='dummy').__file__))

    @staticmethod
    def get_scheduler_app():
        return _change_ext(os.path.abspath(__import__('doi.scheduler.scheduler_app', fromlist='dummy').__file__))

    @staticmethod
    def get_worker_app():
        return _change_ext(os.path.abspath(__import__('doi.worker.worker_app', fromlist='dummy').__file__))


    '''
    Takes a conf object as input
    '''
    def __init__(self, conf, test_module=None, test_class=None):
        super(LocalAstroInstance, self).__init__(conf, test_module, test_class)

        self.work_dir = conf.get('work_dir', '/tmp/astro')
        self.group_id = constants.DEFAULT_GROUP_ID if not self.group_name else self.group_name
        self.scheduler_name = self.group_id + '-scheduler'
        self.api_server_prefix = self.group_id + '-api'
        self.worker_prefix = self.group_id + '-worker'

        root_work_dir = _build_work_dir(self.work_dir, test_module, test_class)
        self.scheduler_work_dir = os.path.join(root_work_dir, 'scheduler')
        self.workers_work_dir = os.path.join(root_work_dir, 'workers')
        self.api_servers_work_dir = os.path.join(root_work_dir, 'api-servers')

        self.scheduler = None
        self.api_servers = {}
        self.workers = {}

        # Default API endpoint to first API Server
        self.api_ep = _ASTRO_API.format(self.api_port+1)


    def _build_scheduler_env(self):
        env = os.environ.copy()

        if self.db_config:
            env[constants.ASTRO_DB_ENV_VAR] = json.dumps(self.db_config)
        if self.mb_config:
            env[constants.ASTRO_MB_ENV_VAR] = json.dumps(self.mb_config)

        env[constants.ASTRO_OUT_DIR_ENV_VAR] = self.scheduler_work_dir
        env[constants.ASTRO_GROUP_ID_ENV_VAR] = self.group_id

        if not os.path.exists(self.scheduler_work_dir):
            os.makedirs(self.scheduler_work_dir)

        return env


    def _build_api_server_env(self, sid, port):
        env = os.environ.copy()

        if self.db_config:
            env[constants.ASTRO_DB_ENV_VAR] = json.dumps(self.db_config)
        if self.mb_config:
            env[constants.ASTRO_MB_ENV_VAR] = json.dumps(self.mb_config)

        api_server_work_dir = os.path.join(self.api_servers_work_dir, sid)
        env[constants.ASTRO_OUT_DIR_ENV_VAR] = api_server_work_dir
        env[constants.ASTRO_GROUP_ID_ENV_VAR] = self.group_id
        env[constants.ASTRO_API_CONFIG_ENV_VAR] = '{"port": ' + str(port) + '}'

        if not os.path.exists(api_server_work_dir):
            os.makedirs(api_server_work_dir)

        if self.api_key:
            env[constants.ASTRO_API_KEY_ENV_VAR] = self.api_key

        if self.bluemix_info.target_api:
            env[constants.ASTRO_BLUEMIX_API_ENDPOINT_ENV_VAR] = self.bluemix_info.target_api

        if self.bluemix_info.access_token:
            env[ACCESS_TOKEN_ENV_VAR] = self.bluemix_info.access_token

        if self.bluemix_info.org_id:
            env[TENANT_ID_ENV_VAR] = self.bluemix_info.org_id

        return (env, api_server_work_dir)


    def _build_worker_env(self, wid, max_running_jobs):
        env = os.environ.copy()

        if self.db_config:
            env[constants.ASTRO_DB_ENV_VAR] = json.dumps(self.db_config)
        if self.mb_config:
            env[constants.ASTRO_MB_ENV_VAR] = json.dumps(self.mb_config)

        worker_work_dir = os.path.join(self.workers_work_dir, wid)
        env[constants.ASTRO_OUT_DIR_ENV_VAR] = worker_work_dir
        env[constants.ASTRO_WORKER_ID_ENV_VAR] = wid
        env[constants.ASTRO_GROUP_ID_ENV_VAR] = self.group_id

        '''
        TODO: ASTRO_WORKER_MAX_RUNNING_TASKS_ENV_VAR replaced by env var
        ASTRO_WORKER_CONFIG_ENV_VAR, whose value is a json payload where
        to set several options.
        if max_running_jobs:
            env[constants.ASTRO_WORKER_MAX_RUNNING_TASKS_ENV_VAR] = str(max_running_jobs)
        '''

        if not os.path.exists(worker_work_dir):
            os.makedirs(worker_work_dir)

        return (env, worker_work_dir)


    def start_scheduler(self, timeout=120):
        assert not self.scheduler

        '''
        Start the Astro scheduler in a child process
        Return the child process.
        '''
        scheduler_env = self._build_scheduler_env()

        log_file = os.path.join(self.scheduler_work_dir, '{}.log'.format(self.scheduler_name))

        with open(log_file, 'w') as logf:
            scheduler = subprocess.Popen(args=[LocalAstroInstance.get_scheduler_app()], shell=False,
                                         stderr=subprocess.STDOUT, stdout=logf,
                                         cwd=self.scheduler_work_dir, env=scheduler_env,
                                         close_fds=True)

        self.scheduler = scheduler

        return self.scheduler


    def start_api_server(self, index=None, timeout=120):
        '''
        Start an Astro API server in a child process
        Return the child process.
        index is a number used to build the api_server id.
        You can start a dead api server or a new api server.
        Assertion error is fired if the api server exists and is alive.
        '''
        if not index:
            index = len(self.api_servers.keys())+1

        sid = '{}-{}'.format(self.api_server_prefix, index)

        s = self.api_servers.get(sid, None)
        assert not LocalAstroInstance._alive(s)

        port = self.api_port + index
        env, api_server_work_dir = self._build_api_server_env(sid, port)
        log_file = os.path.join(api_server_work_dir, '{}.log'.format(sid))

        with open(log_file, 'w') as logf:
            api_server = subprocess.Popen(args=[LocalAstroInstance.get_api_app()], shell=False, stderr=subprocess.STDOUT, stdout=logf, cwd=api_server_work_dir, env=env, close_fds=True)

        self.api_servers[sid] = api_server

        api = self.astro_api(api_ep = _ASTRO_API.format(port))

        def check_status(status_data):
            return (status_data[0]==200, None)

        @repeat_request_until(check_status, poll=5, timeout=timeout)
        def test_api_server():
            try:
                return api.do_request(method='GET', path='/config', as_admin=True)
            except ConnectionError, e:
                return (404, e)

        for _ in test_api_server():
            pass

        return sid


    def get_workers(self):
        '''
        Return an array of worker_id
        '''
        return self.workers.keys()


    def start_worker(self, wid=None, timeout=120, max_running_jobs=None):
        '''
        Start an Astro worker in a child process
        Return the child process.
        index is a number used to build the worker id.
        You can start a dead worker or a new worker.
        Assertion error is fired if the worker exists and is alive.
        '''
        if not wid:
            index = len(self.workers.keys())+1
            wid = '{}-{}'.format(self.worker_prefix, index)

        w = self.workers.get(wid, None)
        assert not LocalAstroInstance._alive(w)

        env, worker_work_dir = self._build_worker_env(wid, max_running_jobs)
        log_file = os.path.join(worker_work_dir, '{}.log'.format(wid))

        with open(log_file, 'w') as logf:
            worker = subprocess.Popen(args=[LocalAstroInstance.get_worker_app()], shell=False,
                                      stderr=subprocess.STDOUT, stdout=logf,
                                      cwd=worker_work_dir, env=env,
                                      close_fds=True)

        self.workers[wid] = worker

        return wid


    def start(self, num_api_servers=1, num_workers=1, max_running_jobs=None, timeout=120):
        self.start_scheduler(timeout)

        for _ in range(num_workers):
            self.start_worker(max_running_jobs=max_running_jobs)

        for _ in range(num_api_servers):
            self.start_api_server(timeout=timeout)


    def build(self):
        '''
        Nothing to do for local test implementation
        '''
        pass


    @staticmethod
    def _alive(p):
        return p and p.poll()==None


    @staticmethod
    def _kill(p):
        if not p:
            return
        while p.poll()==None:
            p.kill()
            time.sleep(1)


    def scheduler_alive(self):
        return LocalAstroInstance._alive(self.scheduler)


    def api_server_alive(self, api_server_id):
        return LocalAstroInstance._alive(self.api_servers.get(api_server_id, None))


    def worker_alive(self, uid):
        return LocalAstroInstance._alive(self.workers.get(uid, None))


    def kill_scheduler(self, timeout=120):
        LocalAstroInstance._kill(self.scheduler)
        self.scheduler = None


    def kill_api_server(self, api_server_id, timeout=120):
        p = self.api_servers.get(api_server_id, None)
        LocalAstroInstance._kill(p)


    def kill_worker(self, worker_id, timeout=120):
        p = self.workers.get(worker_id, None)
        LocalAstroInstance._kill(p)


    def tear_down(self):
        '''
        Tear down the astro instance, scheduler and workers, and
        cleanup (if reset = True) the DB and the MH (remove ES indexes,
        and Kafka topics used by the Astro instance).
        Set reset=False if you don't want to cleanup ES and MH.
        Set clear=False if you don't want to delete projects. Default removes
        all projects of the astro instance.
        '''
        if self.do_tear_down:
            print 'Tearing down man!' # debug
            if self.do_clear:
                print 'Clearing projects' # debug
                self.clear()

            print 'Killing the scheduler!' # debug
            self.kill_scheduler()

            for wid, _ in self.workers.iteritems():
                print 'Killing the worker', wid # debug
                self.kill_worker(wid)

            for sid, _ in self.api_servers.iteritems():
                print 'Killing the api server', sid # debug
                self.kill_api_server(sid)

            if self.do_reset:
                print 'Resetting db and mh' # debug
                self.reset()

    def get_group_id(self):
        '''
        Must return the Astro logical group name, the value set for the
        environment variable ASTRO_GROUP_ID.
        Local and Remote implementations must implement this method depending
        on the conventions they use to generate the logical group name.
        '''
        raise NotImplementedError('get_scheduler_name abstract method called')

