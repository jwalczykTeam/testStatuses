'''
Created on Nov 7, 2016

@author: adminibm
'''

import os
import subprocess
import time
from doi import constants
from doi_test.util.common import AstroInstance, repeat_request_until
from doi_test.util.cf_util import get_db_mh_credentials
from doi_test.util.containers import IBMContainersClient
from urllib3.exceptions import MaxRetryError


'''
__doi_dir = os.path.dirname(os.path.abspath(__import__('doi', fromlist='dummy').__file__))
_ScriptsDir = os.path.abspath(os.path.join(__doi_dir, '../../scripts'))
_RunGroup = os.path.join(_ScriptsDir, 'run_cf_group.sh')
'''


_DEFAULT_PYTHONPATH_EXT = '/usr/src/app/astro/python'
_SCHEDULER_PREFIX = 'astro-scheduler'
_API_SERVER_GROUP_PREFIX = 'astro-rest-api'
_WORKER_GROUP_PREFIX = 'astro-worker'
_DEFAULT_ASTRO_OUT_DIR = '/tmp/astro'


def _build_work_dir(wd, test_module, test_class, group_prefix):
    if test_module:
        wd = os.path.join(wd, test_module)
    if test_class:
        wd = os.path.join(wd, test_class)
    if group_prefix:
        wd = os.path.join(wd, group_prefix)
    return wd


class RemoteAstroInstance(AstroInstance):
    def __init__(self, conf, test_module=None, test_class=None):

        super(RemoteAstroInstance, self).__init__(conf, test_module, test_class)

        '''word_dir is where stdout/stderr of the scripts is written'''
        self.work_dir = _build_work_dir(conf.get('work_dir', '/tmp/astro'), test_module, test_class, self.group_name)

        self.scheduler = None
        self.api_server_group = None
        self.worker_groups = None

        ic_client = IBMContainersClient()
        self.groups_api = ic_client.groups()
        self.containers_api = ic_client.containers()

        '''
        Reading config and setting attributes
        '''
        setup_options = conf.get('setup', {})
        self.do_build = setup_options.get('build', True)
        self.do_deploy = setup_options.get('deploy', True)
        # Adjust tear down options if not setting up a new environment
        if not self.do_deploy:
            self.do_clear = self.do_reset = self.do_tear_down = False

        self.containerbridge_app = conf.get('containerbridge_app', 'containerbridge')
        self.image_name = conf.get('image_name', None)
        self.api_key = conf.get('api_key', None)

        _conf = conf.get('scheduler', {})
        self.scheduler_name = _conf.get('name', self._default_component_name(_SCHEDULER_PREFIX))
        self.scheduler_memory = _conf.get('memory', 512)
        self.scheduler_additional_env = _conf.get('additional_env', [])
        self.scheduler_work_dir = os.path.join(self.work_dir, 'scheduler')

        _conf = conf.get('api_group', {})
        self.api_server_group_name = _conf.get('name', self._default_component_name(_API_SERVER_GROUP_PREFIX))
        self.api_server_memory = _conf.get('memory', 512)
        self.api_server_group_domain = _conf.get('domain', 'mybluemix.net')
        self.api_server_group_host = _conf.get('host', self.api_server_group_name)
        self.api_server_group_port = _conf.get('port', 8080)
        self.api_server_additional_env = _conf.get('additional_env', [])
        self.api_servers_work_dir = os.path.join(self.work_dir, 'api-servers')

        _conf = conf.get('worker_group', {})
        self.worker_group_name = _conf.get('name', self._default_component_name(_WORKER_GROUP_PREFIX))
        self.worker_memory = _conf.get('memory', 512)
        self.worker_additional_env = _conf.get('additional_env', [])
        self.worker_out_dir = _conf.get('out_dir', _DEFAULT_ASTRO_OUT_DIR)
        self.workers_work_dir = os.path.join(self.work_dir, 'workers')

        self.group_id = self.scheduler_name

        if self.do_reset and (not self.db_config or not self.mb_config):
            # Credentials only needed if we have to reset Elasticsearch and MessageHub on tear down
            db_config, mh_config = get_db_mh_credentials(self.bluemix_info.target_api,
                                                         self.bluemix_info.access_token,
                                                         self.containerbridge_app)
            if not self.db_config:
                self.db_config = db_config
            if not self.mb_config:
                self.mb_config = mh_config


    def _default_component_name(self, prefix):
        name = prefix
        if self.group_name:
            name += '-' + self.group_name
        if self.bluemix_info.space_name:
            name += '-' + self.bluemix_info.space_name
        return name

    '''
    def _build_run_group_env(self, num_api_servers, num_workers):

        Example invocation:
            ./run_cf_group.sh -s dev -m 512 -M 512 -n 1 -N 2 registry.ng.bluemix.net/mkrudele/astro:1.0.1

        args = [_RunGroup]

        # Not interactive run
        args.append('-i')

        # scheduler + workers
        args.append('-n')
        args.append(str(num_api_servers))

        args.append('-N')
        args.append(str(num_workers))

        # Deploy only one Astro logical group (scheduler + api servers + workers)
        args.append('-G')
        args.append('1')

        if self.group_name:
            args.append('-g')
            args.append(self.group_name)

        if self.api_key:
            args.append('-k')
            args.append(self.api_key)

        if self.bluemix_info.space_name:
            args.append('-s')
            args.append(self.bluemix_info.space_name)

        if self.scheduler_memory:
            args.append('-l')
            args.append(str(self.scheduler_memory))

        if self.api_server_memory:
            args.append('-m')
            args.append(str(self.api_server_memory))

        if self.worker_memory:
            args.append('-M')
            args.append(str(self.worker_memory))

        if self.image_name:
            args.append(self.image_name)


        env = os.environ.copy()

        # Set this variable, it's not taken by the script as input parameter: num_worker_groups
        # TODO: need to fix this, may be pass as input param to the script!
        env['num_worker_groups'] = str(1)

        return (env, args)
    '''

    def build(self):
        '''
        Build Astro deployable artifacts.
        Remote implementation will build docker images and push onto a docker registry;
        Local implementation will probably only run a git clone.
        '''
        if self.do_build:
            # TODO: build the image, dude!
            pass
        pass


    def _build_common_env(self, component):
        env = []
        env.append(u'COMPONENT=' + component)
        env.append(u'PYTHONPATH=' + _DEFAULT_PYTHONPATH_EXT)
        env.append(constants.ASTRO_GROUP_ID_ENV_VAR + '=' + self.group_id)
        return env

    def _build_scheduler_env(self):
        '''
        Create the environment for the scheduler
        '''
        env = self._build_common_env('scheduler')

        '''
        TODO: allow configuring other variables (see scheduler_app.py)
        '''
        return env + self.scheduler_additional_env

    def _build_worker_group_env(self):
        '''
        Create the environment for the workers
        '''
        env = self._build_common_env('worker')

        env.append(constants.ASTRO_OUT_DIR_ENV_VAR + '=' + self.worker_out_dir)

        '''
        Note that the ASTRO_WORKER_ID is set by the docker-entrypoint.sh
        script to the unique id of the hosting container.
        TODO: need to allow setting the slack webhook
        TODO: need to allow setting other variables (see worker_app.py)
        '''

        return env + self.worker_additional_env

    def _build_api_server_group_env(self):
        '''
        Create the environment for the api servers
        '''
        env = self._build_common_env('api')

        env.append(constants.ASTRO_API_KEY_ENV_VAR + '=' + self.api_key)
        env.append(constants.ASTRO_BLUEMIX_API_ENDPOINT_ENV_VAR + '=' + self.bluemix_info.target_api)
        env.append(constants.ASTRO_API_CONFIG_ENV_VAR + '=\'{"port":' + str(self.api_server_group_port) + '}\'')

        return env + self.api_server_additional_env


    def _deploy(self, num_api_servers, num_workers, max_running_jobs, timeout):
        '''
        Create and start the scheduler
        '''
        scheduler_env = self._build_scheduler_env()
        print '>> Creating scheduler. name = {}, image = {}, memory = {}, containerbridge = {}, env = {}' \
            .format(self.scheduler_name, self.image_name, self.scheduler_memory,
                    self.containerbridge_app, scheduler_env)
        status, data = self.containers_api.create(self.scheduler_name,
                                                  self.image_name,
                                                  memory=self.scheduler_memory,
                                                  env=scheduler_env,
                                                  containerbridge_app=self.containerbridge_app)
        assert status in [200, 201], '>> Cannot create scheduler container: {}, {}'.format(status, data)

        # data contains the 'Id', which is the only thing we need
        self.scheduler = data

        '''
        Create and start api server group
        '''
        api_server_group_env = self._build_api_server_group_env()
        print '>> Creating api server group. name = {}, image = {}, desired_instances = {} \
                autorecovery = {}, antiaffinity = {}, port = {}, host = {}, domain = {}, memory = {}, containerbridge = {}, env = {}' \
            .format(self.api_server_group_name, self.image_name, num_api_servers,
                    True, True, self.api_server_group_port, self.api_server_group_host,
                    self.api_server_group_domain, self.api_server_memory,
                    self.containerbridge_app, api_server_group_env)
        status, data = self.groups_api.create(self.api_server_group_name,
                                              self.image_name,
                                              desired_instances=num_api_servers,
                                              autorecovery=True,
                                              antiaffinity=True,
                                              port=self.api_server_group_port,
                                              host=self.api_server_group_host,
                                              domain=self.api_server_group_domain,
                                              memory=self.api_server_memory,
                                              env=api_server_group_env,
                                              containerbridge_app=self.containerbridge_app)
        assert status in [200, 201], 'Cannot create api server group: {}, {}'.format(status, data)

        # data contains the 'Id', which is the only thing we need
        self.api_server_group = data

        '''
        Create and start worker groups; only one at the moment
        '''
        worker_group_env = self._build_worker_group_env()
        print '>> Creating worker group. name = {}, image = {}, desired_instances = {} \
                autorecovery = {}, antiaffinity = {}, memory = {}, containerbridge = {}, env = {}' \
            .format(self.worker_group_name, self.image_name, num_workers,
                    False, True, self.worker_memory, self.containerbridge_app, worker_group_env)
        status, data = self.groups_api.create(self.worker_group_name,
                                              self.image_name,
                                              desired_instances=num_workers,
                                              autorecovery=False,
                                              antiaffinity=True,
                                              memory=self.worker_memory,
                                              env=worker_group_env,
                                              containerbridge_app=self.containerbridge_app)
        assert status in [200, 201], 'Cannot create worker group: {}, {}'.format(status, data)

        # data contains the 'Id', which is the only thing we need
        self.worker_groups.append(data)

        '''
        Wait for the Routes to be provisioned and set the self.api_ep variable.
        Check the object 
          "Route_Status": {
            "successful": true,
            "in_progress": false,
            "message": "registered route successfully"
          },
        '''
        def check_route_status(status_data):
            '''
            Check the status code and the Route_Status.successful == true
            '''
            if status_data[0] == 200:
                route_status = status_data[1].get('Route_Status', None)
                if route_status:
                    success = route_status.get('successful', None)
                    if success:
                        routes = status_data[1].get('Routes', None)
                        if routes and len(routes) > 0:
                            return (True, routes[0])

            return (False, None)

        @repeat_request_until(check_route_status, poll=10, timeout=timeout)
        def get_route():
            try:
                return self.groups_api.get(self.api_server_group['Id'])
            except MaxRetryError, e:
                return (404, e)

        for t in get_route():
            print "\tTick (Wait for route in {} seconds)".format(t[0])
            if t[1]:
                route = t[1]

        self.api_ep = "http://{}/v1".format(route)


    def start(self, num_api_servers=1, num_workers=1, max_running_jobs=None, timeout=120):
        '''
        Start a complete astro instance with 1 scheduler and num_workers configured
        to talk to the scheduler. You can also configure num_api_servers to start.
        '''
        assert not self.scheduler
        assert not self.api_server_group
        assert not self.worker_groups

        try:
            if not os.path.exists(self.work_dir):
                os.makedirs(self.scheduler_work_dir)
                os.makedirs(self.workers_work_dir)
                os.makedirs(self.api_servers_work_dir)

            self.scheduler = {}
            self.api_server_group = {}
            self.worker_groups = []

            if self.do_deploy:
                self._deploy(num_api_servers, num_workers, max_running_jobs, timeout)

            api = self.astro_api()

            '''
            Wait for the Astro API being responsive
            '''
            def check_status(status_data):
                return (status_data[0]==200, None)

            @repeat_request_until(check_status, poll=5, timeout=timeout)
            def test_api():
                try:
                    return api.do_request(method='GET', path='/config', as_admin=True)
                except MaxRetryError, e:
                    return (404, e)

            for _ in test_api():
                pass

        except AssertionError, e:
            print '>> Got an error, tearing down the environment...'

            self.tear_down()
            self.scheduler = None
            self.api_server_group = None
            self.worker_groups = None

            raise e


    def start_ex(self, num_api_servers=1, num_workers=1, max_running_jobs=None, timeout=120):
        '''
        Start a complete astro instance with 1 scheduler and num_workers configured
        to talk to the scheduler. You can also configure num_api_servers to start
        Execute the run_cf_group.sh script with the right environment and the right
        parameters
        '''
        assert not self.scheduler

        script_env, script_args = self._build_run_group_env(num_api_servers, num_workers)

        if not os.path.exists(self.work_dir):
            os.makedirs(self.work_dir)
            os.makedirs(self.scheduler_work_dir)
            os.makedirs(self.workers_work_dir)
            os.makedirs(self.api_servers_work_dir)

        log_file = os.path.join(self.work_dir, 'run_cf_group.log')

        with open(log_file, 'w') as logf:
            run_group = subprocess.Popen(args=script_args, shell=False,
                                         stderr=subprocess.STDOUT, stdout=logf,
                                         cwd=self.work_dir, env=script_env, close_fds=True)
            rc = run_group.wait()

        assert rc == 0

        self.scheduler = {}
        self.api_server_group = {}
        self.worker_groups = []

        state, groups = self.groups_api.list()
        assert state == 200

        for g in groups:
            gname = g['Name']
            if self.group_name in gname and self.bluemix_info.space_name in gname:
                if 'rest-api' in gname:
                    self.api_server_group = g
                elif 'worker' in gname:
                    self.worker_groups.append(g)
                else:
                    assert False, 'Unexpected group found {}: it is neither a rest-api or a worker group.'.format(g)

        assert len(self.api_server_group.keys())>0, 'No api server group found for group {} and space_name {}.'.format(self.group_name, self.bluemix_info.space_name)
        assert len(self.worker_groups)>0, 'No worker groups found for group{} and space_name {}'.format(self.group_name, self.bluemix_info.space_name)

        state, containers = self.containers_api.list(anystate=True)
        assert state == 200

        for c in containers:
            cname = c['Name']
            if self.group_name in cname and self.bluemix_info.space_name in cname:
                if 'scheduler' in cname:
                    self.scheduler = c
                    break

        assert len(self.scheduler.keys()) > 0, 'Scheduler container not found for group {} and space_name {}.'.format(self.group_name, self.bluemix_info.space_name)

        # Set the astro group id now to the name of the scheduler container, which is set as ASTRO_GROUP_ID
        self.group_id = self.scheduler['Name']

        '''
        Wait for the Routes to be provisioned and set the self.api_ep variable.
        Check the object 
          "Route_Status": {
            "successful": true,
            "in_progress": false,
            "message": "registered route successfully"
          },
        '''
        def check_route_status(status_data):
            '''
            Check the status code and the Route_Status.successful == true
            '''
            if status_data[0] == 200:
                route_status = status_data[1].get('Route_Status', None)
                if route_status:
                    success = route_status.get('successful', None)
                    if success:
                        routes = status_data[1].get('Routes', None)
                        if routes and len(routes) > 0:
                            return (True, routes[0])

            return (False, None)

        @repeat_request_until(check_route_status, poll=10, timeout=timeout)
        def get_route():
            try:
                return self.groups_api.get(self.api_server_group['Name'])
            except MaxRetryError, e:
                return (404, e)

        for t in get_route():
            print "\tTick (Wait for route in {} seconds)".format(t[0])
            if t[1]:
                route = t[1]

        self.api_ep = "http://{}/v1".format(route)

        api = self.astro_api()

        '''
        Wait for the Astro API being responsive
        '''
        def check_status(status_data):
            return (status_data[0]==200, None)

        @repeat_request_until(check_status, poll=5, timeout=timeout)
        def test_api():
            try:
                return api.do_request(method='GET', path='/config', as_admin=True)
            except MaxRetryError, e:
                return (404, e)

        for _ in test_api():
            pass

    def _is_container_running(self, container_id):
        status, data = self.containers_api.status(container_id)
        return status in [200, 201] and data['Status'].lower() == 'running'

    def _wait_on_container_states(self, container_id, states, timeout=120):
        seconds = 1
        while seconds < timeout:
            _, data = self.containers_api.status(container_id)
            container_status = data.get('Status', '')
            if container_status.lower() in states:
                return container_status
            else:
                time.sleep(seconds)
                seconds *= 2

        raise Exception('Timeout ({} seconds) expired'.format(timeout))

    def _start_container_and_wait(self, container_id, timeout):
        status, _ = self.containers_api.start(container_id)
        if status in [200, 204]:
            self._wait_on_container_states(container_id, ['running'], timeout=timeout)

    def _kill_container_and_wait(self, container_id, timeout):
        status, _ = self.containers_api.stop(container_id)
        if status in [200, 204]:
            self._wait_on_container_states(container_id, ['shutdown'], timeout=timeout)

    def _remove_container_and_wait(self, container_id, timeout):
        status, _ = self.containers_api.remove(container_id, force=True)
        if status in [200, 204]:
            seconds = 1
            while seconds < timeout:
                time.sleep(seconds)
                status, _ = self.containers_api.status(container_id)
                if status != 404:
                    seconds *= 2
                else:
                    return

            raise Exception('Timeout ({} seconds) expired'.format(timeout))

    def _remove_group_and_wait(self, group_id, timeout):
        status, data = self.groups_api.remove(group_id, force=True)
        if status in [200, 204]:
            seconds = 1
            while seconds < timeout:
                time.sleep(seconds)
                status, data = self.groups_api.get(group_id)
                if status != 404:
                    '''
                    Sometimes the group ends in a delete failed state.
                    In that case let's send again a remove.
                    '''
                    if data.get('Status').lower() == 'delete_failed':
                        self.groups_api.remove(group_id, force=True)
                        seconds = 1
                    else:
                        seconds *= 2
                else:
                    return

            raise Exception('Timeout ({} seconds) expired'.format(timeout))

    def start_scheduler(self, timeout=120):
        assert self.scheduler['Id']
        return self._start_container_and_wait(self.scheduler['Id'], timeout)

    def start_worker(self, worker_id=None, timeout=120, max_running_jobs=None):
        assert worker_id
        return self._start_container_and_wait(worker_id, timeout)

    def scheduler_alive(self):
        assert self.scheduler['Id']
        return self._is_container_running(self.scheduler['Id'])

    def worker_alive(self, worker_id):
        '''
        The worker_id is the container id so I can call the containers_api
        to check the status of the worker
        '''
        assert worker_id
        return self._is_container_running(worker_id)

    def get_workers(self):
        '''
        Return an array of worker_id
        '''
        workers = []
        if self.worker_groups:
            for group in self.worker_groups:
                _, data = self.containers_api.list(anystate=True, by_group=group['Id'])
                if data:
                    workers += data

        return [x['Id'] for x in workers]

    def get_api_servers(self):
        '''
        Return an array of api_server_id
        '''
        status = 404
        if self.api_server_group:
            status, data = self.containers_api.list(anystate = True, by_group = self.api_server_group['Id'])
        return [x['Id'] for x in data] if status == 200 else []

    def kill_worker(self, worker_id, timeout=120):
        '''
        The worker_id is the container id so I can call the containers_api
        to stop it
        '''
        assert worker_id
        return self._kill_container_and_wait(worker_id, timeout)

    def kill_scheduler(self, timeout=120):
        assert self.scheduler['Id']
        return self._kill_container_and_wait(self.scheduler['Id'], timeout)

    def kill_api_server(self, api_server_id, timeout=120):
        assert api_server_id
        return self._kill_container_and_wait(api_server_id, timeout)

    def _write_container_logs(self, name_or_id, filename):
        with open(filename, 'w') as f:
            status, logs = self.containers_api.get_logs(name_or_id)
            if status == 200:
                f.write(logs)
                del logs
            else:
                print '>> Cannot get logs for container {}. Error: {}'.format(name_or_id, logs)

    def _collect_logs(self):
        if self.scheduler:
            scheduler_id = self.scheduler['Id']
            scheduler_log_file = os.path.join(self.scheduler_work_dir, scheduler_id + '.log')
            self._write_container_logs(scheduler_id, scheduler_log_file)

        for worker_id in self.get_workers():
            worker_log_file = os.path.join(self.workers_work_dir, worker_id + '.log')
            self._write_container_logs(worker_id, worker_log_file)

        for api_server_id in self.get_api_servers():
            api_server_log_file = os.path.join(self.api_servers_work_dir, api_server_id + '.log')
            self._write_container_logs(api_server_id, api_server_log_file)

    def tear_down(self):
        '''
        Tear down the astro instance, scheduler and worker_groups, and
        cleanup (if reset = True) the DB and the MH (remove ES indexes,
        and Kafka topics used by the Astro instance).
        Set reset=False if you don't want to cleanup ES and MH.
        Set clear=False if you don't want to delete projects. Default removes
        all projects of the astro instance.
        '''
        if self.do_tear_down:
            print '>> collecting containers logs...' # debug
            self._collect_logs()

            if self.do_clear:
                print '>> removing projects...' # debug
                self.clear()

            # Remove scheduler container if there
            if self.scheduler:
                print '>> removing scheduler container {} ...'.format(self.scheduler['Id']) # debug
                self._remove_container_and_wait(self.scheduler['Id'], timeout=1200)
                self.scheduler = None

            # Remove worker groups if there
            if self.worker_groups:
                for group in self.worker_groups:
                    print '>> removing worker group {} ...'.format(group['Id'])
                    self._remove_group_and_wait(group['Id'], timeout=1200)
                self.worker_groups = None

            if self.api_server_group:
                print '>> removing api server group {} ...'.format(self.api_server_group['Id'])
                self._remove_group_and_wait(self.api_server_group['Id'], timeout=1200)
                self.api_server_group = None

            '''
            TODO: Do we need to remove the routes of the api servers group ?
            '''

            if self.do_reset:
                print '>> cleaning up Database and MessageHub ...' # debug
                self.reset()
