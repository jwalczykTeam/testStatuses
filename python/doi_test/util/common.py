'''
Created on Nov 7, 2016

@author: adminibm
'''

import os, re, json, requests, time, functools, base64
import doi.constants as constants
from doi_test.util.cf_util import get_cf_login_info


_ASTRO_AUTH_TENANT_ID = 'X-Auth-Tenant-Id'
_ASTRO_AUTH_TOKEN = 'X-Auth-Token'


ACCESS_TOKEN_ENV_VAR = 'ACCESS_TOKEN'
TENANT_ID_ENV_VAR = 'TENANT_ID'


def load_data_file(jsonfile):
    if not jsonfile.endswith('.json'):
        jsonfile += '.json'

    with open(jsonfile, 'r') as fp:
        jsonstring = fp.read()

    p = re.compile('\${([^\${}]+)}')

    all_matches = set(p.findall(jsonstring))

    for m in all_matches:
        m_val = os.environ.get(m, None)
        assert m_val is not None, 'Undefined environment variable {}'.format(m)
        mp = re.compile('\${' + m + '}')
        jsonstring = mp.sub(m_val, jsonstring)

    return json.loads(jsonstring)


def repeat_request_until(condition, poll=2, timeout=3600):
    '''
    A decorator that allows repeating a project request until
    a given condition is satisfied.
    The condition is a function that takes as input the
    output of the request (i.e. (status, data) tuple) and returns
    a tuple (True|False, custom data)
    '''
    def decorator(func):
        @functools.wraps(func)
        def repeat_func(*args, **kwargs):
            done = False
            seconds = 0
            while not done and seconds < timeout:
                done, data = condition(func(*args, **kwargs))
                if not done:
                    time.sleep(poll)
                    seconds += poll

                yield (seconds, data)

            assert done, 'Timeout ({} seconds) expired'.format(timeout)
        return repeat_func
    return decorator


class BluemixInfo:
    '''
    Collect and stores Bluemix and ICS target information
    '''
    def _get_bluemix_auth_info(self):
        def build_response(r):
            try:
                j = r.json()
            except ValueError:
                j = {"message": r.text}
            return (r.status_code, j)

        info_url = "{}/v2/info".format(self.target_api)
        status, data = build_response(requests.get(info_url))
        assert status == 200
        auth_ep = data.get('authorization_endpoint')
        assert auth_ep

        auth_ep = '{}/oauth/token'.format(auth_ep)
        headers = {'Accept-Encoding': 'application/json',
                   'Authorization': 'basic {}'.format(base64.b64encode('cf:')),
                   'Content-type': 'application/x-www-form-urlencoded'}
        body = 'grant_type=password&password={}&scope=&username={}'.format(self.password,
                                                                           self.username)
        status, data = build_response(requests.post(auth_ep,
                                                    headers=headers,
                                                    data=body))
        assert status == 200
        self.access_token = data['access_token']
        assert self.access_token

        orgs_url = '{}/v2/organizations?q=name:{}'.format(self.target_api, self.org_name)
        headers = {'Authorization': 'bearer {}'.format(self.access_token)}
        status, data = build_response(requests.get(orgs_url, headers=headers))
        assert status == 200
        orgs = data['resources']
        assert len(orgs) == 1, 'orgs {}'.format(orgs)
        self.org_id = orgs[0]['metadata']['guid']
        assert self.org_id


    def __init__(self, rawinfo=None):
        '''
        If rawinfo is given with username, password, then
        access token and organization id are taken using
        user's credentials, otherwise they are taken from the
        ./cf/config.json file, which requires you are logged
        in the shell executing this code.
        '''
        if not rawinfo:
            rawinfo = {}

        self.access_token = self.org_id = None
        self.target_api = rawinfo.get('target_api')
        self.org_name = rawinfo.get('org_name')
        self.space_name = rawinfo.get('space_name')
        self.username = rawinfo.get('username')
        self.password = rawinfo.get('password')

        if self.target_api and self.username and self.password and self.org_name:
            self._get_bluemix_auth_info()
        else:
            rawinfo = get_cf_login_info()
            self.target_api = rawinfo['Target']
            self.access_token = rawinfo['AccessToken'].split()[1]
            self.org_name = rawinfo['OrganizationFields']['Name']
            self.org_id = rawinfo['OrganizationFields'].get('GUID')
            if not self.org_id:
                self.org_id = rawinfo['OrganizationFields'].get('Guid')
            self.space_name = rawinfo['SpaceFields']['Name']

    def __repr__(self):
        return 'target_api={}, org_name={}, org_id={}, space_name={}, username={}'.format(
            self.target_api, self.org_name, self.org_id, self.space_name, self.username)

class AstroInstance(object):

    def __init__(self, conf, test_module=None, test_class=None):
        self.group_name = conf.get('group_name', None)
        self.api_ep = conf.get('api_ep', None)
        self.api_port = int(conf.get('api_port', "8080"))
        self.api_key = conf.get('api_key', None)
        self.db_config = conf.get('db_config', None)
        self.mb_config = conf.get('mh_config', None)
        self.bluemix_info = BluemixInfo(conf.get('bluemix_info'))
        self.test_module = test_module
        self.test_class = test_class
        tear_down_options = conf.get('tear_down', {})
        self.do_tear_down = tear_down_options.get('run', True)
        self.do_clear = tear_down_options.get('clear', True)
        self.do_reset = tear_down_options.get('reset', True)

        '''
        It must be set by the Local and Remote implementation, depending on the conventions used to generate it.
        It must be the value of the env variable ASTRO_GROUP_ID.
        '''
        self.group_id = None

        '''
        if self.bluemix_info and self.bluemix_info['username'] and self.bluemix_info['password'] and self.bluemix_info['org_name']:
            self.access_token, self.tenant_id = AstroInstance._get_bluemix_auth_info(self.bluemix_info)
        '''

    def build(self):
        '''
        Build Astro deployable artifacts.
        Remote implementation will build docker images and push onto a docker registry;
        Local implementation will probably only run a git clone.
        '''
        raise NotImplementedError('build abstract method called')

    def start(self, num_api_servers=1, num_workers=1, max_running_jobs=None, timeout=120):
        '''
        Start a complete astro instance with 1 api server and num_workers configured
        to talk to the scheduler.
        '''
        raise NotImplementedError('start abstract method called')

    def start_api_server(self, index=None, timeout=120):
        raise NotImplementedError('start_api_server abstract method called')

    def api_server_alive(self, api_server_id):
        raise NotImplementedError('api_server_alive abstract method called')

    def kill_api_server(self, api_server_id, timeout=120):
        raise NotImplementedError('kill_api_server abstract method called')

    def start_scheduler(self, timeout=120):
        raise NotImplementedError('start_scheduler abstract method called')

    def scheduler_alive(self):
        raise NotImplementedError('scheduler_alive abstract method called')

    def kill_scheduler(self, timeout=120):
        raise NotImplementedError('kill_scheduler abstract method called')

    def start_worker(self, wid=None, timeout=120, max_running_jobs=None):
        raise NotImplementedError('start_worker abstract method called')

    def worker_alive(self, worker_id):
        raise NotImplementedError('worker_alive abstract method called')

    def kill_worker(self, worker_id, timeout=120):
        raise NotImplementedError('kill_worker abstract method called')

    def get_workers(self):
        '''
        Return an array of worker_id
        '''
        raise NotImplementedError('get_workers abstract method called')

    def tear_down(self):
        '''
        Tear down the astro instance, scheduler, api servers, workers, and
        cleanup (if reset = True) the DB and the MH (remove ES indexes,
        and Kafka topics used by the Astro instance).
        Set reset=False if you don't want to cleanup ES and MH.
        Set clear=False if you don't want to delete projects. Default removes
        all projects of the astro instance.
        '''
        raise NotImplementedError('tear_down abstract method called')

    def clear(self):
        '''
        Gracefully remove all Astro projects, including ES, DB, and Kibana
        resources.
        '''
        api = self.astro_api()
        status, all_projects = api.list_projects()

        if status == 200:
            for p in all_projects:
                api.delete_project(p['name'])

            try:
                for p in all_projects:
                    for _ in api.wait_project_on_http_status_change(p['name'], \
                                                 [404], poll=10, timeout=300):
                        pass
            except AssertionError:
                pass

    def reset(self, reset_db=True, reset_mb=True):
        '''
        Reset Astro instance to just-after-deployment state.
        Default is to delete DB and MessageHub data
        '''
        if reset_db and self.db_config:
            from doi.util.db.db_util import Database
            db_client = Database.factory(self.db_config)
            scheduler_db_name = constants.get_scheduler_db_name(self.group_id)

            db_client.delete_database(scheduler_db_name)

        if reset_mb and self.mb_config:
            scheduler_messages_topic_name = \
                constants.get_scheduler_messages_topic_name(self.group_id)
            workers_messages_topic_name = \
                constants.get_workers_messages_topic_name(self.group_id)
            resource_db_events_topic_name = \
                constants.get_resource_db_events_topic_name(self.group_id)
            from doi.util.mb.mb_util import MessageBroker
            mb = MessageBroker(self.mb_config)
            mb_manager = mb.new_manager()
            mb_manager.delete_topic(scheduler_messages_topic_name)
            mb_manager.delete_topic(workers_messages_topic_name)
            mb_manager.delete_topic(resource_db_events_topic_name)

    def astro_api(self, api_ep=None):
        return self.Api(self, api_ep)

    class Api(object):
        def __init__(self, outer, api_ep=None):
            self.api_ep = api_ep if api_ep else outer.api_ep
            self.api_key = outer.api_key
            self.access_token = 'Bearer ' + outer.bluemix_info.access_token
            self.tenant_id = outer.bluemix_info.org_id
            assert self.api_ep

        def do_request(self,
                       method='GET',
                       path='/',
                       body=None, as_admin=False):
            url = "{}{}".format(self.api_ep, path)
            headers = {'Accept': 'application/json'}

            if as_admin:
                headers[_ASTRO_AUTH_TOKEN] = self.api_key
            else:
                headers[_ASTRO_AUTH_TOKEN] = self.access_token
                headers[_ASTRO_AUTH_TENANT_ID] = self.tenant_id

            if body is not None:
                headers['Content-type'] = 'application/json'

            if type(body) == dict:
                body = json.dumps(body)

            r = requests.request(method, url, headers=headers, data=body)
            try:
                j = r.json()
            except ValueError:
                j = {"message": r.text}

            return (r.status_code, j)

        def list_projects(self, as_admin=False):
            return self.do_request(method='GET',
                                   path='/projects',
                                   as_admin=as_admin)

        def list_metrics(self, as_admin=False):
            return self.do_request(method='GET',
                                   path='/metrics',
                                   as_admin=as_admin)

        def get_metric(self, project_name, as_admin=False):
            assert project_name, 'Project name not set'
            return self.do_request(method='GET',
                                   path='/metrics/' + project_name,
                                   as_admin=as_admin)

        def post_project(self, body, as_admin=False):
            assert body, 'Project payload not set'
            return self.do_request(method='POST',
                                   path='/projects',
                                   body=body,
                                   as_admin=as_admin)

        def update_project(self, project_name, body, as_admin=False):
            assert project_name, 'Project name not set'
            assert body, 'Project payload not set'
            return self.do_request(method='PUT',
                                   path='/projects/' + project_name,
                                   body=body,
                                   as_admin=as_admin)

        def get_project(self, project_name, as_admin=False):
            assert project_name, 'Project name not set'
            return self.do_request(method='GET',
                                   path='/projects/' + project_name,
                                   as_admin=as_admin)

        def wait_project_on_property_change(self,
                                            project_name,
                                            project_key,
                                            wait_until_values,
                                            poll=5,
                                            timeout=3600,
                                            as_admin=False):
            '''
            Wait until the the value of project_key (e.g. current_state, current_stage)
            of the project_name is in wait_until_values list, then return.
            yield each value transition in the form: old_val, cur_val, transition_time
            '''
            assert project_name, 'Project name not set'
            assert project_key, 'Project key not set'
            assert type(wait_until_values)==type(list()), 'Project key values must be a list'
            done = False
            old_val = cur_val = None
            seconds = 0
            while not done and seconds < timeout:
                status, data = self.get_project(project_name, as_admin=as_admin)
                if status!=200:
                    print('>> WARNING! Project {} not found. Status: {}, data: {}. It may be a temporary error.'.format(project_name, status, data))
                else:
                    cur_val = data[project_key]
                    if old_val != cur_val:
                        yield (old_val, cur_val, seconds)
                        old_val = cur_val

                if cur_val not in wait_until_values:
                    time.sleep(poll)
                    seconds += poll
                else:
                    done = True

            assert done, 'Timeout {} expired'.format(timeout)


        def wait_project_on_http_status_change(self, project_name, wait_until_values, poll=5, timeout=3600, as_admin=False):
            '''
            Wait until the the value of the http status of the get_project call
            is one of the values in the wait_until_values list, then return.
            yield each value transition in the form: old_val, cur_val, transition_time
            '''
            assert project_name, 'Project name not set'
            assert type(wait_until_values)==type(list()), 'Http status values must be a list'
            done=False
            old_val = None
            seconds = 0
            while not done and seconds < timeout:
                status, _ = self.get_project(project_name, as_admin=as_admin)

                cur_val = status
                if old_val != cur_val:
                    yield (old_val, cur_val, seconds)
                    old_val = cur_val

                if cur_val not in wait_until_values:
                    time.sleep(poll)
                    seconds += poll
                else:
                    done = True

            assert done, 'Timeout {} expired'.format(timeout)


        def stop_project(self, project_name, as_admin=False):
            assert project_name, 'Project name not set'
            return self.do_request(method='PATCH', path='/projects/' + project_name, body='{"desired_state": "stopped"}', as_admin = as_admin)


        def start_project(self, project_name, as_admin=False):
            assert project_name, 'Project name not set'
            return self.do_request(method = 'PATCH', path='/projects/' + project_name, body='{"desired_state": "successful"}', as_admin = as_admin)


        def delete_project(self, project_name, as_admin=False):
            assert project_name, 'Project name not set'
            return self.do_request(method = 'DELETE', path='/projects/' + project_name, as_admin = as_admin)
