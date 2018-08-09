'''
Created on Jan 27, 2017

@author: adminibm
'''

import os, functools, time, requests, docker
from docker.utils import kwargs_from_env
from doi_test.util.cf_util import get_cf_login_info

'''
'https://containers-api.ng.bluemix.net/v3'
'''

'''
Errors emitted by CloudFoundry infrastructure that may be temporary.
It is worth to retry, then.
'''
_INFRASTRUCTURE_ERRORS = [500, 501, 502, 503, 504, 524]

def _retriable(retry_codes, init_poll=0.5, timeout=600):
    '''
    A decorator that allows retrying a request while the status_code is in
    the retry list. A requests.response is expected as return value of the
    decorated function.
    This is intended for handling temporary errors of back-end systems. For
    example Bluemix returning 502 - Bad gateway.
    A back-off exponential algorithm is applied, so each retry is performed
    almost doubling the last polling interval.
    You can set a maximum amount of time to wait on the back-end system to
    respond: timeout parameter.
    '''
    def _build_response(r):
        try:
            j = r.json()
        except ValueError:
            j = {"message": r.text}
        return (r.status_code, j)

    def decorator(func):
        @functools.wraps(func)
        def repeat_func(*args, **kwargs):
            seconds = init_poll
            while seconds < timeout:
                status, data = _build_response(func(*args, **kwargs))
                if status in retry_codes:
                    print ">> Retrying in {} seconds. Status: {}, data: {}".format(seconds, status, data) # debug
                    time.sleep(seconds)
                    seconds *= 2

                else:
                    return (status, data)

            raise Exception('Timeout ({} seconds) expired'.format(timeout))

        return repeat_func
    return decorator

class IBMContainersClient(object):
    def __init__(self, host=None, auth_token=None, project_id=None):
        if not auth_token or not project_id or not host:
            l = get_cf_login_info()
            auth_token = l['AccessToken'].split()[1]
            project_id = l['SpaceFields'].get('GUID')
            if not project_id:
                project_id = l['SpaceFields'].get('Guid')
            host = l['Target']
            i = host.find('://')
            host = os.path.join(host[:i+3] + 'containers-' + host[i+3:], 'v3')

        self.host = host
        self.headers = {
            'X-Auth-Token': auth_token,
            'X-Auth-Project-Id': project_id
        }

    def groups(self):
        return self.Groups(self)

    def containers(self):
        return self.Containers(self)


    class Groups(object):
        def __init__(self, outer):
            self.api_ep = outer.host + '/containers/groups'
            self.headers = outer.headers

        @_retriable(retry_codes=_INFRASTRUCTURE_ERRORS+[404])
        def list(self):
            '''
            List all groups. Same as `cf ic group list`
            '''
            return requests.get(self.api_ep, headers=self.headers)

        @_retriable(retry_codes=_INFRASTRUCTURE_ERRORS)
        def get(self, name_or_id):
            '''
            Inspect one group. Same as `cf ic group inspect
            '''
            return requests.get(self.api_ep + '/{}'.format(name_or_id), headers=self.headers)

        @_retriable(retry_codes=_INFRASTRUCTURE_ERRORS)
        def remove(self, name_or_id, force=False):
            '''
            Destroy a group. Same as `cf ic group rm`
            '''
            params = {'force': 'true' if force else 'false'}
            return requests.delete(self.api_ep + '/{}'.format(name_or_id), headers=self.headers, params=params)

        @_retriable(retry_codes=_INFRASTRUCTURE_ERRORS)
        def create(self, name, image_name,
                   min_instances=1, desired_instances=1, max_instances=1,
                   autorecovery=False, antiaffinity=False, port=None,
                   host=None, domain=None,
                   memory=64, env=[], containerbridge_app=None):
            '''
            Create a new containers group. Same as `cf ic group create`.
            '''
            if desired_instances < min_instances:
                desired_instances = min_instances
            if max_instances < desired_instances:
                max_instances = desired_instances

            body = {
                "Name": name,
                "Memory": memory,
                "Env": env,
                "Image": image_name,
                "NumberInstances": {
                    "Min": min_instances,
                    "Desired": desired_instances,
                    "Max": max_instances
                },
                "AutoRecovery": autorecovery,
                "AntiAffinity": antiaffinity
            }

            if containerbridge_app:
                body['BluemixApp'] = containerbridge_app

            if port:
                body['Port'] = port

            if host and domain:
                body['Route'] = {
                    "domain": domain,
                    "host": host
                }

            return requests.post(self.api_ep, headers=self.headers, json=body)

    class Containers(object):
        def __init__(self, outer):
            self.api_ep = outer.host + '/containers'
            self.headers = outer.headers

        def list(self, anystate=False, by_group=None):
            '''
            List all containers. Same as `cf ic ps [-a]`
            If you want to get only instances of a given group,
            set the parameter by_group to name_or_id of the group.
            '''
            @_retriable(retry_codes=_INFRASTRUCTURE_ERRORS+[404])
            def request(anystate):
                params = {'all': 'true' if anystate else 'false'}
                return requests.get(self.api_ep + '/json', headers=self.headers, params=params)

            status, data = request(anystate)
            if status == 200 and by_group:
                def in_group(c, g):
                    cg = c.get('Group', None)
                    return g in [cg['Name'], cg['Id']] if cg else False

                data = filter(lambda x: in_group(x, by_group), data)

            return (status, data)

        @_retriable(retry_codes=_INFRASTRUCTURE_ERRORS)
        def get(self, name_or_id):
            '''
            Inspect a container. Same as `cf ic inspect`
            '''
            return requests.get(self.api_ep + '/{}/json'.format(name_or_id), headers=self.headers)

        @_retriable(retry_codes=_INFRASTRUCTURE_ERRORS)
        def status(self, container_id):
            '''
            Returns the status of a container. Same as `cf ic ps`.
            '''
            return requests.get(self.api_ep + '/{}/status'.format(container_id), headers=self.headers)

        @_retriable(retry_codes=_INFRASTRUCTURE_ERRORS)
        def remove(self, name_or_id, force=False):
            '''
            Remove a container. Same as `cf ic rm`.
            '''
            params = {'force': 'true' if force else 'false'}
            return requests.delete(self.api_ep + '/{}'.format(name_or_id), headers=self.headers, params=params)

        @_retriable(retry_codes=_INFRASTRUCTURE_ERRORS)
        def stop(self, name_or_id, time=1):
            '''
            Stop a container. Same as `cf ic stop`.
            '''
            params = { 't': time }
            return requests.post(self.api_ep + '/{}/stop'.format(name_or_id), headers=self.headers, params=params)

        @_retriable(retry_codes=_INFRASTRUCTURE_ERRORS)
        def start(self, name_or_id):
            '''
            Start an existing container. Same as `cf ic start`.
            '''
            return requests.post(self.api_ep + '/{}/start'.format(name_or_id), headers=self.headers)

        def get_logs(self, name_or_id, tail='all'):
            '''
            Returns the logs of a container. Default is to get the
            entire log, but you can tail it.
            This uses docker-py library initialized from the
            environment. So, be sure you set the right env variables.
            '''
            status = 200
            logs = None
            try:
                params = kwargs_from_env()
                client = docker.APIClient(**params)
                logs = client.logs(name_or_id, stream=False, stdout=True, stderr=True, tail=tail)
            except Exception, e:
                status = 404
                logs = {'message': str(e)}

            return (status, logs)

        @_retriable(retry_codes=_INFRASTRUCTURE_ERRORS)
        def create(self, name, image_name, memory=64, env=[], containerbridge_app=None):
            '''
            Create a new container. Same as `docker run / cf ic run`.
            '''
            body = {
                "Memory": memory,
                "Env": env,
                "Image": image_name
            }
            if containerbridge_app:
                body['BluemixApp'] = containerbridge_app

            return requests.post(self.api_ep + '/create?name={}'.format(name), headers=self.headers, json=body)
