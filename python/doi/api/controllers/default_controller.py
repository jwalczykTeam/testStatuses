from doi import constants
from doi.api import project_api
from doi.util import astro_util
from doi.util import rate_util
from doi.util import cf_util
from doi.util import log_util
from doi.util import ci_server_events
from flask import request, abort
import functools
import json
import os
import threading
import urllib3
import jwt
import time
from doi.helpers.python.lib import statuses

# Uncomment to support secret checking
# import hmac
# from sys import hexversion
# from os.path import abspath, normpath, dirname, join
# from hashlib import sha1

__event_impl = None
__api_impl = None
__event_impl_lock = threading.Lock()
__api_impl_lock = threading.Lock()
__api_config_var = os.environ.get(constants.ASTRO_API_CONFIG_ENV_VAR)
__api_config = json.loads(__api_config_var) if __api_config_var else {}
__api_key = os.environ.get(constants.ASTRO_API_KEY_ENV_VAR)
__bluemix_api_endpoint = os.environ.get(constants.ASTRO_BLUEMIX_API_ENDPOINT_ENV_VAR)
__syslog_config_var = os.environ.get(constants.ASTRO_SYSLOG_CONFIG_ENV_VAR)
__auth_fail_rate_limit = rate_util.RateLimit(
    __api_config.get('max_auth_fails'),
    __api_config.get('blocked_auth_interval'))

logger = log_util.get_logger("astro.api.controllers.default_controller")

try:
    if __syslog_config_var:
        logger.enable_syslog_notification(json.loads(__syslog_config_var))
except Exception as e:
    logger.warning(e.message)
    logger.warning("enable_syslog_notification failed, check certs.")


def get_event_impl():
    """
    Opens a new api impl if there is none
    yet for the current application context.
    """
    global __event_impl
    with __event_impl_lock:
        if __event_impl is None:
            __event_impl = ci_server_events.HandleWebHookEvents()

        return __event_impl


def get_api_impl():
    """
    Opens a new api impl if there is none
    yet for the current application context.
    """
    global __api_impl
    with __api_impl_lock:
        if __api_impl is None:
            group_id = os.environ.get(constants.ASTRO_GROUP_ID_ENV_VAR)
            db_config = cf_util.get_db_credentials()
            mb_config = cf_util.get_mb_credentials()

            if not group_id:
                group_id = constants.DEFAULT_GROUP_ID

            __api_impl = project_api.ProjectApi({
                "group_id": group_id,
                "api": __api_config,
                "db": db_config,
                "mb": mb_config
            })

        return __api_impl


'''
Authentication decorators
'''


def _get_audit_info(request, user_name):
    env = request.environ
    method = env['REQUEST_METHOD']
    url = request.url
    source_addr, source_port = _get_source(request)
    host_n_port = env['HTTP_HOST'].split(':')
    dest_addr = host_n_port[0]
    dest_port = host_n_port[1] if len(host_n_port) == 2 else "80"
    return [method,
            url,
            user_name,
            source_addr,
            source_port,
            dest_addr,
            dest_port]


def _get_source(request):
    env = request.environ
    headers = request.headers

    if headers.getlist("X-Forwarded-For"):
        forwarded_for = request.headers.getlist("X-Forwarded-For")[0]
        source_addrs = forwarded_for.split(',')
        source_addr = source_addrs[0].strip()
    else:
        source_addr = env['REMOTE_ADDR']

    if headers.getlist("X-Forwarded-Port"):
        forwarded_port = request.headers.get("X-Forwarded-Port")
        source_port = forwarded_port.strip()
    else:
        source_port = env['REMOTE_PORT']

    return (source_addr, source_port)


def _accept_proto(request):
    if not __api_config.get('enforce_https_requests', True):
        return True

    x_forwarded_proto = request.headers.get("X-Forwarded-Proto")
    if x_forwarded_proto:
        proto = x_forwarded_proto.strip()
        if proto == 'https':
            return True

    return False


def _log_web_service_auth_succeeded(request, user_name):
    try:
        audit_info = _get_audit_info(request, user_name)
        logger.log_web_service_auth_succeeded(*audit_info)
    except:
        logger.exception("Unexpected exception while sending syslog event")


def _log_web_service_auth_failed(request, user_name):
    try:
        audit_info = _get_audit_info(request, user_name)
        logger.log_web_service_auth_failed(*audit_info)
    except:
        logger.exception("Unexpected exception while sending syslog event")


def user_auth_required(api):
    @functools.wraps(api)
    def authorize_api(*args, **kwargs):
        def parse_bearer_token(auth_token):
            '''
            Open the JWT token and return the payload
            Return None if auth_token is not a valid JWT token
            '''
            payload = None
            maybe = [] if not auth_token else auth_token.split()
            if len(maybe) == 2:
                try:
                    payload = jwt.decode(maybe[1], verify=False)
                except jwt.DecodeError:
                    logger.exception('Invalid bearer token given')

            return payload

        def request_info(user_name='admin', org_id=None):
            return {
                'request_url': request.url,
                'tenant_id': org_id,
                'user_name': user_name,
                'is_admin': True if org_id is None else False
            }

        def bluemix_auth(auth, org_id):
            auth_info = parse_bearer_token(auth)
            if auth_info is not None:
                http = urllib3.PoolManager()
                url = "{}/v2/organizations/{}".format(__bluemix_api_endpoint,
                                                      org_id)

                results = http.request('GET',
                                       url,
                                       headers={'Authorization': auth})
                if results.status == 200:
                    user_name = auth_info.get('user_name', 'Unknown')
                    _log_web_service_auth_succeeded(request, user_name)
                    return api(request_info(user_name, org_id),
                               *args,
                               **kwargs)

            _log_web_service_auth_failed(request,
                                         auth_info.get('user_name', 'Unknown')
                                         if auth_info is not None else 'Unknown')
            abort(401)

        if not _accept_proto(request):
            abort(403)

        auth = request.headers.get(constants.ASTRO_AUTH_TOKEN)
        org_id = request.headers.get(constants.ASTRO_AUTH_TENANT_ID)
        user_name = request.headers.get(constants.ASTRO_AUTH_USERNAME)
        if auth and org_id:
            '''
            Authenticating with bluemix => ensure the token is valid and
            has access to org_id
            '''
            return bluemix_auth(auth, org_id)

        source_addr, _ = _get_source(request)
        if not __auth_fail_rate_limit.can_proceed(source_addr):
            _log_web_service_auth_failed(request, 'admin')
            abort(403)

        if user_name is None:
            '''
            Authenticating with Api Key but leveraging the user_name from otc broker
            '''
            user_name = 'admin'

        if auth == __api_key and not org_id:
            '''
            Authenticating with Api Key => full access to Astro API
            '''
            _log_web_service_auth_succeeded(request, user_name)
            return api(request_info(user_name), *args, **kwargs)

        __auth_fail_rate_limit.increment(source_addr)
        _log_web_service_auth_failed(request, user_name)
        abort(401)

    return authorize_api


def admin_auth_required(api):
    @functools.wraps(api)
    def authorize_api(*args, **kwargs):
        if not _accept_proto(request):
            abort(403)

        source_addr, _ = _get_source(request)
        if not __auth_fail_rate_limit.can_proceed(source_addr):
            _log_web_service_auth_failed(request, 'admin')
            abort(403)

        auth = request.headers.get(constants.ASTRO_AUTH_TOKEN)
        if auth == __api_key:
            '''
            Authenticating with Api Key => full access to Astro API
            '''
            _log_web_service_auth_succeeded(request, 'admin')
            return api(*args, **kwargs)

        __auth_fail_rate_limit.increment(source_addr)
        _log_web_service_auth_failed(request, 'admin')
        abort(401)

    return authorize_api

#
#   REST API functions
#


def scan():
    path = os.path.join("resources", "IBMDomainVerification.html")
    if not os.path.exists(path):
        return ("", 404)

    with open(path, 'r') as f:
        data = f.read()
        return (data, 200)


def ci_server(body, toolchainId):

    # enforce https
    if not _accept_proto(request):
            abort(403)

    logger.info(">>>  CI Server Start >>>")
    logger.info("processing toolchainId: '%s'", toolchainId)

    # Only POST is supported
    if request.method != 'POST':
            msg = "Only Support POST."
            rc = {"error": msg}
            logger.warning(msg)
            return (rc, 501)

    headers = request.headers

    if 'X-GitHub-Event' in headers:
        return _ci_server_github(body, toolchainId)
    elif 'X-GitLab-Event' in headers:
        return _ci_server_gitlab(body, toolchainId)
    else:
        msg = "X-Github-Event or X-Gitlab-Event not in header. Abort."
        rc = {"response": msg}
        logger.warning(msg)
        return (rc, 403)


def _ci_server_github(body, toolchainId):

    event = request.headers.get('X-GitHub-Event')

    event_impl = get_event_impl()

    return event_impl.github_events(body,
                                    toolchainId,
                                    event,
                                    body.get('action'))


def _ci_server_gitlab(body, toolchainId):

    event = request.headers.get('X-GitLab-Event')

    event_impl = get_event_impl()

    return event_impl.gitlab_events(body,
                                    toolchainId,
                                    event,
                                    body['object_attributes'].get('action','Testing'))


def healthcheck():
    # WARNING: This api is unprotected by a token.
    # This is a requirement of the Estado monitoring
    # Do not include sensitive data

    start_time = time.time()
    # Need a method to ping ES
    # es_health = get_elastic_health()
    es_health = True
    responseTime = time.time() - start_time

    dt_status = 0
    # This method returns a json payload that servers as a starting point for consumption monitoring.
    # What is included today serves as a framework for overall astro health and what it depends on (ES, workers, scheduler)
    # The details of the asserting health of the dependencies will be added over time
    es_status = "OK"
    if (es_health != True):
        dt_status = 1
        es_status = "NOT_OK"

    data = {
            "service": "DeveloperTeamInsights",
            "version": 1,
            "health": [
                {"plan": "Free",
                 "status": dt_status,
                 "serviceInput": "not used",
                 "serviceOutput": "response time to query the dependencies",
                 "responseTime": responseTime,
                 "responsetimeunit": "ms",
                 "dependencies": [
                    {"service": "astro-rest_api",
                     "status": "OK",
                     "statusCode": 200,
                     "responseTime": responseTime,
                     'astro_group_id': os.environ.get(constants.ASTRO_GROUP_ID_ENV_VAR),
                     'astro_last_commit_sha': os.environ.get('ASTRO_LAST_COMMIT_SHA'),
                     "responseTimeUnit": "ms",
                     "serviceOutput": "response time to query astro rest api",
                     "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                     },
                    {"service": "elasticSearch",
                     "status": es_status,
                     "statusCode": 200,
                     "responseTime": responseTime,
                     "responseTimeUnit": "ms",
                     "serviceOutput": "elastic search database - response time to get db health info.",
                     "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                     }]
                 }
            ]
            }

    return (data, 200)


@user_auth_required
def list_projects(request_info, id=None, name=None, state=None,
                  completed=None):

    api_impl = get_api_impl()

    return api_impl.list_projects(request_info, id, name, state, completed)


@user_auth_required
def list_metrics(request_info, id=None, name=None, state=None,
                 completed=None):
    api_impl = get_api_impl()
    return api_impl.list_metrics(request_info, id, name, state, completed)


@user_auth_required
def get_metric(request_info, project_name):
    api_impl = get_api_impl()
    return api_impl.get_metric(request_info, project_name)


@user_auth_required
def get_project(request_info, project_name):
    api_impl = get_api_impl()
    return api_impl.get_project(request_info, project_name)


@user_auth_required
def add_project(request_info, body):
    api_impl = get_api_impl()
    return api_impl.add_project(request_info, body)


@user_auth_required
def delete_project(request_info, project_name):
    api_impl = get_api_impl()
    return api_impl.delete_project(request_info, project_name)


@user_auth_required
def update_project(request_info, project_name, body):
    api_impl = get_api_impl()
    return api_impl.update_project(request_info, project_name, body)


@user_auth_required
def patch_project(request_info, project_name, body):
    api_impl = get_api_impl()
    return api_impl.patch_project(request_info, project_name, body)


@admin_auth_required
def get_config():
    api_impl = get_api_impl()
    return api_impl.get_config()


@admin_auth_required
def get_history(where=None, from_time=None, to_time=None, start=0, size=100):
    api_impl = get_api_impl()
    return api_impl.get_history(where, from_time, to_time, start, size)


@admin_auth_required
def delete_history(where=None, older_than_days=7):
    api_impl = get_api_impl()
    return api_impl.delete_history(where, older_than_days * 24 * 3600 * 1000)


@admin_auth_required
def get_scheduler():
    api_impl = get_api_impl()
    return api_impl.get_scheduler(request.url)


@admin_auth_required
def list_workers(state=None):
    api_impl = get_api_impl()
    return api_impl.list_workers(request.url, state)


@admin_auth_required
def get_worker(worker_id):
    api_impl = get_api_impl()
    return api_impl.get_worker(request.url, worker_id)


@admin_auth_required
def patch_worker(worker_id, body):
    api_impl = get_api_impl()
    return api_impl.patch_worker(request.url, worker_id, body)


@user_auth_required
def query_project_resources(request_info, project_name, resource_type,
                            where=None, from_time=None, to_time=None,
                            sort_by=None, sort_order=None,
                            start=None, size=None, fields=None,
                            min_probability=None):
    api_impl = get_api_impl()
    return api_impl.query_project_resources(
        request_info,
        project_name, resource_type,
        where, from_time, to_time,
        sort_by, sort_order,
        start, size, fields, min_probability)


@user_auth_required
def get_project_resource_distribution(request_info, project_name,
                                      resource_type, group_by_field,
                                      sum_by_field=None, from_time=None,
                                      to_time=None, size=None,
                                      min_probability=None):
    api_impl = get_api_impl()

    return api_impl.get_project_resource_distribution(
        request_info, project_name, resource_type,
        group_by_field, sum_by_field,
        from_time, to_time, size, min_probability)


@user_auth_required
def get_project_resource_distribution_ex(request_info, project_name,
                                         aggregations, resource_type=None,
                                         from_time=None, to_time=None,
                                         query=None, min_probability=None):
    api_impl = get_api_impl()
    return api_impl.get_project_resource_distribution_ex(
        request_info, project_name, aggregations, resource_type,
        from_time, to_time, query, min_probability)


@user_auth_required
def get_project_resource_distribution_ex_freeform(request_info, project_name,
                                                  query, resource_type=None):
    api_impl = get_api_impl()
    return api_impl.get_project_resource_distribution_ex_freeform(
        request_info, project_name, query, resource_type)


@user_auth_required
def get_project_resource_time_series(request_info, project_name, resource_type,
                                     group_by_field, sum_by_field=None,
                                     interval=None, from_time=None,
                                     to_time=None, time_format=None,
                                     size=None):
    api_impl = get_api_impl()
    return api_impl.get_project_resource_time_series(
        request_info, project_name, resource_type,
        group_by_field, sum_by_field,
        interval, from_time, to_time, time_format, size)


@user_auth_required
def get_project_resource_time_series_ex(request_info, project_name,
                                        aggregations, resource_type=None,
                                        interval=None, from_time=None,
                                        to_time=None, time_format=None,
                                        query=None):
    api_impl = get_api_impl()
    return api_impl.get_project_resource_time_series_ex(
        request_info, project_name, aggregations, resource_type,
        interval, from_time, to_time, time_format, query)


@user_auth_required
def get_project_resource_time_series_ex_freeform(request_info, project_name,
                                                 query):
    api_impl = get_api_impl()
    return api_impl.get_project_resource_time_series_ex_freeform(
        request_info, project_name, query)


@user_auth_required
def get_project_freeform(request_info, project_name,
                         query, resource_type=None):
    api_impl = get_api_impl()
    return api_impl.get_project_freeform(
        request_info, project_name, query, resource_type)


@user_auth_required
def get_project_best_practice_distribution(request_info, project_name, name,
                                           from_time=None, to_time=None):
    api_impl = get_api_impl()
    return api_impl.get_project_best_practice_distribution(
        request_info, project_name, name,
        from_time, to_time)


@user_auth_required
def get_project_best_practice_time_series(request_info, project_name, name,
                                          interval=None, from_time=None,
                                          to_time=None, time_format=None):
    api_impl = get_api_impl()
    return api_impl.get_project_best_practice_time_series(
        request_info, project_name, name,
        interval, from_time, to_time, time_format)
