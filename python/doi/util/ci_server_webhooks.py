
import os
import json
import traceback
from urlparse import urlparse

from doi import constants
from doi.util import log_util, cf_util, astro_util
from doi.util.db import db_util
from doi.helpers.python.lib import statuses


logger = log_util.get_logger("astro.util.ci_server_webhooks")

github_webhook_payload_template = '{ \
   "name": "web",\
   "active": true,\
   "events": [\
     "pull_request",\
     "pull_request_review"\
   ],\
   "config": {\
       "insecure_ssl": "0",\
       "content_type": "json"\
   }\
 }'

gitlab_webhook_payload_template = '{ \
    "merge_requests_events": true,\
    "enable_ssl_verification": false,\
    "push_events": false\
}'


class HandleWebhook():

    def __init__(self, pid):

        self.pid = pid

        # Get Creds & Initialize database
        self.group_id = os.environ.get(constants.ASTRO_GROUP_ID_ENV_VAR)
        self.index_name = constants.get_scheduler_db_name(self.group_id)
        self.db_config = cf_util.get_db_credentials()
        self.db = db_util.Database.factory(self.db_config)
        self.es = self.db.get_client()
        self.space = self.group_id.split("-")[-1]
        self.ac = astro_util.AstroClient()

        #  index_name =  astro-scheduler + '@' + group_id
        logger.info("__init__(): HandleWebhook Initialized "
                    "with ES Index: '%s' space: '%s' PID = '%s' ",
                    self.index_name, self.space, self.pid)

    def _make_webhook_url(self, astro_base, toolchainId, source_type):

        webhook_url = statuses.make_url(astro_base,
                                        toolchainId=toolchainId)

        if source_type == 'github_repo':
            webhook_payload = json.loads(github_webhook_payload_template)
            webhook_payload['config']['url'] = webhook_url
        elif source_type == 'gitlab_repo':
            webhook_payload = json.loads(gitlab_webhook_payload_template)
            webhook_payload['url'] = webhook_url

        return webhook_payload

    def _blacklisted_owners(self, owner):
        '''
        Handle blacklist for owner names
        TO-DO we need a blacklist from a config.json file
              for various monitors, etc
        '''
        if ('idsqat' in owner):
            return True
        else:
            return False

    def _blacklisted_toolchains(self, toolchainId):
        '''
        Handle blacklist for toolchainid names
        TO-DO we need a blacklist from a config.json file
              for various monitors, etc
        '''
        if ('astro-' in toolchainId) \
                or ('don_' in toolchainId) \
                or ('__test__' in toolchainId):
            return True
        else:
            return False

    def _process_webhook_deletes(self, toolchainId):
        '''
        This function gets the toolchain data from ES, and loops
        over all sources in that toolchain.  For each source
        with a webhook_id, it deletes this hook from git client.

        TO-DO:  when this method is called during an update,
        we (optionally) pass in the original sources and
        check for sources that already have a webhook. If a
        source has a webhook, we don't need to delete it first,
        then later re-add one. This is bad because we lose the
        webhook history stored in git and also burns git client
        API calls.
        '''

        # First handle blacklist for toolchainid names
        if self._blacklisted_toolchains(toolchainId):
            logger.warn(
                "<<< toolchainId name '%s' is blacklisted.", toolchainId)
            return None

        project = self.ac.get_project(toolchainId)

        if project is None:
            logger.warn("Project not in database: {}".format(toolchainId))
            return None

        for source in project['input']['sources']:

            if 'git_url' not in source:
                logger.info(
                    "_process_webhook_deletes('%s'): "
                    "skipping sources record without git_url", self.pid)
                continue
            if 'webhook_id' in source:
                webhook_id = source['webhook_id']
                logger.info(
                    "_process_webhook_deletes('%s'): webhook_id '%s' "
                    "WAS FOUND in source for '%s'", self.pid,
                    webhook_id, source['git_url'])
                try:
                    # DELETE /repos/:owner/:repo/hooks/:id
                    response, url = self._delete_to_git_api(
                        source, webhook_id)
                except Exception as e:
                    logger.warn(e.message)
                    logger.warn(
                        "<<<<<  _process_webhook_deletes('%s'): "
                        "Failed to delete webhook on Git client.", self.pid)
                    # TO-DO maybe check for any hooks that look
                    #     like ours and delete them here.
            else:
                logger.info(
                    "_process_webhook_deletes('%s'): webhook_id "
                    "NOT FOUND in source for '%s'", self.pid,
                    source['git_url'])

    def _process_webhook_inserts(self, repo, owner, toolchainId, hook_id):

        updated_a_job = False
        updated_a_src = False

        project = self.ac.get_project(toolchainId)

        if project is None:
            logger.warn("Project not in database: {}".format(toolchainId))
            return False

        id_builder = project['id']

        for source in project['input']['sources']:

            if 'git_url' not in source:
                logger.info(
                    "_process_webhook_inserts('%s'): skipping "
                    "sources record without git_url",
                    self.pid)
                continue

            if (source['repo_name'] == repo and
                    source['repo_owner'] == owner and
                    source['source_type'] in ["github_repo", "gitlab_repo"]):
                try:
                    logger.info("_process_webhook_inserts('%s'):"
                                "Inserting webhook_id: '%s' "
                                "associated with index: '%s' "
                                "owner: '%s' "
                                "repo: '%s' "
                                "id: '%s' "
                                "doc_type: '%s' "
                                % (self.pid,
                                   hook_id,
                                   self.index_name,
                                   owner,
                                   repo,
                                   id_builder,
                                   "job"))
                    # Note - if webhook_id already exists, it
                    # is simply overwritten.
                    source[u'webhook_id'] = str(hook_id)
                    updated_a_src = True
                except Exception as e:
                    logger.error(e.message)

        if updated_a_src:
            logger.info("_process_webhook_inserts('%s'): "
                        "WRITING TO ES...", self.pid)

            result = self.es.update(index=self.index_name,
                                    id=id_builder,
                                    doc_type="job",
                                    body={"doc":
                                          {"input": project['input']}},
                                    refresh="true")
            logger.info(
                ">>> _process_webhook_inserts('%s'): webhook "
                "updated. ES response: '%s'", self.pid, result)
            updated_a_job = True
        else:
            logger.info(
                "<<< _process_webhook_inserts('%s'): No ES "
                "updates required for inserts.", self.pid)

        return updated_a_job

    def _delete_to_git_api(self, src, hook_id):

        api_base_url = src['api_base_url']
        owner = src['repo_owner']
        repo = src['repo_name']

        git_client = statuses.GitClient(source=src, wait=True)

        if src['source_type'] == 'github_repo':
            git_url = statuses.make_url(api_base_url,
                                        'repos',
                                        owner,
                                        repo,
                                        'hooks',
                                        hook_id)
        elif src['source_type'] == 'gitlab_repo':
            git_url = statuses.make_url_gitlab(api_base_url + "/projects",
                                               owner,
                                               repo,
                                               'hooks',
                                               hook_id)

        logger.info(
            " >>>>> _delete_to_git_api('%s'): delete "
            "webhook at url: '%s' ", self.pid, git_url)

        # Delete web hook in git client
        response = git_client.delete_to_git_api(git_url=git_url)

        return response, git_url

    def _post_to_git_api(self, src, webhook_payload):

        api_base_url = src['api_base_url']
        owner = src['repo_owner']
        repo = src['repo_name']
        source_type = src['source_type']

        git_client = statuses.GitClient(source=src, wait=True)

        if source_type == 'github_repo':
            git_url = statuses.make_url(api_base_url,
                                        'repos',
                                        owner,
                                        repo,
                                        'hooks')
        elif source_type == 'gitlab_repo':
            git_url = statuses.make_url_gitlab(api_base_url + "/projects",
                                               owner,
                                               repo,
                                               'hooks')

        logger.info(" >>>>> _post_to_git_api('%s'): creating "
                    "webhook at url: '%s' "
                    "payload: '%s'", self.pid, git_url, webhook_payload)

        response = git_client.post_to_git_api(git_url=git_url,
                                              desc=webhook_payload)
        return response, git_url

    def _handle_422_case(self, src, toolchainId):
        '''
        This function handles the case when the webhook
        was already created at git client either by
        a previous add (during the update/add
        step and this is now the real
        add/add step) or the git client owner
        added it. This also happens
        when the broker decides to send multiple
        consecutive updates for unknown reasons.
        Here we get the existing hook id
        from git client and we'll then use
        this to store in the DB.
        '''
        logger.info(
            "<<<<<  _handle_422_case('%s'): "
            "422 case - webhook ALREADY EXISTS on Git Client.",
            self.pid)

        try:
            logger.info(
                "<<<<<  _handle_422_case('%s'): "
                "lookup the webhook id at git client...", self.pid)

            git_client = statuses.GitClient(source=src, wait=True)
            api_base_url = src['api_base_url']
            owner = src['repo_owner']
            repo = src['repo_name']
            source_type = src['source_type']

            if source_type == 'github_repo':
                git_url = statuses.make_url(api_base_url,
                                            'repos',
                                            owner,
                                            repo,
                                            'hooks')
            elif source_type == 'gitlab_repo':
                git_url = statuses.make_url_gitlab(api_base_url + "/projects",
                                                   owner,
                                                   repo,
                                                   'hooks')

            webhook_id = git_client.get_webhook_id(git_url=git_url,
                                                   toolchainId=toolchainId,
                                                   space=self.space)
            if webhook_id is None:
                #  Do not insert failed
                #  webhooks into ES
                logger.warn(
                    "<<<<<  _handle_422_case('%s'): "
                    "I couldn't find the webhook "
                    "id at git client.",
                    self.pid)
                return None
            else:
                logger.info(
                    "<<<<<  _handle_422_case('%s'): "
                    "webhook_id FOUND at git client: '%s' "
                    "use this to insert into DB.",
                    self.pid, webhook_id)

                # hook id gets set here
                src['webhook_id'] = webhook_id

        except Exception:
            logger.error(traceback.print_exc())
            logger.error(
                "<<<<<  _handle_422_case('%s'): "
                "I couldn't find the webhook id at the git client.",
                self.pid)
            #  Do not insert failed webhooks into ES
            return None

        return src

    def _check_webhook_exists(self, src, webhook_payload):
        '''
        Checks and validates existing webhook.

        Return Values:
            Hook Exists, Hook Valid, Hook ID
            False, False, None - webhook with the formatted url doesn't exist
            True, True, <webhook_id> - Webhook Exists and is good
            True, False, <webhook_id> - Webhook Exists and is bad
        '''
        api_base_url = src['api_base_url']
        owner = src['repo_owner']
        repo = src['repo_name']

        git_client = statuses.GitClient(source=src, wait=True)

        logger.debug("_check_webhook_exists() Processing git source_type: {}"
                     .format(src['source_type']))
        logger.debug(
            "_check_webhook_exists() Processing git_url: {}"
            .format(src['git_url']))

        if src['source_type'] == 'github_repo':
            git_url = statuses.make_url(api_base_url,
                                        'repos',
                                        owner,
                                        repo,
                                        'hooks')
        elif src['source_type'] == 'gitlab_repo':
            git_url = statuses.make_url_gitlab(api_base_url + "/projects",
                                               owner,
                                               repo,
                                               'hooks')
        all_hooks = git_client.get_all_hooks(git_url=git_url)

        for git_hook in all_hooks:
            if src['source_type'] == 'github_repo':
                webhook_url = webhook_payload.get('config',
                                                  {'url': None}).get('url')
                git_hook_url = git_hook.get('config', {'url': None}).get('url')
                if git_hook_url is None or webhook_url is None:
                    logger.debug("_check_webhook_exists() github; could not" +
                                 " detect devops insights. Skip this hook.")
                    continue
                if webhook_url == git_hook_url:
                    if ("pull_request" not in git_hook["events"] or
                            "pull_request_review" not in git_hook["events"]):
                        # Exists but Bad
                        return True, False, git_hook["id"]
                    else:
                        # Exists and Good
                        return True, True, git_hook["id"]
            elif src['source_type'] == 'gitlab_repo':
                webhook_url = webhook_payload.get('url')
                git_hook_url = git_hook.get('url')
                if git_hook_url is None or webhook_url is None:
                    logger.debug("_check_webhook_exists() gitlab; could not" +
                                 " detect devops insights. Skip this hook.")
                    continue
                if webhook_url == git_hook_url:
                    if not git_hook["merge_requests_events"]:
                        # Exists but Bad
                        return True, False, git_hook["id"]
                    else:
                        # Exists and Good
                        return True, True, git_hook["id"]
        # Does not Exists
        return False, False, None

    def _edit_webhook(self, src, hook_id, webhook_payload):
        '''
        Sends either a put or a patch to edit the webhook.
        '''
        api_base_url = src['api_base_url']
        owner = src['repo_owner']
        repo = src['repo_name']

        git_client = statuses.GitClient(source=src, wait=True)

        if src['source_type'] == 'github_repo':
            git_url = statuses.make_url(api_base_url,
                                        'repos',
                                        owner,
                                        repo,
                                        'hooks',
                                        hook_id)
            git_client.patch_to_git_api(git_url=git_url,
                                        desc=webhook_payload)

        elif src['source_type'] == 'gitlab_repo':
            git_url = statuses.make_url_gitlab(api_base_url + "/projects",
                                               owner,
                                               repo,
                                               'hooks',
                                               hook_id)
            git_client.put_to_git_api(git_url=git_url,
                                      desc=webhook_payload)

    def _create_webhooks(self, toolchainId, sources, astro_base):

        for src in sources:
            logger.debug(
                "_create_webhooks() PROCESSING SRC: {}".format(src['git_url']))
            if src['source_type'] not in ["github_repo", "gitlab_repo"]:
                logger.error(
                    "_create_webhooks('%s'): Only github and gitlab"
                    "webhooks are supported.",
                    self.pid)
                continue

            # Handle blacklist for owner names
            owner = src.get('repo_owner')
            if self._blacklisted_owners(owner):
                logger.warn(
                    "<<< _create_webhooks('{}'): ".format(self.pid) +
                    "Owner name '{}' is blacklisted.".format(owner))
                continue

            try:
                response = None
                # Create web hook in git client
                webhook_payload = self._make_webhook_url(
                    astro_base, toolchainId, src['source_type'])

                # Check for and validates existing webhook
                webhook_exists, webhook_valid, webhook_id = self._check_webhook_exists(
                    src, webhook_payload)

                if webhook_exists is False:
                    # If false, create webhook

                    logger.info(
                        "Calling _create_webhooks('{}'): ".format(self.pid) +
                        " in repo '{}' for toolchainID '{}'".format(
                            src["git_url"], toolchainId))
                    response, git_url = self._post_to_git_api(
                        src, webhook_payload)
                    webhook_id = json.loads(response.content)['id']
                else:
                    if webhook_valid is False:
                        # webhook exists, but needs fixed
                        try:
                            self._edit_webhook(src,
                                               webhook_id,
                                               webhook_payload)
                        except Exception as e:
                            logger.error(
                                "Failed to edit webhook: {}".format(e))

                src['webhook_id'] = webhook_id
                # Set Pull Request and Commit Assessment Defaults
                #  When these are False a Status check will not
                #  be sent for it
                src['pr_flag'] = True
                src['commit_flag'] = False

            except Exception as e:
                logger.error(traceback.print_exc())
                webhook_id = None
                if response is not None:
                    status_code = response.status_code
                    if (status_code == 422) or \
                       ('422 Client Error' in str(e.message)):

                        updated_src = self._handle_422_case(src,
                                                            toolchainId)
                        if updated_src is not None:
                            src = updated_src
                        else:
                            continue
                    else:
                        logger.warn(
                            "<<<<<  _create_webhooks('%s'): Failed"
                            " to create webhook on Git Client."
                            " Response code: '%s'",
                            self.pid, response.status_code)
                        #  Do not insert failed webhooks into ES
                        continue
                else:
                    logger.warn(
                        "<<<<<  _create_webhooks('%s'): Failed"
                        "to create webhook on Git Client."
                        "response was None.", self.pid)

                    if '422 Client Error' in str(e.message):

                        updated_src = self._handle_422_case(src,
                                                            toolchainId)
                        if updated_src is not None:
                            src = updated_src
                        else:
                            continue
                    else:
                        #  Do not insert failed webhooks into ES
                        continue

        return sources

    def add_webhooks(self, sources, request_url, toolchainId=None):

        try:
            updated_sources = None

            if toolchainId is None:
                # This means add_webhooks() was called
                #    by update_project
                parsed = urlparse(request_url)
                toolchainId = parsed.path.split("/")[3]

            # First handle blacklist for toolchainid names
            if self._blacklisted_toolchains(toolchainId):
                logger.warn(
                    "<<< toolchainId name '%s' is blacklisted.", toolchainId)
                return None

            parsed = urlparse(request_url)
            astro_base = "https://%s%s" % (parsed.netloc,
                                           "/v1/ci_server")

            updated_sources = self._create_webhooks(toolchainId,
                                                    sources,
                                                    astro_base)

        except Exception as e:
            logger.error(e.message)

        return updated_sources

    def delete_webhooks(self, project_name):
        return self._process_webhook_deletes(project_name)

    def close(self):
        # close DB
        logger.info("close('%s'): Closing job database: '%s'",
                    self.pid, self.index_name)
        self.db.close()
        logger.info("close('%s'): Job database closed.", self.pid)
