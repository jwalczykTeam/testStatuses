
import os
import json
from doi import constants
from doi.util import log_util, cf_util, astro_util
from doi.util.db import db_util
from doi.helpers.python.lib import statuses

logger = log_util.get_logger("astro.util.ci_server_events")


class HandleWebHookEvents():

    def __init__(self):

        # Get Creds & Initialize database
        self.group_id = os.environ.get(constants.ASTRO_GROUP_ID_ENV_VAR)
        self.index_name = constants.get_scheduler_db_name(self.group_id)
        self.db_config = cf_util.get_db_credentials()
        self.db = db_util.Database.factory(self.db_config)
        self.es = self.db.get_client()
        self.space = self.group_id.split("-")[-1]
        self.ac = astro_util.AstroClient()

        #  index_name =  astro-scheduler + '@' + group_id
        logger.info("__init__(): HandleWebHookEvents Initialized "
                    "with ES Index: '%s' space: '%s' ",
                    self.index_name, self.space)

    def _get_sources(self, toolchainId):
        ac = astro_util.AstroClient()
        project = ac.get_project(toolchainId)
        sources = project.get('input', {'sources': None}).get('sources')

        if sources is not None:
            logger.info("Astro found toolchainId: '%s'", toolchainId)
            return project, sources, None
        else:
            logger.info("Astro could not find toolchainId: '%s'", toolchainId)
            msg = "Invalid toolchainId: '%s' ", toolchainId
            logger.warning(msg)
            rc = {"response": msg}
            return None, (rc, 400)

    def _get_doi_callback_base(self):
        group_name = os.environ.get(constants.ASTRO_GROUP_ID_ENV_VAR)
        doi_callback_base = None

        if 'rudicitest' in group_name:
            doi_callback_base = statuses.doi_callback_dev
        elif 'dev' in group_name:
            doi_callback_base = statuses.doi_callback_dev
        elif 'staging' in group_name:
            doi_callback_base = statuses.doi_callback_staging
        elif 'kim' in group_name:
            doi_callback_base = statuses.doi_callback_kim
        elif 'prod' in group_name:
            doi_callback_base = statuses.doi_callback_prod

        if doi_callback_base is not None:
            return doi_callback_base, None
        else:
            # fail
            msg = "group_id not supported '%s'", group_name
            logger.error(msg)
            rc = {"response": msg}
            return None, (rc, 400)

    def _iterate_sources_and_post(self,
                                  sources,
                                  project,
                                  repo,
                                  owner,
                                  toolchainId,
                                  pull_id,
                                  doi_callback_base,
                                  statuses_url, desc, state):
        '''
        Itterate through all sources in the given toolchainID.  We need the token(s)
        When we find a match, we send github (vai statuses_url) the state & desc 
        '''
        found_repo = False
        for src in sources:
            if (src['repo_name'] == repo and
                src['repo_owner'] == owner and
                    src['source_type'] in ["github_repo", "gitlab_repo"]):

                if not src.get('pr_flag', True):
                    mesg = "Pull request statuses are disabled for '{}/{}' in toolchainId '{}'. Check settings in DevOps Insights.".format(owner, repo, toolchainId)
                    logger.info(mesg)
                    continue

                git_client = statuses.GitClient(source=src, wait=False)

                bm_target_url = statuses.make_url(doi_callback_base,
                                                  orgName="default",
                                                  toolchainId=toolchainId,
                                                  tab="pr_commit_defect_prediction",
                                                  pullId=pull_id)

                logger.info("Bluemix target URL: '%s'", bm_target_url)

                created_by = project['created_by']
                project_description = project['description']
                contxt = "IBMDevOpsInsights (" + \
                    created_by + " - " + project_description + ")"

                description = {}
                description['state'] = state
                description['target_url'] = bm_target_url
                description['description'] = desc
                description['context'] = contxt

                git_client.post_to_git_api(
                    git_url=statuses_url, desc=description)
                found_repo = True

        if not found_repo:
            msg = "Repo not mined by IBM DevOps Insights - Abort."
            logger.error(msg)
            rc = {"error": msg}
            return None, (rc, 200)

        return (json.dumps(description), 200), None

    def _github_pull_request_review_action(self, body, toolchainId, action):
        '''
        Triggered when a pull request review is submitted into a non-pending state, 
        the body is edited, or the review is dismissed.
        Possible actions can be:  "submitted", "edited", or "dismissed".
        https://developer.github.com/v3/activity/events/types/#pullrequestreviewevent
        '''
        if (action not in ['submitted', 'dismissed']):  # edited not supported
            msg = "Action '{}' not supported or required no work. Only 'submitted' and 'dismissed' actions work.".format(action)
            logger.warning(msg)
            rc = {"response": msg}
            return (rc, 400)
        else:
            repo = body['repository']['name']
            owner = body['repository']['owner']['login']
            pull_id = body['pull_request']['number']
            statuses_url = body['pull_request']['statuses_url']
            reviewer_state = body['review']['state']
            author_association = body['review']['author_association']

            msg = "Processing GitHub Pull Request Review Event; with Action '{}' for '{}/{}' in toolchainId '{}' ".format(action, owner, repo, toolchainId)
            logger.info(msg)

            if action == 'dismissed':
                # If a blocking review with changes_requested gets dismissed, we
                # consider this an approval
                '''
                TO-DO: perhaps we do not set the statuses to green if there are other "requested_reviewers" still pending?
                '''
                desc = "Dismissed '{}' stale review".format(body['review']['user']['login'])
                state = "success"

            if action == 'submitted':

                if reviewer_state == 'approved':

                    if (author_association == 'OWNER' or author_association == 'COLLABORATOR'):
                        desc = "Request Approved by '{}' ".format(body['review']['user']['login'])
                        state = "success"
                    else:
                        msg = "Review State was '{}' but author_association was '{}' needs to be OWNER or COLLABORATOR to change status to green check.".format(reviewer_state, author_association)
                        logger.info(msg)
                        rc = {"response": msg}
                        return (rc, 400)

                elif reviewer_state == 'changes_requested':
                    msg = "Review State '{}' required no actions.".format(reviewer_state)
                    logger.info(msg)
                    rc = {"response": msg}
                    return (rc, 400)
                elif reviewer_state == 'comment':
                    msg = "Review State '{}' required no actions.".format(reviewer_state)
                    logger.info(msg)
                    rc = {"response": msg}
                    return (rc, 400)
                else:
                    msg = "Review State '{}' not supported.".format(reviewer_state)
                    logger.warning(msg)
                    rc = {"response": msg}
                    return (rc, 400)

            project, sources, rc = self._get_sources(toolchainId)
            if sources is None:
                return rc

            doi_callback_base, rc = self._get_doi_callback_base()
            if doi_callback_base is None:
                return rc

            description, rc = self._iterate_sources_and_post(sources,
                                                             project,
                                                             repo,
                                                             owner,
                                                             toolchainId,
                                                             pull_id,
                                                             doi_callback_base,
                                                             statuses_url,
                                                             desc,
                                                             state)
            if description is None:
                return rc
            else:
                return description

    def _github_pull_request_action(self, body, toolchainId, action):

        if (action not in ['opened', 'open', 'synchronize', 'update']):
        '''
        Note "action": "review_requested" has no action.  In the future we might want send a green check or do something when the
            user just adds a code reviewer.  This would be done here...
        '''
            msg = "Action '{}' not supported or required no work. Only 'opened', 'open', 'synchronize' and 'update' actions work.".format(action)
            logger.warning(msg)
            rc = {"response": msg}
            return (rc, 400)
        else:
            repo = body['repository']['name']
            owner = body['repository']['owner']['login']
            pull_id = body['number']
            statuses_url = body['pull_request']['statuses_url']

            msg = "Processing GitHub Pull Request Event with Action '{}' for '{}/{}' in toolchainId '{}' ".format(action, owner, repo, toolchainId)
            logger.info(msg)

            desc = "Analyzing Pull Request..."
            state = "pending"

            project, sources, rc = self._get_sources(toolchainId)
            if sources is None:
                return rc

            doi_callback_base, rc = self._get_doi_callback_base()
            if doi_callback_base is None:
                return rc

            description, rc = self._iterate_sources_and_post(sources,
                                                             project,
                                                             repo,
                                                             owner,
                                                             toolchainId,
                                                             pull_id,
                                                             doi_callback_base,
                                                             statuses_url,
                                                             desc,
                                                             state)
            if description is None:
                return rc
            else:
                return description

    def _gitlab_merge_request_hook_action(self, body, toolchainId, action):
        '''
        Unlike the three github pull request event types
        (pull_request_review_comment, pull_request_review and pull_request),
        Gitlab only has one event type for a PR called 'Merge Hook Request'
        - When the Action is 'approved', we send the green check to the statuses
            URL to that PR
        - When the Action is 'open' or 'opened', we send the pending message to
            the statuses URL to that PR
        '''
        if (action not in ['approved', 'opened', 'open']):
            msg = "Action '{}' not supported or required no work. Only 'approved', opened' or 'open' work.".format(action)
            logger.warning(msg)
            rc = {"response": msg}
            return (rc, 400)
        else:
            url_split = body['project']['web_url'].split('/')
            repo = url_split[4].replace('.git', '')
            owner = url_split[3]
            pull_id = body['object_attributes']['iid']

            msg = "Processing GitLab Merge Request Hook ID '{}' Event with Action '{}' for '{}/{}' in toolchainId '{}' ".format(pull_id, action, owner, repo, toolchainId)
            logger.info(msg)

            statuses_url = '/'.join([
                url_split[0],
                url_split[1],
                url_split[2],
                'api/v4/projects',
                owner + '%2F' + repo,
                'statuses',
                body['object_attributes']['last_commit']['id']])

            project, sources, rc = self._get_sources(toolchainId)
            if sources is None:
                return rc

            doi_callback_base, rc = self._get_doi_callback_base()
            if doi_callback_base is None:
                return rc

            if action == 'approved':
                desc = "Merge Request Approved!"
                state = "success"
            else:  # 'opened', 'open'
                desc = "Analyzing Request..."
                state = "pending"

            description, rc = self._iterate_sources_and_post(sources,
                                                             project,
                                                             repo,
                                                             owner,
                                                             toolchainId,
                                                             pull_id,
                                                             doi_callback_base,
                                                             statuses_url,
                                                             desc,
                                                             state)
            if description is None:
                return rc
            else:
                return description

    def github_events(self, body, toolchainId, event, action):

        if event == 'ping':
            msg = 'pong'
            rc = {'response': msg}
            return (rc, 200)

        if event == "pull_request":
            '''
            Actions can be: "assigned", "unassigned", "review_requested",
                            "review_request_removed", "labeled",
                            "unlabeled", "opened", "edited",
                            "closed", or "reopened".
                https://developer.github.com/v3/activity/events/types/#pullrequestevent
            '''
            return self._github_pull_request_action(body, toolchainId, action)

        if event == "pull_request_review":
            '''
            Actions can be: "submitted", "edited", or "dismissed".
            https://developer.github.com/v3/activity/events/types/#pullrequestreviewevent
            '''
            return self._github_pull_request_review_action(body, toolchainId, action)

        msg = "Github event type \"{}\" not supported.".format(event)
        logger.warning(msg)
        rc = {"response": msg}
        return (rc, 400)

    def gitlab_events(self, body, toolchainId, event, action):

        if event == 'ping':
            msg = 'pong'
            rc = {'response': msg}
            return (rc, 200)

        if event == "Merge Request Hook":
            # For ping requests, gitlab simply doesn't append the action
            return self._gitlab_merge_request_hook_action(
                body, toolchainId, action)

        msg = "Gitlab event type \"{}\" not supported.".format(event)
        logger.warning(msg)
        rc = {"response": msg}
        return (rc, 400)

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
