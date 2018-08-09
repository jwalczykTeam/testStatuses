import requests
import urllib
import json
import os
import traceback
from urlparse import urlparse, parse_qs
import github as helpers_github
import toolchain_settings as tc_settings

try:
    # Rudi
    # from rudi.miner.doi.jobs.miner import github, gitlab
    from rudi.helpers.python.lib import github, gitlab
    from rudi.miner.doi.util import log_util
except ImportError:
    # Astro
    from doi.helpers.python.lib import github, gitlab
    from doi.util import log_util
    # from doi.jobs.miner import github, gitlab

logger = log_util.get_logger("rudi.helpers.python.lib.statuses")

prod_cname = "dnt.us-south.devopsinsights.cloud.ibm.com"

doi_callback_staging = "https://console.stage1.bluemix.net/devops/insights/\
?env_id=ibm:ys1:us-south#!/developerinsights"
doi_callback_dev = "https://dev-console.stage1.bluemix.net/devops/insights/\
?env_id=ibm:ys1:us-south#!/developerinsights"
doi_callback_prod = "https://console.bluemix.net/devops/insights/\
?env_id=ibm:yp:us-south#!/developerinsights"
doi_callback_kim = "https://console.bluemix.net/devops/insights/\
/public/#!/overview"


def make_url(base_url, *res, **params):
    url = base_url
    for r in res:
        url = '{}/{}'.format(url, r)
    if params:
        url = '{}?{}'.format(url, urllib.urlencode(params))
    return url


def make_url_gitlab(base_url, owner, repo, *res, **params):
    url = '{}/{}%2F{}'.format(base_url, owner, repo)
    for r in res:
        url = '{}/{}'.format(url, r)
    if params:
        url = '{}?{}'.format(url, urllib.urlencode(params))
    return url


class GitClient:

    def __init__(self, **config_options):

        self.__dict__.update(**config_options)
        self.session = requests.Session()

        self.session.headers[
            'Authorization'] = 'token %s' % self.source['access_token']

        if self.source['source_type'] == 'github_repo':
            self.git_client = github.GitHubClient(
                source=self.source, wait=self.wait)
        elif self.source['source_type'] == 'gitlab_repo':
            self.git_client = gitlab.GitLabClient(
                source=self.source, wait=self.wait)

    def _update_kwargs(self, kwargs):
        if kwargs is not None:
            for key, value in kwargs.iteritems():
                logger.debug("'%s' : '%s'", key, value)

        self.__dict__.update(**kwargs)

    def get_all_hooks(self, **kwargs):

        self._update_kwargs(kwargs)

        try:
            response = self.git_client.get(uri=self.git_url)
        except Exception as e:
            if '404 Client Error' in e.message:
                logger.error("get_all_hooks(): 404 Client Error: Not Found for url or Unauthorized.")
                return None
            else:
                logger.error(traceback.print_exc())
                raise ValueError(
                    "get_all_hooks(): Error calling git. "
                    "Response content from {}".format(
                        self.git_url))

        logger.debug("get_all_hooks(): CHECK HOOK RESPONSE: '%s'", response)

        try:
            all_hooks = json.loads(response.content)
        except Exception:
            logger.error(traceback.print_exc())
            raise ValueError(
                "get_all_hooks(): Bad Response content from {} "
                "not json parseable: {}".format(
                    self.git_url, response.content))

        return all_hooks

    def get_webhook_id(self, **kwargs):
        '''
        kwargs:
            git_url: 'https://api.github../repo/jwalczyk/test_repo/hooks/17762289'
            toolchainId: 9bc73c79-82b6-4714-b580-aef763666347
            space: 'prod'
        This function calls git*/owner/repo/hooks to get all webhooks
        for the repo. The toolchainID defined within the hook URL is
        compared with the passed in toolchainID.
        '''

        all_hooks = self.get_all_hooks(**kwargs)

        if all_hooks is not None and len(all_hooks) > 0:
            # all_hooks is an array of 1 or more webhooks
            for hook in all_hooks:
                # Gitlab hooks are in a slighlty different format
                if 'url' in hook and 'config' not in hook:
                    hook['config'] = {'url': hook['url']}
                if ('config' not in hook) or ('url' not in hook['config']):
                    logger.debug("get_webhook_id(): No ['config']['url'] "
                                 " found for hook_id: '%s'; continue...",
                                 hook['id'])
                    continue
                # url = "https://.../v1/ci_server?
                    # toolchainId=4a1090f8-53d4-46f6-89a2-680a2b33b16f&orgId=..."
                try:
                    url = hook['config']['url']
                    webhook_id = hook['id']
                    toolchainId = parse_qs(urlparse(url).query).get('toolchainId')
                    if toolchainId is None:
                        logger.info("Webhook '%s' not for DevOps," +
                                    " continuing to other hooks.", url)
                        continue

                    logger.debug("get_webhook_id(): parsed "
                                 "out toolchainId: '%s'",
                                 toolchainId)
                    if self.toolchainId == toolchainId[0]:
                        # Check for correct space such as prod, dev, etc
                        parsed = urlparse(url)

                        if (self.space in parsed.netloc) or (prod_cname in parsed.netloc):
                            logger.info("get_webhook_id(): \
                                        webhook '%s' FOUND for toolchainId: '%s' \
                                        parsed.netloc = '%s'",
                                        webhook_id, self.toolchainId, parsed.netloc)
                            return webhook_id
                        else:
                            logger.debug("get_webhook_id(): Found hook "
                                         "but wrong space. Expecting "
                                         "'%s' git webhook "
                                         "URL had: '%s'. continue...",
                                         self.space, parsed.netloc)
                            # Get the next webhook, this is the wrong one
                            continue

                except Exception as e:
                    logger.info("get_webhook_id(): no toolchainId found in "
                                "URL or some other error prevented finding "
                                "one; error: '%s'", e.message)
                    logger.info(traceback.print_exc())
                    continue
        else:
            logger.info("get_webhook_id(): webhook NOT FOUND "
                        "for toolchainId: '%s'",
                        self.toolchainId)
            return None

    def post_to_git_api(self, **kwargs):

        logger.debug("post_to_git_api() calling git client with following args:")
        self._update_kwargs(kwargs)
        data = json.dumps(self.desc)
        response = self.git_client.post(uri=self.git_url, data=data)
        logger.debug("POST RESPONSE: '%s'", response)

        return response

    def patch_to_git_api(self, **kwargs):

        logger.debug("patch_to_git_api() calling git client with following args:")
        self._update_kwargs(kwargs)
        data = json.dumps(self.desc)
        response = self.git_client.patch(uri=self.git_url, data=data)
        logger.debug("PATCH RESPONSE: '%s'", response)

        return response

    def put_to_git_api(self, **kwargs):

        logger.debug("put_to_git_api() calling git client with following args:")
        self._update_kwargs(kwargs)
        data = json.dumps(self.desc)
        response = self.git_client.put(uri=self.git_url, data=data)
        logger.debug("PUT RESPONSE: '%s'", response)

        return response

    def delete_to_git_api(self, **kwargs):

        logger.debug("delete_to_git_api() calling git client with following args:")
        self._update_kwargs(kwargs)
        response = self.git_client.delete(uri=self.git_url)
        logger.debug("DELETE RESPONSE: '%s'", response)

        return response


class Statuses:

    def __init__(self,
                 project,
                 array_of_commits,
                 group_name=None,
                 test=False,
                 test_owner=None,
                 debug=False):

        #  Default to prod
        self.group_name = "prod"

        if group_name is not None:
            print "Statuses __init__;  group_name: ", group_name
            self.group_name = group_name
        else:
            print "Statuses __init__;  group_name = None,\
                   use default: ", self.group_name

        self.project = project
        self.array_of_commits = array_of_commits
        self.test = test
        self.test_owner = test_owner
        self.debug = debug
        self.sources_dict = {}

    def transform_sources(self):
        #
        # Loop through each of the sources in the project
        # array object,  ONLY process sources with webhook
        # If webhook, get owner and repo. Transform into
        # a dictionary with the key equal to `github.com/owner/repo`
        # and the value is the entire sources element for
        # this associated url
        #
        srcs = self.project['input']['sources']

        found = False
        self.sources_dict = {}

        for src in srcs:

            if "webhook_id" not in src:
                continue

            source_type = src['source_type']
            if source_type not in ["github_repo", "gitlab_repo"]:
                logger.debug("transform_sources(): source_type '%s' "
                             " is not github or gitlab", source_type)
                continue

            parsed = urlparse(src['git_url'])
            url = os.path.splitext(parsed.path)[0]
            idx = "%s%s" % (parsed.netloc, url)

            logger.debug("using idx = '%s' as the key into dictionary"
                         " for the sources entry with git_url['%s']"
                         % (idx, src['git_url']))

            # { u'github.com/owner/repo': {u'access_token': u'b28e245e0...},
            found = True
            self.sources_dict[idx] = src

        if not found:
            return None
        else:
            return self.sources_dict

    @staticmethod
    def get_git_url_api_repos(source):
        if source['source_type'] == 'github_repo':
            git_url_api_repos = "{}/repos".format(
                source['api_base_url'])
        elif source['source_type'] == 'gitlab_repo':
            git_url_api_repos = "{}/projects".format(
                source['api_base_url'])
        return git_url_api_repos

    def correlate_commits_with_sources(self, action):
        #
        #  Loop through each element of the commit
        #  list and for each commit, grab and parse the url
        #  into the same key format as the sources_dict
        #  created above: `github.com/owner/repo`; then
        #  retrieve it's associated sources element.
        #
        #  Return all data needed to call github's statuses API to mark
        #  the commit with appropriate status
        #  ref: https://developer.github.com/v3/repos/statuses/#create-a-status
        #
        rc_array = []

        if not self.sources_dict:
            logger.debug("correlate_commits_with_sources(): "
                         "sources_dict empty")
            return rc_array

        if not self.array_of_commits:
            logger.debug("correlate_commits_with_sources(): "
                         "array_of_commits empty")
            return rc_array

        toolchainId = self.project['name']

        for commit_record in self.array_of_commits:

            # https://github.com/atom/atom/commit/8a6ef7061126ed5d51949...
            parsed = urlparse(commit_record['url'])

            owner = parsed.path.split("/")[1]
            repo = parsed.path.split("/")[2]

            # github.ibm.com/owner/repo  or  github.com/owner/repo
            idx = "%s/%s/%s" % (parsed.netloc, owner, repo)

            source = self.sources_dict[idx]

            commit_flag = source.get('commit_flag')
            pr_flag = source.get('pr_flag')
            if (commit_flag is None or pr_flag is None):
                mesg = "Could not determine if statuses are disabled for '{}/{}' in toolchainId '{}'. \
                        Check settings in DevOps Insights.".format(owner,
                                                                   repo,
                                                                   toolchainId)
                logger.debug(mesg)
            else:
                if (action == "commit_processing" and not commit_flag):
                    mesg = "Commit Statuses are disabled for '{}/{}' in toolchainId '{}'. \
                            Check settings in DevOps Insights.".format(owner,
                                                                       repo,
                                                                       toolchainId)
                    logger.debug(mesg)
                    continue

                if (action == "pr_processing" and not pr_flag):
                    mesg = "Pull Request Statuses are disabled for '{}/{}' in toolchainId '{}'. \
                            Check settings in DevOps Insights.".format(owner,
                                                                       repo,
                                                                       toolchainId)
                    logger.debug(mesg)
                    continue

            git_url_api_repos = Statuses.get_git_url_api_repos(source)

            logger.debug("PROCESSING parsed: '%s' ", parsed)
            logger.debug("git_url_api_repos: '%s'",
                         git_url_api_repos)
            logger.debug(" owner: %s", owner)
            logger.debug(" repo: %s", repo)

            if idx in self.sources_dict:

                res = self.sources_dict.get(idx)

                git_token = res['access_token']

                webhook_id = ""
                if 'webhook_id' in res:
                    webhook_id = res['webhook_id']

                orgId = "default"
                if 'orgId' in res:
                    orgId = res['orgId']

                logger.debug("correlate_commits_with_sources action: '%s'",
                             action)

                defect_likelihood = commit_record['defect_likelihood']
                probability = commit_record['probability']

                if defect_likelihood == 'High':
                    state = 'failure'
                    desc = 'Code Review Recommended. Check Details'
                elif defect_likelihood == 'Moderate':
                    state = 'success'
                    desc = 'No Code Review Recommended.'
                elif defect_likelihood == 'Low':
                    state = 'success'
                    desc = 'No Code Review Recommended.'
                else:
                    state = 'success'
                    desc = 'No Analyzable Code'

                if 'dev' in self.group_name:
                    doi_callback_base = doi_callback_dev
                elif 'staging' in self.group_name:
                    doi_callback_base = doi_callback_staging
                else:
                    doi_callback_base = doi_callback_prod

                if action == "commit_processing":

                    commit_sha = commit_record['commit_id']

                    bm_target_url = make_url(doi_callback_base,
                                             'commits',
                                             orgName=orgId,
                                             toolchainId=toolchainId)

                    desc = "Commit Risk is %s; probability %s. " % (
                        defect_likelihood, probability)

                elif action == "pr_processing":

                    array_commit_shas = commit_record['pull_commit_shas']
                    # for a PR, we're going to attach the status to
                    # the last commit in the list of that PR
                    #
                    if len(array_commit_shas) == 0:
                        continue
                    if source['source_type'] == 'gitlab_repo':
                        commit_sha = array_commit_shas[0]
                        pull_id = -commit_record['pull_id']
                    else:
                        commit_sha = array_commit_shas[-1]
                        pull_id = commit_record['pull_id']

                    bm_target_url = make_url(doi_callback_base,
                                             'pullrequests',
                                             orgName=orgId,
                                             toolchainId=toolchainId,
                                             pullId=pull_id)

                else:
                    # Action not supported - this should never happen
                    logger.error("Action '%s' not supported!", action)
                    return {}

                logger.debug(">>>>>> Action '%s' bm_target_url: '%s'"
                             % (action, bm_target_url))

                if "gitlab_repo" in source['source_type']:
                    git_url_statuses = make_url_gitlab(git_url_api_repos,
                                                       owner,
                                                       repo,
                                                       'statuses',
                                                       commit_sha)
                else:
                    git_url_statuses = make_url(git_url_api_repos,
                                                owner,
                                                repo,
                                                'statuses',
                                                commit_sha)

                contxt = "IBMDevOpsInsights (" + self.project['created_by'] + \
                    " - " + self.project['description'] + ")"
                description = {}
                description['state'] = state
                description['target_url'] = bm_target_url
                description['description'] = desc
                description['context'] = contxt

                logger.debug("git_url_statuses: '%s' ", git_url_statuses)
                logger.debug("description: '%s' ", description)
                logger.debug("webhook_id: '%s' ", webhook_id)
                rc_array.append({'git_url_statuses': git_url_statuses,
                                 'git_url_api_repos': git_url_api_repos,
                                 'owner': owner,
                                 'repo': repo,
                                 'toolchainId': toolchainId,
                                 'git_token': git_token,
                                 'desc': description,
                                 'webhook_id': webhook_id,
                                 'source': source})
            else:
                logger.info(
                    "The commit record url '%s' derived from commit_record[url]"
                    "was not in any project payload sources", idx)

        return rc_array

    def _process_statuses(self, action):
        '''
        This process is designed to only be called
        from the stream_miner.py in RUDI
        '''
        logger.debug("inside _process_statuses(%s)", action)
        logger.debug("Processing toolchainId = %s ", self.project['name'])

        try:
            logger.debug("calling transform_sources...")
            srcs_dict = self.transform_sources()

            if srcs_dict is None:
                logger.info(
                    "No webhooks in any sources for toolchainId {}".format(
                        self.project['name']))
                return None

            logger.debug("calling correlate_commits_with_sources...")
            rc_array = self.correlate_commits_with_sources(action)

            for i in rc_array:

                git_client = GitClient(source=i['source'], wait=False)

                # check for webhook
                logger.debug("_process_statuses(): check for webhooks")

                if "gitlab_repo" in i['source']['source_type']:
                    git_webhook_url = make_url_gitlab(i['git_url_api_repos'],
                                                      i['owner'],
                                                      i['repo'],
                                                      'hooks')
                else:
                    git_webhook_url = make_url(i['git_url_api_repos'],
                                               i['owner'],
                                               i['repo'],
                                               'hooks')

                toolchainId = i['toolchainId']

                logger.info("Checking for webhooks configured "
                            "with toolchainId: '%s' "
                            "at URL: '%s'"
                            % (toolchainId, git_webhook_url))
                try:
                    webhook_id = git_client.get_webhook_id(
                        git_url=git_webhook_url,
                        toolchainId=toolchainId,
                        space=self.group_name)
                except Exception:
                    webhook_id = None
                    logger.error("Lookup webhook failed for toolchainId: '%s'",
                                 toolchainId)
                    logger.error(traceback.print_exc())
                    continue

                if webhook_id is None:
                    logger.info("No webhook found for toolchainId: '%s'",
                                toolchainId)
                    continue

                try:
                    logger.info(">>>> POST status url: "
                                "'%s' to git for TC: "
                                "'%s' webhook_id: '%s'",
                                i['git_url_statuses'], toolchainId, webhook_id)
                    git_client.post_to_git_api(git_url=i['git_url_statuses'],
                                               desc=i['desc'])
                except Exception:
                    logger.error("POST FAILED for status url: "
                                 "'%s' to git for TC: "
                                 "'%s' webhook_id: '%s'",
                                 i['git_url_statuses'], toolchainId, webhook_id)
                    logger.error(traceback.print_exc())
                    continue

        except Exception:
            logger.error(traceback.print_exc())
            return None

    def process_pr_statuses(self):
        return self._process_statuses(action="pr_processing")

    def process_commit_statuses(self):
        return self._process_statuses(action="commit_processing")
