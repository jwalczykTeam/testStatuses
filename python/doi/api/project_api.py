'''
Created on Aug 23, 2016

@author: alberto, jwalczyk
'''

from doi import constants
from doi.common import FailedException
from doi.api.job_api import JobApi
from doi.scheduler.common import Job, Task, Condition
from doi.util import log_util
from doi.util.log_util import mask_sensitive_info
from doi.util import uri_util
from doi.util.db import db_util
from doi.insights.best_practices import common as bp
from doi.util import ci_server_webhooks
import datetime
import random
import traceback
'''
Examples:
http://localhost:8080/v1/projects/astro/reports/distribution?resource_type=issue&group_by_field=issue_types
http://localhost:8080/v1/projects/astro/reports/distribution?resource_type=issue&group_by_field=issue_types&from_time="2016-10-01"
http://localhost:8080/v1/projects/astro/reports/distribution?resource_type=committed_file&sum_by_field=file_changed_line_count&group_by_field=commit_issues.issue_types
http://localhost:8080/v1/projects/astro/reports/distribution?resource_type=commit&group_by_field=commit_author_name
http://localhost:8080/v1/projects/astro/reports/time_series?resource_type=issue&group_by_field=issue_types&interval=month
http://localhost:8080/v1/projects/astro/reports/time_series?resource_type=committed_file&group_by_field=commit_issues.issue_types&interval=month
http://localhost:8080/v1/projects/astro/reports/time_series?resource_type=committed_file&sum_by_field=file_changed_line_count&group_by_field=commit_issues.issue_types&interval=month
http://localhost:8080/v1/projects/astro/reports/time_series?resource_type=committed_file&sum_by_field=file_changed_line_count&group_by_field=commit_issues.issue_types&interval=month&from_time="2016-10-01"
http://localhost:8080/v1/projects/astro/reports/best_practices/big_commits/distribution
http://localhost:8080/v1/projects/astro/reports/best_practices/big_commits/time_series
http://localhost:8080/v1/projects/astro/reports/best_practices/hero_project/distribution
http://localhost:8080/v1/projects/astro/reports/best_practices/hero_project/time_series
http://localhost:8080/v1/projects/astro/reports/best_practices/not_linked_commits/distribution
http://localhost:8080/v1/projects/astro/reports/best_practices/not_linked_commits/time_series
http://localhost:8080/v1/projects/astro/reports/best_practices/not_typed_issues/distribution
http://localhost:8080/v1/projects/astro/reports/best_practices/not_typed_issues/time_series
http://localhost:8080/v1/projects/astro/reports/best_practices/summary/distribution
http://localhost:8080/v1/projects/astro/reports/best_practices/summary/time_series
'''


class MineTask(Task):
    def __init__(self, id_, function, condition=None):

        super(MineTask, self).__init__(id_, function, condition)


GITHUB_REPO_TYPE            = 'github_repo'
GITLAB_REPO_TYPE            = 'gitlab_repo'
BITBUCKET_REPO_TYPE         = 'bitbucket_repo'
BITBUCKETSERVER_REPO_TYPE   = 'bitbucketserver_repo'
JIRA_PROJECT_TYPE           = 'jira_project'
RTC_PROJECT_TYPE            = 'rtc_project'
JENKINS_JOB_TYPE            = 'jenkins_job'

PREPARE_DATA_ID = "prepare_data"
PREPARE_DATA_FUNCTION = "doi.jobs.miner.preparer.prepare_data"

MINE_GITHUB_ISSUES_ID = "mine_github_issues"
MINE_GITHUB_ISSUES_FUNCTION = "doi.jobs.miner.github_miner.mine_github_issues"

MINE_GITHUB_BRANCHES_ID = "mine_github_branches"
MINE_GITHUB_BRANCHES_FUNCTION = "doi.jobs.miner.github_miner.mine_github_branches"

MINE_GITHUB_CONTRIBUTORS_ID = "mine_github_contributors"
MINE_GITHUB_CONTRIBUTORS_FUNCTION = "doi.jobs.miner.github_miner.mine_github_contributors"

MINE_GITHUB_COMMITS_ID = "mine_github_commits"
MINE_GITHUB_COMMITS_FUNCTION = "doi.jobs.miner.github_miner.mine_github_commits"

MINE_GITHUB_PULLS_ID = "mine_github_pulls"
MINE_GITHUB_PULLS_FUNCTION = "doi.jobs.miner.github_miner.mine_github_pulls"

MINE_GITLAB_ISSUES_ID = "mine_gitlab_issues"
MINE_GITLAB_ISSUES_FUNCTION = "doi.jobs.miner.gitlab_miner.mine_gitlab_issues"

MINE_GITLAB_BRANCHES_ID = "mine_gitlab_branches"
MINE_GITLAB_BRANCHES_FUNCTION = "doi.jobs.miner.gitlab_miner.mine_gitlab_branches"

MINE_GITLAB_CONTRIBUTORS_ID = "mine_gitlab_contributors"
MINE_GITLAB_CONTRIBUTORS_FUNCTION = "doi.jobs.miner.gitlab_miner.mine_gitlab_contributors"

MINE_GITLAB_COMMITS_ID = "mine_gitlab_commits"
MINE_GITLAB_COMMITS_FUNCTION = "doi.jobs.miner.gitlab_miner.mine_gitlab_commits"

MINE_GITLAB_PULLS_ID = "mine_gitlab_pulls"
MINE_GITLAB_PULLS_FUNCTION = "doi.jobs.miner.gitlab_miner.mine_gitlab_pulls"

MINE_BITBUCKET_ISSUES_ID = "mine_bitbucket_issues"
MINE_BITBUCKET_ISSUES_FUNCTION = "doi.jobs.miner.bitbucket_miner.mine_bitbucket_issues"

MINE_BITBUCKET_BRANCHES_ID = "mine_bitbucket_branches"
MINE_BITBUCKET_BRANCHES_FUNCTION = "doi.jobs.miner.bitbucket_miner.mine_bitbucket_branches"

MINE_BITBUCKET_COMMITS_ID = "mine_bitbucket_commits"
MINE_BITBUCKET_COMMITS_FUNCTION = "doi.jobs.miner.bitbucket_miner.mine_bitbucket_commits"

MINE_BITBUCKET_PULLS_ID = "mine_bitbucket_pulls"
MINE_BITBUCKET_PULLS_FUNCTION = "doi.jobs.miner.bitbucket_miner.mine_bitbucket_pulls"

MINE_BITBUCKETSERVER_BRANCHES_ID = "mine_bitbucketserver_branches"
MINE_BITBUCKETSERVER_BRANCHES_FUNCTION = "doi.jobs.miner.bitbucketserver_miner.mine_bitbucketserver_branches"

MINE_BITBUCKETSERVER_COMMITS_ID = "mine_bitbucketserver_commits"
MINE_BITBUCKETSERVER_COMMITS_FUNCTION = "doi.jobs.miner.bitbucketserver_miner.mine_bitbucketserver_commits"

MINE_JIRA_ISSUES_ID = "mine_jira_issues"
MINE_JIRA_ISSUES_FUNCTION = "doi.jobs.miner.jira_miner.mine_jira_issues"

MINE_RTC_ISSUES_ID = "mine_rtc_issues"
MINE_RTC_ISSUES_FUNCTION = "doi.jobs.miner.rtc_miner.mine_rtc_issues"

MINE_JENKINS_JOB_ID = "mine_jenkins_job"
MINE_JENKINS_JOB_FUNCTION = "doi.jobs.miner.jenkins_miner.mine_jenkins_job"

JOIN_DATA_ID = "join_data"
JOIN_DATA_FUNCTION = "doi.jobs.miner.joiner.join_data"

ANALYZE_CURRENT_DATA_ID = "analyze_current_data"
ANALYZE_CURRENT_DATA_FUNCTION = "doi.jobs.analyzer.analyzer.analyze_current_data"

PROCESS_KIBANA_DASHBOARDS_ID = "process_kibana_dashboards"
PROCESS_KIBANA_DASHBOARDS_FUNCTION = "doi.jobs.kibana.kibana.process_project_dashboards"

logger = log_util.get_logger("astro.api.project_api")


class ProjectApi(JobApi):
    # Designed not to raise exceptions
    def __init__(self, config):
        logger.info("Initializing project api: %s ...",
                    mask_sensitive_info(config))
        super(ProjectApi, self).__init__(config)

        self.group_id = config['group_id']
        logger.info("group_id=%s", self.group_id)

        self.group_name = config['group_id'].split('-')[-1]
        logger.info("group_name=%s", self.group_name)

        self.metrics_index = 'metrics@{}'.format(self.group_name)

        self.db = db_util.Database.factory(config["db"])
        logger.info("Project api initialized.")

    def close(self):

        logger.info("Closing project api ...")
        super(ProjectApi, self).close()

        if self.db is not None:
            self.db.close()
            self.db = None
        logger.info("Project api closed.")

    def _get_project_full_name(self, tenant_id, name):
        '''
        Returns:
            <project>
            <project>@<tenant>
            <project>@<group>
            <project>@<group>.<tenant>
        '''
        assert name, "Missing mandatory project name"

        full_name = name + '@' + self.group_id
        if tenant_id:
            full_name += '.' + tenant_id

        return full_name

#
#   Project methods
#
    def list_projects(self, request_info, id_=None, name=None,
                      state=None, completed=None):
        return self.list_jobs(request_info, constants.PROJECT_JOB_TYPE, id_,
                              name, state, completed)

    def list_metrics(self, request_info, id_=None, name=None,
                     state=None, completed=None):
        return self.list_metric_jobs(request_info,
                                     constants.PROJECT_JOB_TYPE,
                                     id_,
                                     name,
                                     state,
                                     completed,
                                     self.metrics_index)

    def get_project(self, request_info, project_name):
        return self.get_job(request_info, project_name)

    def get_metric(self, request_info, project_name):
        return self.get_metric_job(request_info, project_name, self.metrics_index)

    def get_metrics(self, request_info, project_name):
        return self.get_metrics_job(request_info, project_name)

    def add_project(self, request_info, body):

        PID = random.randint(1, 10001)
        print(datetime.datetime.now())

        job = Job()
        job.job_type = constants.PROJECT_JOB_TYPE
        job.tenant_id = request_info['tenant_id']
        job.created_by = request_info['user_name']
        job.name = body['name']
        job.title = body.get('title', job.name)
        job.full_name = self._get_project_full_name(job.tenant_id, job.name)
        job.description = body.get('description')
        job.html_url = body.get('html_url')
        job.channels.decode(body.get("channels"))

        job_input = {}
        if "default_project_options" in self.config:
            job_input.update(self.config["default_project_options"])
        job_input.update(body)
        job_input.pop("name", None)
        job_input.pop("title", None)
        job_input.pop("description", None)
        job_input.pop("html_url", None)
        job_input.pop("channels", None)

        # Create webhook & store / update ES
        try:
            print(datetime.datetime.now())
            hwh = ci_server_webhooks.HandleWebhook(pid=PID)
            toolchainId = body['name']
            updated_sources = hwh.add_webhooks(job_input['sources'],
                                               request_info['request_url'],
                                               toolchainId)
            if updated_sources is not None:
                job_input['sources'] = updated_sources

            print(datetime.datetime.now())
        except Exception:
            logger.error(traceback.print_exc())
            print "***  ADD WEBHOOKS FAILED *** ", PID

        job.input = job_input

        analyze_data = body.get("analyze_current_data", True)
        self.add_tasks(job, job_input['sources'], analyze_data)

        rc = self.create_job(request_info['request_url'], job)

        return rc

    def add_tasks(self, job, sources, analyze_data):
        source_types = set(source['source_type'] for source in sources)
        mining_task_ids = set()

        # Prepare data
        job.add_task(Task(PREPARE_DATA_ID, PREPARE_DATA_FUNCTION))

        if GITHUB_REPO_TYPE in source_types:
            # Mine github branches
            job.add_task(MineTask(MINE_GITHUB_BRANCHES_ID,
                                  MINE_GITHUB_BRANCHES_FUNCTION,
                                  Condition([PREPARE_DATA_ID])))
            mining_task_ids.add(MINE_GITHUB_BRANCHES_ID)

            # Mine github contributors
            job.add_task(MineTask(MINE_GITHUB_CONTRIBUTORS_ID,
                                  MINE_GITHUB_CONTRIBUTORS_FUNCTION,
                                  Condition([PREPARE_DATA_ID])))
            mining_task_ids.add(MINE_GITHUB_CONTRIBUTORS_ID)

            # Mine github issues
            job.add_task(MineTask(MINE_GITHUB_ISSUES_ID,
                                  MINE_GITHUB_ISSUES_FUNCTION,
                                  Condition([PREPARE_DATA_ID])))
            mining_task_ids.add(MINE_GITHUB_ISSUES_ID)

            # Mine github commits
            job.add_task(MineTask(MINE_GITHUB_COMMITS_ID,
                                  MINE_GITHUB_COMMITS_FUNCTION,
                                  Condition([PREPARE_DATA_ID])))
            mining_task_ids.add(MINE_GITHUB_COMMITS_ID)

            '''
            # Mine github pull requests
            job.add_task(MineTask(MINE_GITHUB_PULLS_ID,
                                  MINE_GITHUB_PULLS_FUNCTION,
                                  Condition([PREPARE_DATA_ID])))
            mining_task_ids.append(MINE_GITHUB_PULLS_ID)
            '''

        if GITLAB_REPO_TYPE in source_types:
            # Mine gitlab branches
            job.add_task(MineTask(MINE_GITLAB_BRANCHES_ID,
                                  MINE_GITLAB_BRANCHES_FUNCTION,
                                  Condition([PREPARE_DATA_ID])))
            mining_task_ids.add(MINE_GITLAB_BRANCHES_ID)

            # Mine gitlab contributors
            job.add_task(MineTask(MINE_GITLAB_CONTRIBUTORS_ID,
                                  MINE_GITLAB_CONTRIBUTORS_FUNCTION,
                                  Condition([PREPARE_DATA_ID])))
            mining_task_ids.add(MINE_GITLAB_CONTRIBUTORS_ID)

            # Mine gitlab issues
            job.add_task(MineTask(MINE_GITLAB_ISSUES_ID,
                                  MINE_GITLAB_ISSUES_FUNCTION,
                                  Condition([PREPARE_DATA_ID])))
            mining_task_ids.add(MINE_GITLAB_ISSUES_ID)

            # Mine gitlab commits
            job.add_task(MineTask(MINE_GITLAB_COMMITS_ID,
                                  MINE_GITLAB_COMMITS_FUNCTION,
                                  Condition([PREPARE_DATA_ID])))
            mining_task_ids.add(MINE_GITLAB_COMMITS_ID)

            '''
            # Mine gitlab pull requests
            job.add_task(MineTask(MINE_GITLAB_PULLS_ID,
                                  MINE_GITLAB_PULL_REQUESTS_FUNCTION,
                                  Condition([PREPARE_DATA_ID])))
            mining_task_ids.append(MINE_GITLAB_PULLS_ID)
            '''

        if BITBUCKET_REPO_TYPE in source_types:
            # Mine bitbucket branches
            job.add_task(MineTask(MINE_BITBUCKET_BRANCHES_ID,
                                  MINE_BITBUCKET_BRANCHES_FUNCTION,
                                  Condition([PREPARE_DATA_ID])))
            mining_task_ids.add(MINE_BITBUCKET_BRANCHES_ID)

            # Mine bitbucket issues
            job.add_task(MineTask(MINE_BITBUCKET_ISSUES_ID,
                                  MINE_BITBUCKET_ISSUES_FUNCTION,
                                  Condition([PREPARE_DATA_ID])))
            mining_task_ids.add(MINE_BITBUCKET_ISSUES_ID)

            # Mine bitbucket commits
            job.add_task(MineTask(MINE_BITBUCKET_COMMITS_ID,
                                  MINE_BITBUCKET_COMMITS_FUNCTION,
                                  Condition([PREPARE_DATA_ID])))
            mining_task_ids.add(MINE_BITBUCKET_COMMITS_ID)

        if BITBUCKETSERVER_REPO_TYPE in source_types:
            # Mine bitbucket branches
            job.add_task(MineTask(MINE_BITBUCKETSERVER_BRANCHES_ID,
                                  MINE_BITBUCKETSERVER_BRANCHES_FUNCTION,
                                  Condition([PREPARE_DATA_ID])))
            mining_task_ids.add(MINE_BITBUCKETSERVER_BRANCHES_ID)

            # Mine bitbucket commits
            job.add_task(MineTask(MINE_BITBUCKETSERVER_COMMITS_ID,
                                  MINE_BITBUCKETSERVER_COMMITS_FUNCTION,
                                  Condition([PREPARE_DATA_ID])))
            mining_task_ids.add(MINE_BITBUCKETSERVER_COMMITS_ID)

        if JIRA_PROJECT_TYPE in source_types:
            # Mine jira issues
            job.add_task(MineTask(MINE_JIRA_ISSUES_ID,
                                  MINE_JIRA_ISSUES_FUNCTION,
                                  Condition([PREPARE_DATA_ID])))
            mining_task_ids.add(MINE_JIRA_ISSUES_ID)

        if RTC_PROJECT_TYPE in source_types:
            # Mine rtc issues
            job.add_task(MineTask(MINE_RTC_ISSUES_ID, MINE_RTC_ISSUES_FUNCTION,
                                  Condition([PREPARE_DATA_ID])))
            mining_task_ids.add(MINE_RTC_ISSUES_ID)

        if JENKINS_JOB_TYPE in source_types:
            # Mine jenkins builds
            job.add_task(MineTask(MINE_JENKINS_JOB_ID,
                                  MINE_JENKINS_JOB_FUNCTION,
                                  Condition([PREPARE_DATA_ID])))
            mining_task_ids.add(MINE_JENKINS_JOB_ID)

        # Join data
        if mining_task_ids:
            job.add_task(Task(JOIN_DATA_ID, JOIN_DATA_FUNCTION,
                              Condition(list(mining_task_ids))))

            # Process kibana dashboards
            job.add_task(Task(PROCESS_KIBANA_DASHBOARDS_ID,
                              PROCESS_KIBANA_DASHBOARDS_FUNCTION,
                              Condition([JOIN_DATA_ID])))

            if analyze_data:
                job.add_task(Task(ANALYZE_CURRENT_DATA_ID,
                                  ANALYZE_CURRENT_DATA_FUNCTION,
                                  Condition([JOIN_DATA_ID])))

    def update_project(self, request_info, project_name, body):
        #  Start webhook processing
        try:
            PID = random.randint(1, 10001)
            print(datetime.datetime.now())
            hwh = ci_server_webhooks.HandleWebhook(pid=PID)
            hwh.delete_webhooks(project_name)
            print(datetime.datetime.now())
        except Exception:
            logger.error(traceback.print_exc())
            print "*** UPDATE WEBHOOKS (DELETE) FAILED ***", PID
        # End webhook processing

        job = Job()
        job.job_type = constants.PROJECT_JOB_TYPE
        job.tenant_id = request_info['tenant_id']
        job.last_updated_by = request_info['user_name']
        job.name = project_name
        job.title = body.get('title', job.name)
        job.full_name = self._get_project_full_name(job.tenant_id, job.name)
        job.description = body.get('description')
        job.html_url = body.get('html_url')
        job.channels.decode(body.get("channels"))

        job_input = {}
        if "default_project_options" in self.config:
            job_input.update(self.config["default_project_options"])
        job_input.update(body)
        job_input.pop("name", None)
        job_input.pop("title", None)
        job_input.pop("description", None)
        job_input.pop("html_url", None)
        job_input.pop("channels", None)

        # Create webhook & store / update ES
        try:
            print(datetime.datetime.now())

            updated_sources = hwh.add_webhooks(job_input['sources'],
                                               request_info['request_url'])
            if updated_sources is not None:
                job_input['sources'] = updated_sources

            print(datetime.datetime.now())
        except Exception:
            logger.error(traceback.print_exc())
            print "*** UPDATE WEBHOOKS (ADD) FAILED *** ", PID

        job.input = job_input

        analyze_data = body.get("analyze_current_data", True)
        self.add_tasks(job, job_input['sources'], analyze_data)
        rc = self.update_job(request_info, job)

        return rc

    def patch_project(self, request_info, project_name, body):
        return self.patch_job(request_info, project_name, body)

    def delete_project(self, request_info, project_name):

        # Delete webhook & store / update ES
        try:
            PID = random.randint(1, 10001)
            print(datetime.datetime.now())
            hwh = ci_server_webhooks.HandleWebhook(pid=PID)
            hwh.delete_webhooks(project_name)
            print(datetime.datetime.now())
        except Exception:
            logger.error(traceback.print_exc())
            print "*** PROCESS DELETE WEBHOOK FAILED *** ", PID
        # End webhook processing

        return self.delete_job(request_info, project_name)

#
#   Query Methods
#
    def query_project_resources(self, request_info, project_name,
                                resource_type,
                                where=None, from_time=None, to_time=None,
                                sort_by=None, sort_order=None,
                                start=None, size=None, fields=None,
                                min_probability=None):
        try:
            job = self.manager.get_job_by_name(request_info['tenant_id'],
                                               project_name)
            results = self.db.query(
                job.full_name, resource_type,
                where, from_time, to_time,
                sort_by, sort_order,
                start, size, fields, min_probability)
            return self.build_data(200, results)  # OK
        except FailedException as e:
            return self.build_error(e)

#
#   Reports Methods
#

    def get_project_resource_distribution(self, request_info, project_name,
                                          resource_type,
                                          group_by_field, sum_by_field,
                                          from_time, to_time, size,
                                          min_probability):
        try:
            job = self.manager.get_job_by_name(request_info['tenant_id'],
                                               project_name)
            resource_type = uri_util.unquote_query_param(resource_type)
            group_by_field = uri_util.unquote_query_param(group_by_field)
            sum_by_field = uri_util.unquote_query_param(sum_by_field)
            from_time = uri_util.unquote_query_param(from_time)
            to_time = uri_util.unquote_query_param(to_time)
            min_probability = uri_util.unquote_query_param(min_probability)

            if sum_by_field is not None:
                results = self.db.get_sum_distribution(
                    job.full_name,
                    resource_type, sum_by_field, group_by_field,
                    from_time, to_time, size, min_probability)
            else:
                results = self.db.get_count_distribution(
                    job.full_name,
                    resource_type, group_by_field,
                    from_time, to_time, size, min_probability)
            return self.build_data(200, results)  # OK
        except FailedException as e:
            return self.build_error(e)

    def get_project_resource_distribution_ex(self, request_info, project_name,
                                             aggregations, resource_type,
                                             from_time, to_time,
                                             query, min_probability):
        try:
            job = self.manager.get_job_by_name(request_info['tenant_id'],
                                               project_name)
            resource_type = uri_util.unquote_query_param(resource_type)
            from_time = uri_util.unquote_query_param(from_time)
            to_time = uri_util.unquote_query_param(to_time)

            results = self.db.get_count_distribution_ex(job.full_name,
                                                        aggregations,
                                                        resource_type,
                                                        from_time,
                                                        to_time,
                                                        query,
                                                        min_probability)
            return self.build_data(200, results)  # OK
        except FailedException as e:
            return self.build_error(e)

    def get_project_resource_distribution_ex_freeform(self, request_info,
                                                      project_name, query,
                                                      resource_type):
        try:
            job = self.manager.get_job_by_name(request_info['tenant_id'],
                                               project_name)
            resource_type = uri_util.unquote_query_param(resource_type)

            results = self.db.get_count_distribution_ex_freeform(job.full_name,
                                                                 query,
                                                                 resource_type)
            return self.build_data(200, results)  # OK
        except FailedException as e:
            return self.build_error(e)

    def get_project_resource_time_series(self, request_info, project_name,
                                         resource_type,
                                         group_by_field, sum_by_field=None,
                                         interval=None, from_time=None,
                                         to_time=None, time_format=None,
                                         size=None):
        try:
            job = self.manager.get_job_by_name(request_info['tenant_id'],
                                               project_name)
            resource_type = uri_util.unquote_query_param(resource_type)
            interval = uri_util.unquote_query_param(interval)
            group_by_field = uri_util.unquote_query_param(group_by_field)
            sum_by_field = uri_util.unquote_query_param(sum_by_field)
            from_time = uri_util.unquote_query_param(from_time)
            to_time = uri_util.unquote_query_param(to_time)
            time_format = uri_util.unquote_query_param(time_format)

            if sum_by_field is not None:
                results = self.db.get_sum_time_series(
                    job.full_name,
                    resource_type, sum_by_field, group_by_field,
                    interval,
                    from_time, to_time,
                    time_format, size)
            else:
                results = self.db.get_count_time_series(
                    job.full_name,
                    resource_type, group_by_field,
                    interval,
                    from_time, to_time,
                    time_format, size)
            return self.build_data(200, results)  # OK
        except FailedException as e:
            return self.build_error(e)

    def get_project_resource_time_series_ex(self, request_info, project_name,
                                            aggregations, resource_type,
                                            interval=None, from_time=None,
                                            to_time=None, time_format=None,
                                            query=None):
        try:
            job = self.manager.get_job_by_name(request_info['tenant_id'],
                                               project_name)
            resource_type = uri_util.unquote_query_param(resource_type)
            interval = uri_util.unquote_query_param(interval)
            from_time = uri_util.unquote_query_param(from_time)
            to_time = uri_util.unquote_query_param(to_time)
            time_format = uri_util.unquote_query_param(time_format)

            results = self.db.get_count_time_series_ex(
                job.full_name,
                aggregations,
                resource_type,
                interval,
                from_time, to_time,
                time_format, query)
            return self.build_data(200, results)  # OK
        except FailedException as e:
            return self.build_error(e)

    def get_project_resource_time_series_ex_freeform(self, request_info,
                                                     project_name, query,
                                                     resource_type):
        try:
            job = self.manager.get_job_by_name(request_info['tenant_id'],
                                               project_name)
            resource_type = uri_util.unquote_query_param(resource_type)

            results = self.db.get_count_time_series_ex_freeform(
                job.full_name,
                query,
                resource_type)
            return self.build_data(200, results)  # OK
        except FailedException as e:
            return self.build_error(e)

    def get_project_freeform(self, request_info, project_name,
                             query, resource_type):
        try:

            job = self.manager.get_job_by_name(request_info['tenant_id'],
                                               project_name)
            resource_type = uri_util.unquote_query_param(resource_type)

            results = self.db.get_count_freeform(job.full_name, query,
                                                 resource_type)

            return self.build_data(200, results)  # OK
        except FailedException as e:
            return self.build_error(e)


#
#   Best practices Methods
#

    def get_project_best_practice_distribution(self, request_info,
                                               project_name,
                                               best_practice_name,
                                               from_time, to_time):
        try:
            job = self.manager.get_job_by_name(request_info['tenant_id'],
                                               project_name)
            results = bp.best_practice_assess_distribution(best_practice_name,
                                                           self.db,
                                                           job.full_name,
                                                           from_time, to_time)

            return self.build_data(200, results)
        except FailedException as e:
            return self.build_error(e)

    def get_project_best_practice_time_series(self, request_info, project_name,
                                              best_practice_name,
                                              interval, from_time, to_time,
                                              time_format):
        try:
            job = self.manager.get_job_by_name(request_info['tenant_id'],
                                               project_name)
            results = bp.best_practice_assess_time_series(best_practice_name,
                                                          self.db,
                                                          job.full_name,
                                                          interval, from_time,
                                                          to_time, time_format)

            return self.build_data(200, results)
        except FailedException as e:
            return self.build_error(e)


def clean_source(source):
    ignore_keys = ['access_token', u'access_token']
    return {
        key: value for key, value in source.iteritems()
        if key not in ignore_keys}
