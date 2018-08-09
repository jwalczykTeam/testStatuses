'''
Created on Mar 10, 2016

@author: alberto
'''

from doi.common import PermanentlyFailedException
from doi.jobs.miner.resourcedb import ResourceDB, ResourceDBBulkInserter
from doi.jobs.miner.resourcedb import ResourceDBBulkUpdater
from doi.util.db import elasticsearch_util
from doi.jobs.miner import github
from doi.jobs.miner import gitlab
from doi.jobs.miner import bitbucket
from doi.jobs.miner import bitbucketserver
from doi.jobs.miner.miner import Miner
from doi.jobs.miner.miner import mine_data
from doi.util import file_util
from doi.util import log_util
from doi.jobs.miner import fields as f
import time


logger = log_util.get_logger("astro.jobs.miner")


class Preparer(Miner):

    def __init__(self, params, progress_callback, stop_event):
        super(Preparer, self).__init__(params, progress_callback, stop_event)

    def mine_data(self):
        self.validate_sources()

        with ResourceDB(self.job_name) as resource_db:
            logger.info("Creating database: '%s'", resource_db.db_name)
            resource_db.create_database()

        self.create_repo_to_index_mapping()

    def delete_data(self):
        with ResourceDB(self.job_name) as resource_db:
            logger.info("Deleting database: '%s'", resource_db.db_name)
            resource_db.delete_database()

    def create_repo_to_index_mapping(self):
        index_name = 'repo-map'
        epoch_time = int(time.time()) * 1000
        with ResourceDB(index_name) as res_db:
            if res_db.db.database_exists(index_name):
                for source in self.sources:
                    if 'git_url' not in source:
                        continue
                    if res_db.db.exists(
                            index_name,
                            ResourceDB.REPO_MAP_TYPE.name,
                            {f.GIT_URI: source['git_url']}):

                        try:
                            with ResourceDBBulkUpdater(
                                res_db,
                                ResourceDB.REPO_MAP_TYPE.name
                            ) as writer:
                                writer.write(
                                    {
                                        f.GIT_URI: source['git_url'],
                                        f.INDEX_NAME: self.job_name,
                                        'timestamp': epoch_time
                                    })
                        except elasticsearch_util.ElasticsearchIndexException:
                            with ResourceDBBulkInserter(
                                    res_db,
                                    ResourceDB.REPO_MAP_TYPE.name) as writer:
                                writer.write(
                                    {
                                        f.GIT_URI: source['git_url'],
                                        f.INDEX_NAME: self.job_name,
                                        'timestamp': epoch_time
                                    })

                    else:
                        with ResourceDBBulkInserter(
                                res_db,
                                ResourceDB.REPO_MAP_TYPE.name) as writer:
                            writer.write(
                                {
                                    f.GIT_URI: source['git_url'],
                                    f.INDEX_NAME: self.job_name,
                                    'timestamp': epoch_time
                                })
            else:
                res_db.create_database()
                for source in self.sources:
                    if 'git_url' not in source:
                        continue
                    with ResourceDBBulkInserter(
                        res_db,
                        ResourceDB.REPO_MAP_TYPE.name
                    ) as writer:
                        writer.write(
                            {
                                f.GIT_URI: source['git_url'],
                                f.INDEX_NAME: self.job_name,
                                'timestamp': epoch_time
                            })

    def validate_sources(self):
        logger.info("Validating work dir: '%s'", self.work_dir)
        if not file_util.is_path_absolute(self.work_dir):
            # Work dir must be absolute for security reasons
            raise PermanentlyFailedException(
                "about:blank",
                "Internal Configuration Error",
                500,
                "Astro work dir must be absolute: '{}'".format(self.work_dir))

        logger.info("Validate sources ...")
        for source in self.sources:
            source_type = source['source_type']
            if source_type == 'github_repo':
                api_base_url = source['api_base_url']
                access_token = source['access_token']
                repo_owner = source['repo_owner']
                repo_name = source['repo_name']

                # Raises exception if repo does not exist
                repo_uri = github.GitHubClient.REPO_URI_FORMAT.format(
                    api_base_url, repo_owner, repo_name)
                github.get_repo(repo_uri, access_token)

            elif source_type == 'gitlab_repo':
                api_base_url = source['api_base_url']
                access_token = source['access_token']
                repo_owner = source['repo_owner']
                repo_name = source['repo_name']

                # raise exception if repo does not exist
                repo_uri = gitlab.GitLabClient.REPO_URI_FORMAT.format(
                    api_base_url, repo_owner, repo_name)
                gitlab.get_repo(repo_uri, access_token)

            elif source_type == 'bitbucket_repo':
                api_base_url = source['api_base_url']
                access_token = source['access_token']
                repo_owner = source['repo_owner']
                repo_name = source['repo_name']

                # raise exception if repo does not exist
                repo_uri = bitbucket.BitBucketClient.REPO_URI_FORMAT.format(
                    api_base_url, repo_owner, repo_name)
                bitbucket.get_repo(repo_uri, access_token)

            elif source_type == 'bitbucketserver_repo':
                api_base_url = source['api_base_url']
                access_token = source['access_token']
                repo_owner = source['repo_owner']
                repo_name = source['repo_name']

                # raise exception if repo does not exist
                repo_uri = bitbucketserver.BitBucketServerClient.REPO_URI_FORMAT.format(
                    api_base_url, repo_owner, repo_name)
                bitbucketserver.get_repo(repo_uri, access_token)

            elif source_type == 'jira_project':
                pass
            elif source_type == 'rtc_project':
                pass
            elif source_type == 'jenkins_job':
                pass


def prepare_data(params, progress_callback, stop_event):
    return mine_data(Preparer, params, progress_callback, stop_event)
