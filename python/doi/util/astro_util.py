'''
Cloud Foundry utils to parse VCAP_SERVICES for extracting Astro services
'''

from doi import constants
from doi.common import TaskMethod
from doi.util import cf_util, log_util
from doi.util.db import db_util
from doi.util.mb import mb_util
from doi.jobs.kibana import kibana
import argparse
import json
import logging
import os
import sys
import requests
import time


logger = log_util.get_logger('astro.doi.util.astro_util')


class AstroUtil(object):

    def __init__(self, db_config, mb_config):
        self.db_client = db_util.Database.factory(db_config)
        mb = mb_util.MessageBroker(mb_config)
        self.mb_manager = mb.new_manager()

    def clean(self, group_id, data=False):
        self.delete_job_manager_db(group_id)
        self.delete_job_manager_topics(group_id)
        if data:
            self.delete_data_dbs(group_id)

    def delete_job_manager_db(self, group_id):
        manager_database_name = constants.get_scheduler_db_name(group_id)
        logger.info("Deleting index '{}' ...".format(manager_database_name))
        self.db_client.delete_database(manager_database_name)
        logger.info("Index '{}' deleted".format(manager_database_name))

    def delete_job_manager_topics(self, group_id):
        manager_messages_topic_name = constants.get_scheduler_messages_topic_name(
            group_id)
        workers_messages_topic_name = constants.get_workers_messages_topic_name(
            group_id)
        resource_db_events_topic_name = constants.get_resource_db_events_topic_name(
            group_id)
        logger.info("Deleting topic '{}' ...".format(
            manager_messages_topic_name))
        self.mb_manager.delete_topic(manager_messages_topic_name)
        logger.info("Topic '{}' deleted".format(manager_messages_topic_name))
        logger.info("Deleting topic '{}' ...".format(
            workers_messages_topic_name))
        self.mb_manager.delete_topic(workers_messages_topic_name)
        logger.info("Topic '{}' deleted".format(workers_messages_topic_name))
        logger.info("Deleting topic '{}' ...".format(
            resource_db_events_topic_name))
        self.mb_manager.delete_topic(resource_db_events_topic_name)
        logger.info("Topic '{}' deleted".format(resource_db_events_topic_name))

    def delete_data_dbs(self, group_id):
        all_db_names = self.db_client.list_database_names()
        for db_name in all_db_names:
            if '@' + group_id in db_name:
                logger.info("Deleting index '{}' ...".format(db_name))
                self.db_client.delete_database(db_name)
                logger.info("Index '{}' deleted".format(db_name))
                logger.info(
                    "Deleting kibana resources for '{}' ...".format(db_name))
                kibana.process_dashboards(db_name, TaskMethod.DELETE)
                logger.info(
                    "Kibana resources for '{}' deleted".format(db_name))


class AstroClient(object):

    def __init__(self):

        self._get_config()

    def _get_config(self):

        config = json.loads(os.environ['VCAP_SERVICES'])
        for item in config['user-provided']:
            if 'astro' in item.get('name', '').lower():
                astro = item
            if 'elastic' in item.get('name', '').lower():
                self._es_config(item)

        self.rest_address = astro['credentials']['REST_API_URL'][0]
        self.astro_token = astro['credentials']['TOKEN']

        self.headers = {"x-Auth-Token": self.astro_token}

    def _es_config(self, service):

        # -rudicitest has no Rudi consumer of its own
        if "-rudicitest" in service['name']:
            self.scheduler_index = "astro-scheduler@astro-scheduler-adtech-1-rudicitest"
        elif "-sandbox" in service['name']:
            self.scheduler_index = "astro-scheduler@astro-scheduler-adtech-1-sandbox"
        elif "-dev" in service['name']:
            self.scheduler_index = "astro-scheduler@astro-scheduler-adtech-1-dev"
        elif "-staging" in service['name']:
            self.scheduler_index = "astro-scheduler@astro-scheduler-adtech-1-staging"
        elif "-prod" in service['name']:
            self.scheduler_index = "astro-scheduler@astro-scheduler-adtech-1-prod"
        elif "-kim" in service['name']:
            self.scheduler_index = "astro-scheduler@astro-scheduler-adtech-1-kim"
        else:
            logger.error(
                    "VCAP has an invalid ES name: {}".format(service['name']))

        db_config = cf_util.get_db_credentials()
        self.db = db_util.Database.factory(db_config)

    def _get_projects_from_api(self):
        while True:

            try:
                logger.info("Hitting astro API: {}".format(self.rest_address))
                response = requests.get(
                    self.rest_address,
                    headers=self.headers)
                return response.json()
            except Exception as e:
                logger.error(
                    "Could not hit astro at {}".format(self.rest_address))
                logger.error("Error: {}".format(e))
                time.sleep(60)

    def _get_projects_from_es(self):
        while True:

            try:
                logger.debug("Getting all projects at {}".format(
                    self.scheduler_index))
                projects = self.db.client.search(
                    self.scheduler_index,
                    {"size": 10000})["hits"]["hits"]

                # Excluding ES index info
                return [project["_source"] for project in projects]
            except Exception as e:
                logger.error(
                    "Could not hit ES at {}".format(self.scheduler_index))
                logger.error("Error: {}".format(e))
                time.sleep(60)

    def get_projects(self):

        return self._get_projects_from_es()

    def _get_project_from_api(self, name):

        try:
            project_uri = self.rest_address + "/" + name
            response = requests.get(project_uri, headers=self.headers)
            project_rest_payload = response.json()
            return project_rest_payload
        except Exception as e:
            logger.error(
                "Project name {} is not available in astro".format(name))
            logger.error(e)
            return None

    def _get_project_from_es(self, name):
        while True:

            try:
                if "@" in name:
                    query = self.db.query_string_match("full_name", name)
                else:
                    query = self.db.query_string_match("name", name)

                logger.debug("Getting project: {}".format(name))
                project = self.db.client.search(
                    index=self.scheduler_index,
                    body=query)["hits"]["hits"]

                if project == []:
                    logger.debug("Project doesn't exist: {}".format(name))
                    return None
                else:
                    project = project[0]["_source"]
                    return project
            except Exception as e:
                logger.error(
                    "Could not hit ES at {}".format(self.scheduler_index))
                logger.error("Error: {}".format(e))
                time.sleep(60)

    def get_project(self, name):

        return self._get_project_from_es(name)

    def has_project_changed(self, p):
        current_p = self.get_project(p['name'])
        if current_p is None:
            return True

        if current_p['ended_at'] != p['ended_at']:
            return True
        else:
            return False


if __name__ == '__main__':
    logger = log_util.get_logger("astro")
    logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler(sys.stdout)
    log_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)

    parser = argparse.ArgumentParser(
        description='Delete Astro internal indexes and topics + user data indexes.')
    parser.add_argument('group_id', help='the astro group id')
    parser.add_argument(
        '--data',
        help='delete user data indexes (default: false)',
        action='store_true')
    args = parser.parse_args()
    db_config = cf_util.get_db_credentials()
    mb_config = cf_util.get_mb_credentials()
    os.environ[constants.ASTRO_DB_ENV_VAR] = json.dumps(db_config)
    os.environ[constants.ASTRO_MB_ENV_VAR] = json.dumps(mb_config)
    astro = AstroUtil(db_config, mb_config)
    astro.clean(args.group_id, args.data)
