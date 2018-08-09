'''
Created on Sep 25, 2016

@author: alberto
'''

from doi import constants
from doi.common import TaskMethod
from doi.util.db import db_util
from doi.util.kibana_util import KibanaProcessor, import_kibana_objects, DRAProcessor, import_dra_objects
from doi.util import log_util
import json
import os


logger = log_util.get_logger("astro.jobs.kibana")


def _get_templates_path():
    from doi.jobs.kibana import templates
    return os.path.dirname(templates.__file__)


def _get_elasticsearch_config():
    db_config = json.loads(os.environ[constants.ASTRO_DB_ENV_VAR])  
    return db_util.Database.factory(db_config)


def process_project_dashboards(params, progress_callback, stop_event):
    process_dashboards(params.job_name, params.method)


def process_dashboards(index_name, method):
    logger.info("Started processing Kibana dashboard objects")

    templates_dir = _get_templates_path()
    es_client = _get_elasticsearch_config()
    kibana_objs = import_kibana_objects(templates_dir)
    kibana_proc = KibanaProcessor(es_client, index_name = index_name)
    dra_proc = DRAProcessor(es_client, index_name = index_name)
    dra_objs = import_dra_objects(templates_dir)

    if method in [TaskMethod.DELETE, TaskMethod.UPDATE]:
        logger.info("Removing Kibana objects for index %s ...", index_name)    
        kibana_proc.remove_objects(kibana_objs)
        logger.info("Removing DevRisk Analysis plugin objects for index %s ...", index_name)
        dra_proc.remove_objects(dra_objs)

    if method in [TaskMethod.CREATE, TaskMethod.UPDATE]:
        logger.info("Adding Kibana objects for index %s ...", index_name)    
        kibana_proc.add_objects(kibana_objs)
        logger.info("Adding DevRisk Analysis plugin objects for index %s ...", index_name)
        dra_proc.add_objects(dra_objs)
    
    logger.info("Done processing Kibana dashboard objects")

