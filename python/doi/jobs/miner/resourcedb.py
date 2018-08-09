'''
Created on Jul 15, 2016

@author: alberto
'''


from doi import constants
from doi.jobs.miner import fields as f
from doi.jobs.miner import pipeline
from doi.util import log_util
from doi.util.db import db_util
import hashlib
import json
import os


logger = log_util.get_logger("astro.jobs.miner.resourcedb")


class ResourceType(object):

    def __init__(self, name, id_field_names):
        self.name = name
        self.id_field_names = id_field_names

    def build_id(self, resource):
        resource_sha = hashlib.sha1()
        for field_name in self.id_field_names:
            value = resource[field_name]
            if isinstance(value, unicode):
                value = value.encode('utf-8')
            resource_sha.update(value)

        return resource_sha.hexdigest()


class ResourceDBException(Exception):

    def __init__(self, message):
        super(ResourceDBException, self).__init__(message)


class ResourceDB(object):
    BRANCH_TYPE = ResourceType('branch', [f.BRANCH_URI])
    COMMIT_TYPE = ResourceType('commit', [f.COMMIT_URI])
    COMMITTED_FILE_TYPE = ResourceType(
        'committed_file', [f.COMMIT_URI, f.FILE_ID])
    ISSUE_TYPE = ResourceType('issue', [f.ISSUE_URI])
    PULL_TYPE = ResourceType('pull', [f.PULL_URI])
    USER_TYPE = ResourceType('user', [f.USER_URI])
    BUILD_TYPE = ResourceType('build', [f.BUILD_URI])
    REPO_MAP_TYPE = ResourceType('index_mappings', [f.GIT_URI])

    def __init__(self, db_name, creds=None):
        self.db_name = db_name

        # Populate resource types
        self.resource_types = {}
        self.add_resource_type(ResourceDB.BRANCH_TYPE)
        self.add_resource_type(ResourceDB.COMMIT_TYPE)
        self.add_resource_type(ResourceDB.COMMITTED_FILE_TYPE)
        self.add_resource_type(ResourceDB.ISSUE_TYPE)
        self.add_resource_type(ResourceDB.ISSUE_TYPE)
        self.add_resource_type(ResourceDB.USER_TYPE)
        self.add_resource_type(ResourceDB.BUILD_TYPE)
        self.add_resource_type(ResourceDB.REPO_MAP_TYPE)

        # Initialize database
        if creds is None:
            db_config = json.loads(os.environ[constants.ASTRO_DB_ENV_VAR])
        else:
            db_config = creds
        self.db = db_util.Database.factory(db_config)

        db_config_path = self._get_db_config_path()
        with open(db_config_path, 'r') as f:
            self.db_config = json.load(f)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.close()
        except:
            logger.exception("Unexpected exception while closing resource db.")

    def close(self):
        if self.db.database_exists(self.db_name):
            logger.info("Flushing resource db index ...")
            self.db.flush(self.db_name)
        # There is no elasticsearch client 'close' method

    def add_resource_type(self, resource_type):
        self.resource_types[resource_type.name] = resource_type

    def get_resource_type(self, name):
        return self.resource_types[name]

    def create_database(self):
        if self.db.database_exists(self.db_name):
            self.db.delete_database(self.db_name)
        self.db.create_database(self.db_name, self.db_config)

    def delete_database(self):
        self.db.delete_database(self.db_name)

    def insert(self, resource_type_name, resource_id, body):
        self.db.insert(self.db_name, resource_type_name, resource_id, body)

    def get_bulk_writer(self, resource_type_name, action):
        resource_type = self.get_resource_type(resource_type_name)
        return self.db.get_bulk_writer(
            self.db_name, resource_type_name,
            action, resource_type.build_id)

    def get_bulk_reader(self, resource_type_name, key_values, exclude_fields, size):
        return self.db.get_bulk_reader(self.db_name, resource_type_name, key_values, exclude_fields, size)

    def exists(self, resource_type_name, key_values):
        return self.db.exists(self.db_name, resource_type_name, key_values)

    def find(self, resource_type_name, key_values=None, size=None, sort_key=None, sort_order='asc'):
        return self.db.find(self.db_name, resource_type_name, key_values, size, sort_key, sort_order)

    def count_query(self, resource_type_name, key_values, key_value_before_time):
        return self.db.count_query(self.db_name, resource_type_name, key_values, key_value_before_time)

    def count(self, resource_type_name, key_values, key_value_before_time):
        return self.db.count(self.db_name, resource_type_name, key_values, key_value_before_time)

    def sum(self, resource_type_name, key_values, key_value_before_time, *field_names):
        return self.db.sum(self.db_name, resource_type_name, key_values, key_value_before_time, *field_names)

    def search(self, *args, **kwargs):
        return self.db.search(self.db_name, *args, **kwargs)

#
#    Internal methods
#

    def _get_db_config_path(self):
        from doi.jobs.miner import model
        model_path = os.path.dirname(model.__file__)
        db_config_path = os.path.join(model_path, "mapping.json")
        return db_config_path


class ResourceDBBulkReader(pipeline.Reader):

    def __init__(self, resource_db, resource_type_name, key_values=None, exclude_fields=None, size=None):
        self.reader = resource_db.get_bulk_reader(
            resource_type_name, key_values, exclude_fields, size)

    def iter(self):
        return self.reader.iter()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self.reader.close()


class ResourceDBBulkWriter(pipeline.Writer):

    def __init__(self, resource_db, resource_type_name, action):
        super(ResourceDBBulkWriter, self).__init__()
        self.writer = resource_db.get_bulk_writer(resource_type_name, action)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def write(self, resource):
        self.writer.write(resource)

    def close(self):
        logger.info("Closing db bulk writer ...")
        self.writer.close()


class ResourceDBBulkInserter(ResourceDBBulkWriter):

    def __init__(self, resource_db, resource_type_name):
        super(ResourceDBBulkInserter, self).__init__(
            resource_db, resource_type_name, "index")


class ResourceDBBulkUpdater(ResourceDBBulkWriter):

    def __init__(self, resource_db, resource_type_name):
        super(ResourceDBBulkUpdater, self).__init__(
            resource_db,
            resource_type_name,
            "update")
