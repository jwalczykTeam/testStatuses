'''
Created on Sep 1, 2016

@author: alberto
'''


from doi.util import log_util
from doi.util import time_util
import uuid


logger = log_util.get_logger("astro.util.db")


class Resource(object):
    def __init__(self):
        super(Resource, self).__init__()

    def deserialize(self, input_):
        self.type = input_['type']
        self.id = input_['id']
        self.url = input_['url']

    def serialize(self, output):
        output['type'] = self.type
        output['id'] = self.id
        output['url'] = self.url


class DatabaseException(Exception):
    def __init__(self, message):
        super(DatabaseException, self).__init__(message)


class DatabaseClient(object):
    def close(self):
        raise NotImplementedError("close")

    def create_resource(self, db_name, resource_type, resource,
                        resource_id=None, resource_created_at=None):
        new_resource = resource.copy()
        new_resource['type'] = resource_type
        new_resource['id'] = resource_id \
            if resource_id is not None else str(uuid.uuid1())
        new_resource['created_at'] = resource_created_at \
            if resource_created_at is not None else time_util.get_current_utc_millis()
        new_resource['last_updated_at'] = new_resource['created_at']
        self.insert(db_name, resource_type, new_resource['id'], new_resource)
        return new_resource

    def get_resource(self, db_name, resource_type, resource_id):
        return self.get(db_name, resource_type, resource_id)

    def list_resources(self,
                       db_name,
                       resource_type,
                       key_values=None,
                       size=None):
        return self.find(db_name, resource_type, key_values, size)

    def list_metric_resources(self,
                              metrics_index=None,
                              size=None,
                              doc_type=None,
                              key_values=None,
                              sort_key=None,
                              sort_order=None):


        return self.find(metrics_index, doc_type, key_values, size, sort_key=sort_key, sort_order=sort_order)

    def update_resource(self, db_name, resource):
        new_resource = resource.copy()
        new_resource['last_updated_at'] = time_util.get_current_utc_millis()
        self.insert(db_name,
                    new_resource['type'],
                    new_resource['id'],
                    new_resource)
        return new_resource

    def delete_resource(self, db_name, resource_type, resource_id):
        return self.delete(db_name, resource_type, resource_id)

    def list_database_names(self):
        raise NotImplementedError("list_database_names")

    def database_exists(self, db_name):
        raise NotImplementedError("database_exists")

    def create_database(self, db_name, db_config=None):
        raise NotImplementedError("create_database")

    def delete_database(self, db_name):
        raise NotImplementedError("delete_database")

    def insert(self, db_name, doc_type, doc_id, doc):
        raise NotImplementedError("insert")

    def get_bulk_inserter(self, db_name, doc_type, id_builder):
        raise NotImplementedError("get_bulk_inserter")

    def get_bulk_updater(self, db_name, doc_type, id_builder):
        raise NotImplementedError("get_bulk_updater")

    def get(self, db_name, doc_type, doc_id):
        raise NotImplementedError("get")

    def delete(self, db_name, doc_type, doc_id):
        raise NotImplementedError("delete")

    def find(self, db_name, doc_type, key_values=None, size=None, sort_key=None, sort_order='asc'):
        raise NotImplementedError("find")

    def exists(self, db_name, doc_type, key_values):
        raise NotImplementedError("exists")

    def count(self, db_name, doc_type, key_values, key_value_before_time):
        raise NotImplementedError("count")

    def sum(self, db_name, doc_type, key_values, key_value_before_time, *field_names):
        raise NotImplementedError("sum")

    def search(self, db_name, *args, **kwargs):
        raise NotImplementedError("search")

    # Advanced queries
    def query(self, db_name, doc_type,
              where=None, from_time=None, to_time=None,
              sort_by=None, sort_order='asc',
              start=0, size=10, fields=None, min_probability=None):
        raise NotImplementedError("query")

    def get_count_distribution(self, db_name, doc_type,
                               group_by_field,
                               from_date=None, to_date=None,
                               size=None, min_probability=None):
        raise NotImplementedError("get_count_distribution")

    def get_count_distribution_ex(self, db_name, aggregations, doc_type=None,
                               from_date=None, to_date=None,
                               query=None, min_probability=None):
        raise NotImplementedError("get_count_distribution_ex")

    def get_count_distribution_ex_freeform(self, db_name, query, doc_type=None):
        raise NotImplementedError("get_count_distribution_ex_freeform")

    def get_count_time_series_ex(self, db_name, aggregations, doc_type=None,
                                 interval=None, from_date=None, to_date=None,
                                 time_format=None, query=None):
        raise NotImplementedError("get_count_time_series_ex")

    def get_count_time_series_ex_freeform(self, db_name, query, doc_type=None):
        raise NotImplementedError("get_count_time_series_ex_freeform")

    def get_count_freeform(self, db_name, query, doc_type=None):
        raise NotImplementedError("get_count_freeform")

    def get_sum_distribution(self, db_name, doc_type,
                             sum_by_field, group_by_field,
                             from_date=None, to_date=None,
                             size=None, min_probability=None):
        raise NotImplementedError("get_sum_distribution")

    def get_count_time_series(self, db_name, doc_type,
                              group_by_field,
                              interval=None,
                              from_date=None, to_date=None,
                              time_format=None, size=None):
        raise NotImplementedError("get_count_time_series")

    def get_sum_time_series(self, db_name, doc_type,
                            sum_by_field, group_by_field,
                            interval=None,
                            from_date=None, to_date=None,
                            time_format=None, size=None):
        raise NotImplementedError("get_sum_time_series")
