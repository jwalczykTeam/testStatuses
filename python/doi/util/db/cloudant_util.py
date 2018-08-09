'''
Created on Sep 1, 2016

@author: alberto
'''


from doi.util import log_util
from doi.util import time_util
from doi.util.db.common import DatabaseClient, DatabaseException, BulkInserter
from cloudant.client import Cloudant
import threading


'''
Config:

    {
        "cloudant": {
            "username": ...
            "password": ...
            "url": ...
        }
    }
'''


logger = log_util.get_logger("astro.util.db.cloudant")


NAMESPACE = "code_insights_"


class CloudantException(DatabaseException):
    def __init__(self, results):
        self.results = results
        super(CloudantException, self).__init__(
            "Cloudant error: {}".format(results))


class CloudantClient(DatabaseClient):
    RequestTimeout = 60
    MaxHitsSize = 10000

    def __init__(self, config):
        super(CloudantClient, self).__init__()
        logger.info("Initializing cloudant client ...")
        self.client = Cloudant(config['username'], config["password"],
                               url=config['url'])
        self.client.connect()

    def database_exists(self, db_name):
        full_db_name = NAMESPACE + db_name
        all_db_names = self.client.all_dbs()
        return full_db_name in all_db_names

    def create_database(self, db_name, db_config=None):
        full_db_name = NAMESPACE + db_name
        self.client.create_database(full_db_name)

    def delete_database(self, db_name):
        full_db_name = NAMESPACE + db_name
        self.client.delete_database(full_db_name)

    def insert(self, db_name, doc_type, doc_id, doc):
        full_db_name = NAMESPACE + db_name
        db = self.client[full_db_name]
        doc["_id"] = doc_id
        doc["doc_type"] = doc_type
        db.create_document(doc)

    def get_bulk_inserter(self, db_name, doc_type, id_builder):
        full_db_name = NAMESPACE + db_name
        return CloudantBulkInserter(self,
                                    full_db_name, doc_type, id_builder)

    def get(self, db_name, doc_type, doc_id):
        raise NotImplementedError("get")

    def delete(self, db_name, doc_type, doc_id):
        raise NotImplementedError("delete")

    def find(self, db_name, doc_type, key_values=None, size=None):
        raise NotImplementedError("find")

    def exists(self, db_name, doc_type, key_values):
        raise NotImplementedError("exists")

    def count(self, db_name, doc_type, key_values, key_value_before_time):
        raise NotImplementedError("count")

    def sum(self, db_name, doc_type, key_values, key_value_before_time,
            *field_names):
        raise NotImplementedError("sum")

    #
    # Advanced Queries
    #

    def get_count_distribution(self, db_name, doc_type,
                               group_by_field,
                               from_date=None, to_date=None,
                               size=None, min_probability=None):
        raise NotImplementedError("get_count_distribution")

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

    def _resolve_interval(self, interval):
        if interval is None:
            interval = "day"

        return interval

    def _resolve_from_date(self, date):
        if date is None:
            # No lower precision than a day (first 10 chars of an iso8601 string
            date = time_util.utc_millis_to_iso8601_datetime(0)[0: 10]

        return date

    def _resolve_to_date(self, date):
        if date is None:
            now = time_util.get_current_utc_millis()
            # No lower precision than a day (first 10 chars of an iso8601 string
            date = time_util.utc_millis_to_iso8601_datetime(now)[0: 10]

        return date

    def _resolve_size(self, size):
        if size is None:
            size = 10

        return size


class CloudantBulkInserter(BulkInserter):
    MAX_DOCS = 100

    def __init__(self, db_client, db_name, doc_type, id_builder=None):
        self.db_client = db_client
        self.db = self.db_client.client[db_name]
        self.doc_type = doc_type
        self.id_builder = id_builder
        self.docs = []
        self.db_lock = threading.Lock()

    def insert(self, docs):
        with self.db_lock:
            for doc in docs:
                if len(self.docs) == CloudantBulkInserter.MAX_DOCS:
                    self.db.bulk_docs(self.docs)
                    self.docs = []
                else:
                    doc["_id"] = self.id_builder(doc)
                    doc["doc_type"] = self.doc_type
                    self.docs.append(doc)

            return docs

    def flush(self):
        with self.db_lock:
            if len(self.docs) != 0:
                self.db.bulk_docs(self.docs)
                self.docs = []

    def close(self):
        self.flush()
