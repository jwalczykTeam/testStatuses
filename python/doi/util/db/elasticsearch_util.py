'''
Created on Sep 1, 2016

@author: alberto
'''

from doi.util.db.common import DatabaseClient, DatabaseException
from doi.util import log_util
from doi.util import time_util
from doi.util.log_util import mask_sensitive_info
import elasticsearch
import json
import re
import os
import doi
import threading
from StringIO import StringIO
import certifi

'''
Config:

    {
        "elasticsearch": {
            "hosts": [ ... ]
        }
    }

    Or

    {
        "compose_elasticsearch": {
            "public_hostname": ...
            "username": ...
            "password: ...
        }
    }
'''


logger = log_util.get_logger("astro.util.db.elasticsearch")


class ElasticsearchIndexException(DatabaseException):

    def __init__(self, results):
        self.results = results
        super(ElasticsearchIndexException, self).__init__(
            "Error indexing: {}".format(results))


class ElasticsearchClient(DatabaseClient):
    REQUEST_TIMEOUT = 60
    MAX_HIT_SIZE = 10000

    def __init__(self, config, client=None):
        super(ElasticsearchClient, self).__init__()
        self.config = config
        self.client = client
        if client is None:
            self.open()

    def open(self):
        logger.debug("Initializing elasticsearch client: %s ...",
                     mask_sensitive_info(self.config))

        es_config = {
            'send_get_body_as': 'POST',
            'sniff_on_start': False,
            'sniff_on_connection_fail': False,
            'timeout': 60,
            'retry_on_timeout': True,
            'max_retries': 10
        }

        es_config.update(self.config)

        logger.debug("Elasticsearch config: %s",
                     mask_sensitive_info(es_config))
        hosts = es_config.pop('hosts')

        self.client = elasticsearch.Elasticsearch(hosts, **es_config)

        self.client.ping()
        logger.debug("Elasticsearch client initialized.")

    def get_client(self):
        return self.client

    def close(self):
        logger.debug("Closing elasticsearch client ...")
        self.client = None
        logger.debug("Elasticsearch client closed.")

    def list_database_names(self):
        return self.client.indices.get("*").keys()

    def database_exists(self, db_name):
        return self.client.indices.exists(db_name)

    def create_database(self, db_name, db_config=None):
        logger.debug("Creating index '%s' ...", db_name)
        try:
            self.client.indices.create(
                db_name, db_config, update_all_types=True)
        except Exception as e:
            logger.error(traceback.print_exc())
            raise ValueError(
                "create_database(): Error creating index. "
                "Result: {}".format(e))

    def delete_database(self, db_name):
        logger.debug("Deleting index '%s' ...", db_name)
        if self.database_exists(db_name):
            self.client.indices.delete(db_name)

    def count_query(
            self,
            db_name,
            doc_type,
            key_values,
            key_value_before_time,
            use_raw_field=False,
            quote_value=True):

        q = self._build_query_string(
            key_values, key_value_before_time, use_raw_field, quote_value)

        body = {
            "query": {
                "query_string": {
                    "query": q,
                    "analyze_wildcard": True
                }
            }
        }

        result = self.client.count(index=db_name, doc_type=doc_type, body=body)

        return result['count']

    def _build_tokenized_string_query(self, list_of_key_values):

        return {
            'query': {
                'bool': {
                    'must': [{'term': {key: value}}
                             for _ in list_of_key_values for key, value in
                             _.iteritems()]
                }
            }
        }

    def _build_untokenized_string_query(self, list_of_key_values):

        return {
            'query': {
                'bool': {
                    'must': {'term': {key: v}}
                    for _ in list_of_key_values for key, value in
                    _.iteritems() for v in value.split("/")
                }
            }
        }

    def query_string_match(self, key, value):
        return {
            'query': {
                'query_string': {
                    'analyze_wildcard': True,
                    'query': '{}:"{}"'.format(
                        key, value).replace('"', '\"')
                }
            }
        }

    def get_latest_record(
            self,
            db_name,
            doc_type,
            list_of_key_values,
            timestamp_key='timestamp',
            query_string=False):
        """
        db_name             index
        doc_type            doc_type
        list_of_key_values  list of dict key value pairs to match token to
        timestamp_key       timestamp_key
        """
        if len(list_of_key_values) > 0:
            if query_string is False:
                body = self._build_tokenized_string_query(list_of_key_values)
            else:
                # If query_string is true, we'll receive a dictionary
                # with only one pair, instead of a list of dics
                key, value = list_of_key_values.items()[0]
                body = self.query_string_match(key, value)
        else:
            body = {}

        body['sort'] = [
            {timestamp_key: {'order': 'desc', 'unmapped_type': 'string'}}]
        body['size'] = 1

        result = self.client.search(
            index=db_name,
            doc_type=doc_type,
            body=body)[
            'hits']['hits']

        if len(result) == 0:
            return None
        else:
            return result[0]['_source']

    def get_latest_record_with_keys(
            self,
            db_name,
            doc_type,
            source_mapping,
            keys=[],
            timestamp_key='timestamp'):

        (key, value), = source_mapping.iteritems()

        body = {
            'query': {
                'query_string': {
                    'analyze_wildcard': True,
                    'query': '{}:"{}" {}'.format(
                        key, value, "".join(
                            "AND _exists_:{} ".format(k) for k in keys)
                    ).replace('"', '\"')
                }
            }
        }

        body['sort'] = [
            {timestamp_key: {'order': 'desc', 'unmapped_type': 'string'}}]
        body['size'] = 1

        result = self.client.search(
            index=db_name,
            doc_type=doc_type,
            body=body)[
            'hits']['hits']

        if len(result) == 0:
            return None
        else:
            return result[0]['_source']

    def match_tokenized_string(
            self,
            db_name,
            doc_type,
            list_of_key_values):
        """
        db_name             index
        doc_type            doc_type
        list_of_key_values  list of dict key value pairs to match token to
        timestamp_key       timestamp_key
        """

        body = self._build_tokenized_string_query(list_of_key_values)

        result = self.client.search(
            index=db_name, doc_type=doc_type, body=body)[
            'hits']['hits']

        return [_['_source'] for _ in result]

    def insert(self, db_name, doc_type, doc_id, doc):
        result = self.client.index(
            index=db_name,
            doc_type=doc_type,
            body=doc,
            id=doc_id,
            refresh=True,
            request_timeout=ElasticsearchClient.REQUEST_TIMEOUT)
        if 'error' in result:
            error = result['error']
            excres = {
                "total": 1,
                "in_error": 1,
                "error": error
            }
            raise ElasticsearchIndexException(excres)

    def get_bulk_writer(self, db_name, doc_type, action, id_builder):
        return ElasticsearchBulkWriter(
            self, db_name, doc_type, action, id_builder)

    def get_bulk_reader(
            self,
            db_name,
            doc_type,
            key_values=None,
            exclude_fields=None,
            size=None):
        return ElasticsearchBulkReader(
            self, db_name, doc_type, key_values, exclude_fields, size)

    def get(self, db_name, doc_type, doc_id):
        try:
            result = self.client.get(
                index=db_name,
                doc_type=doc_type,
                id=doc_id)
            return result['_source']
        except elasticsearch.NotFoundError:
            return None

    def delete(self, db_name, doc_type, doc_id):
        try:
            self.client.delete(
                index=db_name,
                doc_type=doc_type,
                id=doc_id)
            return True
        except elasticsearch.NotFoundError:
            return False

    def delete_by_query(
            self,
            db_name,
            doc_type=None,
            where=None,
            from_time=None,
            to_time=None):
        '''
        Delete all docs of the `doc_type` matching the `where` in the interval
        `from_time`, `to_time`.
        Default `doc_type` means all doc types;
        Default `where` means all docs; otherwise
        set a valid Lucene query string
        Default `from_time` means the epoch
        Default `to_time` means now

        So, by default all the docs in the `db_name` will  be deleted!
        '''
        from_time = time_util.resolve_from_date(from_time)
        to_time = time_util.resolve_to_date(to_time)

        # Get only the _id of the docs matching the query: fields: []
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": from_time,
                                    "lte": to_time
                                }
                            }
                        }
                    ]
                }
            },
            "fields": []
        }

        if where:
            query['query']['bool']['must'].append(
                {
                    "query_string": {
                        "query": where
                    }
                })

        logger.info("Deleting docs: index='%s', doc_type='%s', "
                    "where='%s', from_date='%s', to_date='%s'",
                    db_name, doc_type, where, from_time, to_time)

        size = 5000
        deleted = 0
        page = self.client.search(
            db_name,
            doc_type=doc_type,
            scroll='1m',
            body=query,
            size=size)

        scroll_id = page['_scroll_id']
        result_count = page['hits']['total']

        while result_count > 0:
            buff = StringIO()

            page = self.client.scroll(
                scroll_id=scroll_id,
                scroll='1m')

            scroll_id = page['_scroll_id']
            results = page['hits']['hits']
            result_count = len(results)

            if result_count > 0:
                for result in results:
                    option = {"delete": {"_id": result["_id"]}}
                    json.dump(option, buff)
                    buff.write('\n')

                logger.debug("Bulk deleting %d docs ...", result_count)
                self._bulk(db_name, doc_type, buff.getvalue())
                buff.close()
                deleted += result_count

        logger.info("%d docs deleted.", deleted)
        return {"deleted": deleted}

    def find(
            self,
            db_name,
            doc_type,
            key_values=None,
            size=None,
            sort_key=None,
            sort_order='asc'):
        return self._find(
            db_name, doc_type, key_values, size, sort_key, sort_order,
            use_raw_field=False,
            quote_value=True)

    def exists(self, db_name, doc_type, key_values):
        '''
        Returns a boolean indicating whether or not given document exists
        '''
        results = self.find(db_name, doc_type, key_values)
        return len(results) != 0

    def flush(self, db_name):
        self.client.indices.flush(db_name)

    def search(self, db_name, *args, **kwargs):
        '''
        Provides direct access to elasticsearch search method
        '''

        return self.client.search(index=db_name, *args, **kwargs)

#
#   Protected methods
#
    def _bulk(self, db_name, doc_type, body):
        result = self.client.bulk(
            index=db_name,
            doc_type=doc_type,
            body=body,
            refresh=True,
            request_timeout=ElasticsearchClient.REQUEST_TIMEOUT)
        if result['errors'] is True:
            # The errors array can be huge, take only the first element
            errors = [item for item in result.get('items', [])
                      if 'index' in item and 'error' in item['index']]
            excres = {
                "total": len(result["items"]),
                "in_error": len(errors),
                "error": result if len(errors) == 0 else errors[0]
            }
            raise ElasticsearchIndexException(excres)

    def _scroll(self, scroll_id, scroll):
        '''
        Provides direct access to elasticsearch scroll method
        '''
        return self.client.scroll(scroll_id=scroll_id, scroll=scroll)

    def _clear_scroll(self, scroll_id):
        '''
        Provides direct access to elasticsearch clear_scroll method
        '''
        return self.client.clear_scroll(scroll_id=scroll_id, ignore=(404, ))

#
#   Higher level methods
#

    def count(self, db_name, doc_type, key_values, key_value_before_time):
        '''
        Count the documents matching the specified type and key/value pairs
        '''

        return self._count(db_name, doc_type,
                           key_values, key_value_before_time,
                           use_raw_field=False, quote_value=True)

    def sum(self, db_name, doc_type, key_values, key_value_before_time, *field_names):
        '''
        Sum the values of the specified field for all documents matching the specified type and key/value pairs
        '''

        return self._sum(db_name, doc_type,
                         key_values, list(field_names), key_value_before_time,
                         use_raw_field=False, quote_value=True)

    def query(self, db_name, doc_type,
              where=None, from_time=None, to_time=None,
              sort_by=None, sort_order='asc',
              start=0, size=10, fields=None, min_probability=None):
        '''
        where is expected to be a well-formed Lucene query on
        the fields of the given db_name / doc_type (index / type)
        '''

        logger.info("Querying docs: index='%s', doc_type='%s', "
                    "where='%s', from_date='%s', to_date='%s', "
                    "sort_by='%s', sort_order='%s', "
                    "start=%s, size=%s, min_probability=%s",
                    db_name, doc_type, where, from_time, to_time,
                    sort_by, sort_order, start, size, min_probability)

        from_time = time_util.resolve_from_date(from_time)
        to_time = time_util.resolve_to_date(to_time)

        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": from_time,
                                    "lte": to_time
                                }
                            }
                        }
                    ]
                }
            },
            "size": size,
            "from": start
        }

        if doc_type == 'commit_defect_prediction':
            if min_probability:
                query['query']['bool']['filter'] = [{
                    "range": {
                        "probability": {
                            "gte": min_probability,
                            "lte": "1"
                        }
                    }
                  }]

        if where:
            query['query']['bool']['must'].append(
                {
                    "query_string": {
                        "query": where
                    }
                })

        if sort_by:
            query["sort"] = [{
                sort_by: {
                    "order": sort_order
                }
            }]

        obj = '_source'
        if fields:
            query['fields'] = re.split('\s*,\s*', fields.strip())
            obj = 'fields'

        logger.debug('Submitting function query %s', query)
        result = self.client.search(
            index=db_name, doc_type=doc_type, body=query)
        return [x[obj] for x in result['hits']['hits']]

    def get_count_distribution(self, db_name, doc_type,
                               group_by_field,
                               from_date=None, to_date=None,
                               size=None, min_probability=None):
        logger.info("Getting count distribution: index='%s', doc_type='%s', "
                    "group_by_field='%s', from_date='%s', to_date='%s', size=%s",
                    db_name, doc_type, group_by_field, from_date, to_date, size)
        query = self._get_count_distribution_query(
            doc_type, group_by_field,
            from_date, to_date, size, min_probability)
        raw_results = self.client.search(db_name, body=query)
        results = self._map_count_distribution_results(raw_results)
        return results

    def get_count_distribution_ex(self, db_name, aggregations, doc_type=None,
                                  from_date=None, to_date=None,
                                  query=None, min_probability=None):
        fullquery = self._get_count_distribution_query_ex(
            aggregations, doc_type, from_date, to_date, query, min_probability)
        raw_results = self.client.search(
            db_name, doc_type=doc_type, body=fullquery)
        return raw_results

    def get_count_distribution_ex_freeform(self, db_name, query, doc_type=None):
        raw_results = self.client.search(
            db_name, doc_type=doc_type, body=query)
        return raw_results

    def get_count_time_series_ex(self,
                                 db_name,
                                 aggregations,
                                 doc_type,
                                 interval=None,
                                 from_date=None,
                                 to_date=None,
                                 time_format=None,
                                 query=None):
        logger.info("Getting count time series: index='%s', aggregations='%s', doc_type='%s', "
                    "group_by_field='%s', interval='%s', "
                    "from_date='%s', to_date='%s', time_format='%s', size=%s",
                    db_name, aggregations, doc_type, interval,
                    from_date, to_date, time_format, query)
        fullquery = self._get_count_time_series_query_ex(
            aggregations, doc_type, interval, from_date, to_date, time_format, query)
        raw_results = self.client.search(
            db_name, doc_type=doc_type, body=fullquery)
        return raw_results

    def get_count_time_series_ex_freeform(self,
                                 db_name,
                                 query,
                                 doc_type):
        raw_results = self.client.search(
            db_name, doc_type=doc_type, body=query)
        return raw_results

    def get_count_freeform(self,
                                 db_name,
                                 query,
                                 doc_type):
        raw_results = self.client.search(
            db_name, body=query)
        return raw_results

    def get_sum_distribution(self, db_name, doc_type,
                             sum_by_field, group_by_field,
                             from_date=None, to_date=None,
                             size=None, min_probability=None):

        query = self._get_sum_distribution_query(
            doc_type, sum_by_field, group_by_field,
            from_date, to_date, size, min_probability)
        raw_results = self.client.search(db_name, body=query)
        results = self._map_sum_distribution_results(raw_results)
        return results

    def get_count_time_series(self, db_name, doc_type,
                              group_by_field,
                              interval=None,
                              from_date=None, to_date=None,
                              time_format=None, size=None):
        logger.info("Getting count time series: index='%s', doc_type='%s', "
                    "group_by_field='%s', interval='%s', "
                    "from_date='%s', to_date='%s', time_format='%s', size=%s",
                    db_name, doc_type, group_by_field, interval,
                    from_date, to_date, time_format, size)
        query = self._get_count_time_series_query(
            doc_type, group_by_field, interval,
            from_date, to_date, time_format, size)
        raw_results = self.client.search(db_name, body=query)
        results = self._map_count_time_series_results(raw_results, time_format)
        return results

    def get_sum_time_series(self, db_name, doc_type,
                            sum_by_field, group_by_field,
                            interval=None,
                            from_date=None, to_date=None,
                            time_format=None, size=None):
        logger.info("Getting sum time series: index='%s', doc_type='%s', "
                    "sum_by_field='%s', group_by_field='%s', interval='%s', "
                    "from_date='%s', to_date='%s', time_format='%s', size=%s",
                    db_name, doc_type, sum_by_field, group_by_field, interval,
                    from_date, to_date, time_format, size)
        query = self._get_sum_time_series_query(
            doc_type, sum_by_field, group_by_field, interval,
            from_date, to_date, time_format, size)
        raw_results = self.client.search(db_name, body=query)
        results = self._map_sum_time_series_results(raw_results, time_format)
        return results

    def _find(self, db_name, doc_type, key_values, size,
              sort_key=None, sort_order='asc',
              use_raw_field=False, quote_value=True):
        '''
        Get all the docs of type `doc_type` matching the query `field_name: field_value` for all keys
        in the `key_values` dict.

        Args:
          doc_type(str): the doc type to search for
          key_values(dict): a dict of `field_name: field_value` to search for
          use_raw_field(Optional[bool]): whether to use the not analyzed (raw) version of the field or not. Defaults to not raw
          quote_value(Optional[bool]): whether to search for exact match of `field-value` or not. Defaults to exact match
          size(Optional[int]): the maximum number of matches to return. Defaults to 10

        Returns:
          docs: the list of docs matching the query

        Raises:
          Exception: Any other Elasticsearch library exception not catched: e.g. network errors
        '''

        q = ElasticsearchClient._build_query_string(
            key_values, None, use_raw_field, quote_value)
        if size is None:
            size = ElasticsearchClient.MAX_HIT_SIZE

        query = {
            "query": {
                "query_string": {
                    "query": q
                }
            },
            "size": size
        }

        if sort_key:
            query["sort"] = [{
                sort_key: {
                    "order": sort_order
                }
            }]

       # logger.info("_find query %s, %s, %s",
       #            db_name, doc_type, query)
        result = self.client.search(
            index=db_name, doc_type=doc_type, body=query)
        return [x['_source'] for x in result['hits']['hits']]

    def _count(self, db_name, doc_type, key_values,
               key_value_before_time=None, use_raw_field=False, quote_value=True):
        '''
        Count all the docs of type `doc_type` matching the query `field_name: field_value` for all keys
        in the `key_values` dict and whose value of key_value_before_time field is less than the given
        timestamp set in key_value_before_time value.

        Args:
          doc_type(str): the doc type to search for
          key_values(dict): a dict of `field_name: field_value` to search for
          key_value_before_time(dict): timestamp field name and the value to check
          use_raw_field(Optional[bool]): whether to use the not analyzed (raw) version of the field or not. Defaults to not raw
          quote_value(Optional[bool]): whether to search for exact match of `field-value` or not. Defaults to exact match

        Returns:
          num: the number of docs matching the query

        Raises:
          Exception: Any other Elasticsearch library exception not catched: e.g. network errors
        '''

        q = ElasticsearchClient._build_query_string(
            key_values, key_value_before_time, use_raw_field, quote_value)

        # pprint.pprint(q)
        result = self.client.count(index=db_name, doc_type=doc_type, q=q)
        return result['count']

    def _sum(self, db_name, doc_type,
             key_values, numeric_field_names,
             key_value_before_time=None, use_raw_field=False, quote_value=True):
        '''
        Perform sum aggregation over the numeric_field_name of all the docs of type `doc_type` matching
        the query `field_name: field_value` for all keys in the `key_values` dict and whose value of
        key_value_before_time field is less than the given timestamp set in key_value_before_time value.

        Args:
          doc_type(str): the doc type to search for
          key_values(dict): a dict of `field_name: field_value` to search for
          numeric_field_names(list): a list of numeric fields to aggregate for summarization
          key_value_before_time(dict): timestamp field name and the value to check
          use_raw_field(Optional[bool]): whether to use the not analyzed (raw) version of the field or not. Defaults to not raw
          quote_value(Optional[bool]): whether to search for exact match of `field-value` or not. Defaults to exact match

        Returns:
          num: the accumulated sum of numeric_field values

        Raises:
          Exception: Any other Elasticsearch library exception not catched: e.g. network errors
        '''

        q = ElasticsearchClient._build_query_string(
            key_values, key_value_before_time, use_raw_field, quote_value)

        # Build the aggregations dict
        aggs = {}
        for f in numeric_field_names:
            aggs[f] = {
                "sum": {
                    "field": f
                }
            }
        body = {
            "size": 0,
            "aggs": aggs
        }

        result = self.client.search(
            index=db_name, doc_type=doc_type, q=q, body=body)
        aggs = result['aggregations']
        counters = ()
        for f in numeric_field_names:
            counters += (aggs[f]['value'], )

        return counters

    @staticmethod
    def _build_query_string(key_values, key_value_before_time,
                            use_raw_field, quote_value):
        q = ''
        if key_values:
            for f in key_values.iteritems():
                k = f[0]
                v = f[1]
                if use_raw_field:
                    k += '.raw'
                if quote_value:
                    v = '"%s"' % v

                q += "+%s: %s " % (k, v)

        if key_value_before_time:
            for f in key_value_before_time.iteritems():
                q += "+%s: [0 TO %s} " % (f[0], f[1])

        return q if len(q) != 0 else '*'

    def _get_count_distribution_query(self, doc_type,
                                      group_by_field,
                                      from_date, to_date,
                                      size, min_probability):
        from_date = self._resolve_from_date(from_date)
        to_date = self._resolve_to_date(to_date)
        size = self._resolve_size(size)

        query = {
            "size": 0,
            "query": {
                "query_string": {
                    "query": "+_type: {} +timestamp:[\"{}\" TO \"{}\"]".format(
                        doc_type, from_date, to_date)
                }
            },
            "aggs": {
                "distribution": {
                    "terms": {
                        "field": group_by_field + ".raw",
                        "missing":  "Unassigned",
                        "size": size
                    }
                }
            }
        }

        return query

    def _get_count_distribution_query_ex(self, aggregations, doc_type, from_date, to_date, query, min_probability):
        from_date = self._resolve_from_date(from_date)
        to_date = self._resolve_to_date(to_date)

        from_time = time_util.resolve_from_date(from_date)
        to_time = time_util.resolve_to_date(to_date)

        fullquery = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": from_time,
                                    "lte": to_time
                                }
                            }
                        }
                    ]
                }
            },
            "size": 0,
            "aggs": aggregations
        }

        if min_probability:
            fullquery['query']['bool']['filter'] = [{
                "range": {
                    "probability": {
                        "gte": min_probability,
                        "lt": "1"
                    }
                }
              }]

        if query:
            fullquery['query']['bool']['must'].append(
                {
                    "query_string": {
                        "query": query
                    }
                })

        return fullquery

    def _map_count_distribution_results(self, results):
        return [{"name": x["key"], "value": x["doc_count"]} for x in results['aggregations']["distribution"]["buckets"]]

    def _get_sum_distribution_query(self, doc_type,
                                    sum_by_field, group_by_field,
                                    from_date, to_date,
                                    size, min_probability):
        from_date = self._resolve_from_date(from_date)
        to_date = self._resolve_to_date(to_date)

        query = {
            "size": 0,
            "query": {
                "query_string": {
                    "query": "+_type: {} +timestamp:[\"{}\" TO \"{}\"]".format(
                        doc_type, from_date, to_date)
                }
            },
            "aggs": {
                "distribution": {
                    "aggs": {
                        "field_sum": {
                            "sum": {"field": sum_by_field}
                        }
                    },
                    "terms": {
                        "field": group_by_field + ".raw",
                        "size": size
                    }
                }
            }
        }

        return query

    def _map_sum_distribution_results(self, results):
        out_buckets = []
        for in_bucket in results['aggregations']["distribution"]["buckets"]:
            field_sum = in_bucket["field_sum"]
            out_buckets.append(
                {"name": in_bucket["key"], "value": int(field_sum["value"])})
        return out_buckets

    def _get_count_time_series_query(self, doc_type,
                                     group_by_field,
                                     interval,
                                     from_date, to_date,
                                     time_format, size):
        interval = self._resolve_interval(interval)
        from_date = self._resolve_from_date(from_date)
        to_date = self._resolve_to_date(to_date)
        size = self._resolve_size(size)

        query = {
            "size": 0,
            "query": {
                "query_string": {
                    "query": "+_type: {} +timestamp:[\"{}\" TO \"{}\"]".format(
                        doc_type, from_date, to_date)
                }
            },
            "aggs": {
                "time_series": {
                    "date_histogram": {
                        "field": "timestamp",
                        "interval": interval,
                        "missing": "2000-01-01"
                    },
                    "aggs": {
                        "distribution": {
                            "terms": {
                                "field": group_by_field + ".raw",
                                "size": size
                            }
                        }
                    }
                }
            }
        }

        return query

    def _get_count_time_series_query_ex(self, aggregations, doc_type, interval, from_date, to_date, time_format, query):
        interval = self._resolve_interval(interval)
        from_date = self._resolve_from_date(from_date)
        to_date = self._resolve_to_date(to_date)

        fullquery = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": from_date,
                                    "lte": to_date
                                }
                            }
                        }
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "time_series": {
                    "date_histogram": {
                        "field": "timestamp",
                        "interval": interval,
                        "missing": "2000-01-01"
                    },
                    "aggs": aggregations
                }
            }
        }

        if query:
            fullquery['query']['bool']['must'].append(
                {
                    "query_string": {
                        "query": query
                    }
                })

        return fullquery

    def _map_count_time_series_results(self, results, time_format):
        out_buckets = []
        for in_bucket in results['aggregations']["time_series"]["buckets"]:
            if time_format == time_util.ISO_8601_TIME_FORMAT:
                out_bucket = {"date": in_bucket["key_as_string"]}
            else:
                out_bucket = {"date": in_bucket["key"]}
            distribution = [{"name": x["key"], "value": x["doc_count"]}
                            for x in in_bucket["distribution"]["buckets"]]
            out_bucket["distribution"] = distribution
            out_buckets.append(out_bucket)

        return out_buckets

    def _get_sum_time_series_query(self, doc_type,
                                   sum_by_field, group_by_field,
                                   interval,
                                   from_date, to_date,
                                   time_format, size):
        interval = self._resolve_interval(interval)
        from_date = self._resolve_from_date(from_date)
        to_date = self._resolve_to_date(to_date)
        size = self._resolve_size(size)

        query = {
            "size": 0,
            "query": {
                "query_string": {
                    "query": "+_type: {} +timestamp:[\"{}\" TO \"{}\"]".format(
                        doc_type, from_date, to_date)
                }
            },
            "aggs": {
                "time_series": {
                    "date_histogram": {
                        "field": "timestamp",
                        "interval": interval,
                        "missing": "2000-01-01"
                    },
                    "aggs": {
                        "distribution": {
                            "aggs": {
                                "field_sum": {
                                    "sum": {"field": sum_by_field}
                                }
                            },
                            "terms": {
                                "field": group_by_field + ".raw",
                                "size": size
                            }
                        }
                    }
                }
            }
        }

        return query

    def _map_sum_time_series_results(self, results, time_format):
        out_buckets = []
        for in_bucket in results['aggregations']["time_series"]["buckets"]:
            if time_format == time_util.ISO_8601_TIME_FORMAT:
                out_bucket = {"date": in_bucket["key_as_string"]}
            else:
                out_bucket = {"date": in_bucket["key"]}

            out_distribution_buckets = []
            for distribution_bucket in in_bucket["distribution"]["buckets"]:
                field_sum = distribution_bucket["field_sum"]
                out_distribution_buckets.append(
                    {"name": distribution_bucket["key"], "value": int(field_sum["value"])})
            out_bucket["distribution"] = out_distribution_buckets
            out_buckets.append(out_bucket)

        return out_buckets

    def _resolve_interval(self, interval):
        if interval is None:
            interval = "day"

        return interval

    def _resolve_from_date(self, date):
        if date is None:
            # No lower precision than a day (first 10 chars of an iso8601
            # string
            date = time_util.utc_millis_to_iso8601_datetime(0)[0: 10]

        return date

    def _resolve_to_date(self, date):
        if date is None:
            now = time_util.get_current_utc_millis()
            # No lower precision than a day (first 10 chars of an iso8601
            # string
            date = time_util.utc_millis_to_iso8601_datetime(now)[0: 10]

        return date

    def _resolve_size(self, size):
        if size is None:
            size = 10

        return size


class ElasticsearchBulkReader(object):
    DEFAULT_SIZE = 500

    def __init__(self, db_client, db_name, doc_type, key_values=None, exclude_fields=None, size=None):
        self.db_client = db_client
        self.db_name = db_name
        self.doc_type = doc_type
        self.key_values = key_values
        self.exclude_fields = exclude_fields
        self.size = size if size is not None else ElasticsearchBulkReader.DEFAULT_SIZE
        self.scroll_id = None

    def iter(self):
        logger.info("##############   Reading from Elastic: ...")
        body = self._get_body()
        page = self.db_client.search(
            self.db_name,
            doc_type=self.doc_type,
            body=body,
            size=self.size)

        total = page['hits']['total']
        if total <= self.size:
            results = page['hits']['hits']
            for result in results:
                yield result["_source"]
        else:
            # Use the scroll instead
            page = self.db_client.search(
                self.db_name,
                doc_type=self.doc_type,
                scroll='5m',
                body=body,
                size=self.size)

            self.scroll_id = page['_scroll_id']
            result_count = page['hits']['total']

            while result_count > 0:
                page = self.db_client._scroll(
                    scroll_id=self.scroll_id,
                    scroll='5m')

                self.scroll_id = page['_scroll_id']
                results = page['hits']['hits']
                result_count = len(results)

                for result in results:
                    yield result["_source"]

    def close(self):
        if self.scroll_id is not None:
            self.db_client._clear_scroll(self.scroll_id)

    def _get_body(self):
        if not self.key_values:
            return None

        if self.key_values:
            filters = [{"term": {key: value}}
                       for key, value in self.key_values.iteritems()]
            body = {
                "query": {
                    "bool": {
                        "filter": [
                            filters
                        ]
                    }
                }
            }
            if self.exclude_fields:
                body["_source"] = {
                    "excludes": self.exclude_fields
                }
        return body


class ElasticsearchBulkWriter(object):
    '''
    Implements a generic Elasticsearch buffered bulk data writer
    'insert', and 'update' operations are implemented.
    'update' is implemented as an `update doc`, with a retry on conflict
    policy = 3
    '''

    MaxDataLen = 104857600  # 100 MB (max allowed HTTP content length)

    def __init__(self, db_client, db_name, doc_type, action, id_builder):
        self.db_client = db_client
        self.db_name = db_name
        self.doc_type = doc_type
        self.action = action
        self.id_builder = id_builder
        self.buff = StringIO()
        self.max_buff_len = ElasticsearchBulkWriter.MaxDataLen / 5
        self.db_lock = threading.Lock()

    def write(self, doc):
        logger.info("##############   Writing to Elastic: %s ...",doc)
        with self.db_lock:
            if self.buff.tell() > self.max_buff_len:
                self.db_client._bulk(
                    self.db_name, self.doc_type, self.buff.getvalue())
                self.buff.close()
                self.buff = StringIO()

            option = {self.action: {}}
            doc_id = self.id_builder(doc)
            option[self.action]['_id'] = doc_id
            if self.action == 'update':
                option[self.action]['_retry_on_conflict'] = 3
            json.dump(option, self.buff)
            self.buff.write('\n')

            if self.action == 'index':
                json.dump(doc, self.buff)
            elif self.action == 'update':
                json.dump({"doc": doc}, self.buff)
            self.buff.write('\n')

    def flush(self):
        # Index the last docs in the buffer
        with self.db_lock:
            if self.buff.tell() > 0:
                self.db_client._bulk(
                    self.db_name, self.doc_type, self.buff.getvalue())

    def close(self):
        self.flush()
        self.buff.close()


class ComposeElasticsearchClient(ElasticsearchClient):

    def __init__(self, config):
        logger.info("Initializing compose elasticsearch client: %s ...",
                    mask_sensitive_info(config))

        es_conf = {
            'hosts': config['public_hostname'],
            'http_auth': (config['username'], config['password']),
            'use_ssl': True,
            'verify_certs': True,
            # 'ca_certs': './certs/compose_cacerts.pem'
            'ca_certs': get_compose_cert_path()
        }
        super(ComposeElasticsearchClient, self).__init__(es_conf)
        logger.info("Compose elasticsearch client initialized.")

def get_compose_cert_path():

    if "kim" in os.environ.get("SPACE", ""):
        # The Open Source env uses Compose Standard which uses certifi
        cert_path = certifi.where()

        if os.path.exists(cert_path):
            return cert_path
        else:
            raise IOError("Compose certs not found")
    else:
        # Looking for astro root directory, where /certs also reside
        path = doi.__file__.replace("/python/doi", "")
        cert_path = os.path.join(
            os.path.dirname(path),
            'certs',
            'compose_cacerts.pem')

        if os.path.exists(cert_path):
            return cert_path

        cert_path = '/usr/src/app/astro/certs/compose_cacerts.pem'

        if os.path.exists(cert_path):
            return cert_path
        else:
            raise IOError("Compose certs not found")
