'''
Created on Sep 30, 2016

@author: adminibm
'''

from doi.util import log_util
from doi.util import time_util
from itertools import islice, tee
import pkgutil
import numpy as np
from numpy import mean

logger = log_util.get_logger("astro.insights.best_practices")

def moving_average(n, iterable):
    # Leading 0s
    for _ in range(1, n):
        yield 0.

    # Actual averages
    head, tail = tee(iterable)
    sum_ = float(sum(islice(head, n)))
    while True:
        yield sum_ / n
        sum_ += next(head) - next(tail)


def stats(iterable):
    '''
    Calc several stats on an iterable of numbers
    '''    
    #logger.info("*****  stats: '%s'", iterable)

    return {"count": len(iterable), "min": min(iterable), "max": max(iterable), "avg": mean(iterable), "sigma": np.std(iterable)}

def resolve_interval(interval):
    if interval is None:
        interval = "day"

    return interval


class BestPracticeResult(object):
    '''
    Hold the result of a best practice assessment
    '''
    def __init__(self, data, passed=None, message=None, action=None):
        self.data = data
        self.passed = passed
        self.message = message
        self.action = action

def get_packages(package):
    '''
    Return the list of children packages of package
    '''
    packages = []
    prefix = package.__name__ + "."
    print 'looking into package ', prefix
    for _, modname, ispkg in pkgutil.iter_modules(package.__path__, prefix):
        module = __import__(modname, fromlist="dummy")
        print 'found module ', module.__name__
        if ispkg:
            packages.append(module)

    print 'returning packages ', packages
    return packages

def get_distribution_of_missing_field(db, index, type_, field, field_label, from_time=None, to_time=None, add_to_query=""):
    '''
    Create an aggregation query for missing field and return
    the buckets to the caller.
    It can be used in several best practices that need to check for
    missing fields.
    '''

    def map_results(raw):
        return [
            {
                "name": "missing_{}_count".format(field_label),
                "value": raw['aggregations']['missing_field']['doc_count']
            },
            {
                "name": "{}_count".format(field_label),
                "value": raw['hits']['total'] - raw['aggregations']['missing_field']['doc_count']
            }
        ]

    from_time = time_util.resolve_from_date(from_time)
    to_time = time_util.resolve_to_date(to_time)

    query =  {
        "size": 0,
        "query": {
            "query_string": {
                "query": "+_type: {} {} +timestamp:[\"{}\" TO \"{}\"]".format(type_, add_to_query, from_time, to_time)
            }
        },
        "aggs": {
            "missing_field": {
                "missing": {
                    "field": field
                }
            }
        }
    }

    logger.info('get_distribution_of_missing_field. Submitting query: %s',
                query)

    raw_data = db.search(index, body=query)
    missing_field_percentage = 0.0 if raw_data['hits']['total']==0 else raw_data['aggregations']['missing_field']['doc_count'] * 100.0 / raw_data['hits']['total']
    return (map_results(raw_data), missing_field_percentage)


def get_time_series_of_missing_field(db, index, type_, field, field_label, interval=None, from_time=None, to_time=None, time_format=None, add_to_query=""):

    def map_results(raw):
        time_field = 'key_as_string' if time_format==time_util.ISO_8601_TIME_FORMAT else 'key'
        time_series = raw['aggregations']['time_series']['buckets']

        def to_value(bucket):
            return {
                "date": bucket[time_field],
                "distribution": [
                    {
                        "name": "missing_{}_count".format(field_label),
                        "value": bucket['missing_field']['doc_count']
                    },
                    {
                        'name': "{}_count".format(field_label),
                        'value': bucket['doc_count']-bucket['missing_field']['doc_count']
                    },
                    {
                        'name': "missing_{}_percentage".format(field_label),
                        'value': 0.0 if bucket['doc_count']==0.0 else bucket['missing_field']['doc_count'] * 100.0 / bucket['doc_count']
                    }
                ]
            }

        return [to_value(bucket) for bucket in time_series]

    def good_trend(raw):
        '''
        Calculate a moving_average on the last 5 samples and verifies that
        the last moving avg is less than the previous one (the trend is decreasing).
        Need at least 6 samples for being able to evaluate the last two moving averages.
        If not, then let's say it's a good trend.
        '''
        num_samples = 5
        buckets = raw['aggregations']['time_series']['buckets']
        if len(buckets) <= num_samples:
            return True

        '''
        Create the series of the bad commits percentages
        '''
        bads = lambda x: 0.0 if  x['doc_count']==0 else x['missing_field']['doc_count'] * 100.0 / x['doc_count']
        avgs = list(moving_average(num_samples, map(bads, buckets)))
        return (avgs[-1] < avgs[-2])


    from_time = time_util.resolve_from_date(from_time)
    to_time = time_util.resolve_to_date(to_time)
    interval = resolve_interval(interval)

    query = {
        "size": 0,
        "query": {
            "query_string": {
                "query": "+_type: {} {} +timestamp:[\"{}\" TO \"{}\"]".format(type_, add_to_query, from_time, to_time)
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
                    "missing_field": {
                        "missing": {
                            "field": field
                        }
                    }
                }
            }
        }
    }

    raw_data = db.search(index, body=query)
    return (map_results(raw_data), good_trend(raw_data))

def _get_method(package, name):
    '''
    Return the first callable method with name
    found in any of the modules in the package.
    Return None if name cannot found.
    '''
    prefix = package.__name__ + "."
    for _, modname, ispkg in pkgutil.iter_modules(package.__path__, prefix):
        if not ispkg:
            module = __import__(modname, fromlist="dummy")
            method = getattr(module, name, None)
            if method and callable(method):
                return method

    return None

def _get_package(name):
    return __import__(name, fromlist="dummy")

'''
The name of the best practice function that performs a global assessment.
'''
_AssessDistributionFn='assess_distribution'

'''
The name of the best practice function that performs a timeseries assessment
that is, an assessment repeated on last weeks, months, and so on. It is used to
figure out the trend of the best practice.
'''
_AssessTimeSeriesFn='assess_time_series'


def _get_assess_fn(best_practice_name, fn_name):
    assess_fn = None
    fullname = __package__ + '.' + best_practice_name
    try:
        assess_fn = _get_method(_get_package(fullname), fn_name)
    except ImportError as e:
        logger.error('Cannot load package %s for best practice %s. Best practice does not exist.', fullname, best_practice_name)
        raise ValueError('Cannot load best practice {}. Error: {}'.format(best_practice_name, str(e)))

    if not assess_fn:
        logger.error('Cannot load %s function for best practice %s. Best practice not implemented.', fn_name, best_practice_name)
        raise ValueError('Best practice function {} not implemented for {}.'.format(fn_name, best_practice_name))

    return assess_fn


def get_assessment_function(package):
    return _get_method(package, _AssessDistributionFn)

def best_practice_assess_distribution(best_practice_name, db, index, from_time=None, to_time=None):
    '''
    Load the best practice package and invoke the assess_distribution method
    '''

    assess_fn = _get_assess_fn(best_practice_name, _AssessDistributionFn)
    results = assess_fn(db, index, from_time, to_time)

    return results

def best_practice_assess_time_series(best_practice_name, db, index, interval, from_time, to_time, time_format):
    '''
    Load the best practice package and invoke the assess_time_series method
    '''
    assess_fn = _get_assess_fn(best_practice_name, _AssessTimeSeriesFn)

    results = assess_fn(db, index, interval, from_time, to_time, time_format)

    return results
