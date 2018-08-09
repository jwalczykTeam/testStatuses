'''
Created on Sep 30, 2016

@author: adminibm
'''

'''
Best practice that checks against a big number of defective files in the project.
'''

from doi.insights.best_practices.common import BestPracticeResult, resolve_interval, moving_average
from doi.util import log_util
from doi.util import time_util


logger = log_util.get_logger(__package__)


'''
More than 20% of the files are classified as defective
'''
BAD_FILES_THRESHOLD = 20


def _get_distribution(buckets, write_percentage=False):
    predictions = len(buckets)>0
    files = { "good": 0, "bad": 0 }

    def goods_n_bads(bucket, files):
        k = bucket["key"]
        if k == 0:
            files["good"] = bucket["doc_count"]
        elif k == 1:
            files["bad"] = bucket["doc_count"]
        else:
            logger.fatal('_get_distribution. Unexpected value for key field: %s', k)

    if predictions:
        goods_n_bads(buckets[0], files)
        if len(buckets)==2:
            goods_n_bads(buckets[1], files)

    results = [
        {
            "name": "defective_files_count",
            "value": files["bad"]
        },
        {
            "name": "other_files_count",
            "value": files["good"]
        }
    ]

    bad_files_percentage = 0.0 if not predictions else files["bad"] * 100.0 / (files["bad"] + files["good"])

    if write_percentage:
        results.append({
            'name': "defective_files_percentage",
            'value': bad_files_percentage
        })

    return (results, bad_files_percentage, predictions)


def assess_distribution(db, index, from_time=None, to_time=None):
    from_time = time_util.resolve_from_date(from_time)
    to_time = time_util.resolve_to_date(to_time)

    query =  {
        "size": 0,
        "query": {
            "query_string": {
                "query": "+_type: file_defect_prediction +timestamp:[\"{}\" TO \"{}\"]".format(from_time, to_time)
            }
        },
        "aggs": {
            "predictions": {
                "terms": {
                    "field": "prediction",
                    "size": 1000
                }
            }
        }
    }

    logger.info('assess_distribution. index %s, from_time %s, to_time %s. Submitting query: %s', index, from_time, to_time, query)
    raw_data = db.search(index, body=query)

    (data, bad_files_percentage, predictions) = _get_distribution(raw_data['aggregations']['predictions']['buckets'])

    passed = True if bad_files_percentage < BAD_FILES_THRESHOLD else False
    message = None
    action = None
    if not predictions:
        message = "No files predictions available."
        action = "See documentation to know how to enable files predictions."
    elif not passed:
        message = "More than {}% of the files ({}%) are classified as defective.".format(BAD_FILES_THRESHOLD, bad_files_percentage)
        action = "See best practice documentation to know how to fix this problem."
    else:
        message = "You are doing well, there is a small percentage ({}%) of the files classified as defective.".format(bad_files_percentage)

    results =  BestPracticeResult(data, passed=passed, message=message, action=action).__dict__
    logger.info('assess_distribution. index %s, from_time %s, to_time %s. Results: %s', index, from_time, to_time, results)
    return results


def assess_time_series(db, index, interval=None, from_time=None, to_time=None, time_format=None):

    def map_results(raw_data):
        time_field = 'key_as_string' if time_format==time_util.ISO_8601_TIME_FORMAT else 'key'
        time_series = raw_data['aggregations']['time_series']['buckets']

        def to_value(bucket):
            (data, _, _) = _get_distribution(bucket["predictions"]["buckets"], True)
            return {
                "date": bucket[time_field],
                "distribution": data
            }

        return [ to_value(bucket) for bucket in time_series]

    def good_trend(data):
        '''
        Let's calc the moving average over the last 5 samples
        The trend is good if the percentage of defective files
        is decreasing over time
        '''
        num_samples = 5
        if len(data) <= num_samples:
            return True

        avgs = list(moving_average(num_samples, map(lambda x: x['distribution'][2]['value'], data)))
        return (avgs[-1] < avgs[-2])


    from_time = time_util.resolve_from_date(from_time)
    to_time = time_util.resolve_to_date(to_time)
    interval = resolve_interval(interval)

    query = {
        "size": 0,
        "query": {
            "query_string": {
                "query": "+_type: file_defect_prediction +timestamp:[\"{}\" TO \"{}\"]".format(from_time, to_time)
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
                    "predictions": {
                        "terms": {
                            "field": "prediction",
                            "size": 1000
                        }
                    }
                }
            }
        }
    }

    logger.info('assess_time_series. index %s, from_time %s, to_time %s, interval %s. Submitting query: %s', index, from_time, to_time, interval, query)
    raw_data = db.search(index, body=query)

    data = map_results(raw_data)
    passed = good_trend(data)
    message = None
    action = None
    if passed:
        message = "You are doing well, the number of defective files is decreasing over time."
    else:
        message = "Be careful, the amount of defective files is increasing."
        action = "See best practice documentation to know how to fix this problem."

    logger.info('assess_time_series. index %s, from_time %s, to_time %s, interval %s, passed %s.', index, from_time, to_time, interval, passed)
    return BestPracticeResult(data, passed=passed, message=message, action=action).__dict__


def assess_resources(db, index, resources):
    '''
    Run assessment against a group of resources and returns
    the results
    TODO:
    '''
    pass
