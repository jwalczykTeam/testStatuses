'''
Created on Sep 30, 2016

@author: adminibm
'''

'''
Best practice that checks whether a few developers are doing mostly
of the work in a project.
The assessment fails if more than 80% of the work is done by less than 20% of
the developers.
'''


from doi.insights.best_practices.common import BestPracticeResult, resolve_interval, moving_average
from doi.util import log_util
from doi.util import time_util


logger = log_util.get_logger(__package__)


'''
More than 20% of developers do 80% of the work
'''
TOP_DEVELOPERS_PERCENTAGE = 20
WORK_THRESHOLD_PERCENTAGE = 80


def _get_distribution(top_developers_committed_files, total_committed_files, write_percentage=False):
    results = [
        {
            "name": "top_developers_committed_files_count",
            "value": top_developers_committed_files
        },
        {
            "name": "other_developers_committed_files_count",
            "value": total_committed_files - top_developers_committed_files
        }
    ]
    if write_percentage:
        results.append({
            'name': "top_developers_committed_files_percentage",
            'value': 0.0 if total_committed_files==0.0 else top_developers_committed_files * 100.0 / total_committed_files
        })
    return results


def _check_and_calc(buckets, doc_count):
#    total_committed_files = raw_data['hits']['total']
    total_committed_files = doc_count
    committed_files_threshold = WORK_THRESHOLD_PERCENTAGE * total_committed_files / 100

#    buckets = raw_data['aggregations']['all']['buckets']
    total_developers = len(buckets)
    developers_threshold = TOP_DEVELOPERS_PERCENTAGE * total_developers / 100

    committed_files = 0
    passed = None
    for i, bucket in enumerate(buckets):
        committed_files += bucket['doc_count']
        if (i+1) > developers_threshold:
            passed = False if committed_files > committed_files_threshold else True
            break

    return (passed, committed_files, total_committed_files)

def assess_distribution(db, index, from_time=None, to_time=None):
    from_time = time_util.resolve_from_date(from_time)
    to_time = time_util.resolve_to_date(to_time)

    query =  {
        "size": 0,
        "query": {
          "query_string": {
            "query": "+_type: committed_file +timestamp:[\"{}\" TO \"{}\"]".format(from_time, to_time)
          }
        },
        "aggs": {
          "all": {
            "terms": {
              "field": "commit_author_name.raw",
              "size": 100000
            }
          }
        }
    }

    logger.info('assess_distribution. index %s, from_time %s, to_time %s. Submitting query: %s', index, from_time, to_time, query)
    raw_data = db.search(index, body=query)

    (passed, committed_files, total_committed_files) = _check_and_calc(raw_data['aggregations']['all']['buckets'], raw_data['hits']['total'])
    message = None
    action = None
    if not passed:
        message = "More than {}% of the work is made by less than {}% of the developers. This seems to be a sort of `hero project`.".format(WORK_THRESHOLD_PERCENTAGE, TOP_DEVELOPERS_PERCENTAGE)
        action = "See best practice documentation to know how to fix this problem."
    else:
        message = "You are doing well, there is a fair distribution of work among developers."

    results =  BestPracticeResult(_get_distribution(committed_files, total_committed_files), passed=passed, message=message, action=action).__dict__
    logger.info('assess_distribution. index %s, from_time %s, to_time %s. Results: %s', index, from_time, to_time, results)
    return results


def assess_time_series(db, index, interval=None, from_time=None, to_time=None, time_format=None):

    def map_results(raw_data):
        time_field = 'key_as_string' if time_format==time_util.ISO_8601_TIME_FORMAT else 'key'
        time_series = raw_data['aggregations']['time_series']['buckets']

        def to_value(bucket):
            (_, top_developers_committed_files, total_committed_files) = _check_and_calc(bucket['all']['buckets'], bucket['doc_count'])

            return {
                "date": bucket[time_field],
                "distribution": _get_distribution(top_developers_committed_files, total_committed_files, True)
            }

        return [to_value(bucket) for bucket in time_series]

    def good_trend(data):
        '''
        Let's calc the moving average over the last 5 samples
        The trend is good if the percentage of work done by top developers
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
                "query": "+_type: committed_file +timestamp:[\"{}\" TO \"{}\"]".format(from_time, to_time)
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
                    "all": {
                        "terms": {
                            "field": "commit_author_name.raw",
                            "size": 100000
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
        message = "You are doing well, the amount of work done by {}% of the developers is decreasing over time, which means a better utilization of the team.".format(TOP_DEVELOPERS_PERCENTAGE)
    else:
        message = "Be careful, the amount of work done by {}% of the developers is increasing, which means a not fair distribution of the work among the team.".format(TOP_DEVELOPERS_PERCENTAGE)
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
