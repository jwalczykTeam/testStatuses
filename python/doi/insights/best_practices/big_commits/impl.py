'''
Created on Sep 30, 2016

@author: adminibm
'''

'''
Best practice that checks against commits with too many files.
WARNING: the implementation would be much easier if commit would contain
the files_changed_count attribute
'''

from doi.insights.best_practices.common import BestPracticeResult, resolve_interval, moving_average, stats
from doi.util import time_util
from doi.util import log_util

logger = log_util.get_logger("astro.insights.best_practices.big_commits")

def _get_distribution(buckets):

    '''
    Calculate average, standard deviation, and max # files on the
    returned commits,
    then return the commits that include > 3 * sigma files.
    '''
    file_stats = stats(map(lambda x: x["doc_count"], buckets))

    sigma1 = (file_stats["sigma"]*1) + file_stats["avg"]
    sigma2 = (file_stats["sigma"]*2) + file_stats["avg"]
    sigma3 = (file_stats["sigma"]*3) + file_stats["avg"]

    bad_commits = filter(lambda x: x["doc_count"]>=sigma2, buckets)

    logger.info("*****  _get_dist: bad commits = '%s' len bad commits = '%s' sigma3 = '%s' sigma2 = '%s' sigma1 = '%s' sigma = '%s'", bad_commits, len(bad_commits), sigma3,sigma2,sigma1,file_stats["sigma"] )
    
    results = [
        {
            "name": "big_commits_count",
            "value": len(bad_commits)
        },
        {
            "name": "max_files_count",
            "value": file_stats["max"]
        },
        {
            "name": "avg_files_count",
            "value": file_stats["avg"]
        }
    ]

    return results


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
            "commits": {
                "terms": {
                    "field": "commit_uri",
                    "size": 10000000
                }
            }
        }
    }

    logger.info('assess_distribution. index %s, from_time %s, to_time %s. Submitting query: %s',
                index, from_time, to_time, query)

    raw_data = db.search(index, body=query)

    data = _get_distribution(raw_data['aggregations']['commits']['buckets'])

    passed = True if data[0]["value"]==0 else False
    message = None
    action = None
    if not passed:
        message = "There are {} commits that contain an unusually high number of files. The biggest one contains {} files.".format(data[0]["value"], data[1]["value"])
        action = "See documentation to know how to enable files predictions."
    else:
        message = "You are doing well, there are no commits changing a high number of files. The biggest one contains {} files.".format(data[1]["value"])

    results =  BestPracticeResult(data, passed=passed, message=message, action=action).__dict__
    logger.info('assess_distribution. index %s, from_time %s, to_time %s. Results: %s', index, from_time, to_time, results)
    return results


def assess_time_series(db, index, interval=None, from_time=None, to_time=None, time_format=None):

    def map_results(raw_data):
        time_field = 'key_as_string' if time_format==time_util.ISO_8601_TIME_FORMAT else 'key'
        time_series = raw_data['aggregations']['time_series']['buckets']

        def to_value(bucket):
            data = _get_distribution(bucket["commits"]["buckets"])
            return {
                "date": bucket[time_field],
                "distribution": data
            }

        return [to_value(bucket) for bucket in time_series]

    def good_trend(data):
        '''
        Let's calc the moving average over the last 5 samples
        The trend is good if the maximum number of files in a commit
        is not increasing over time.
        '''
        num_samples = 5
        if len(data) <= num_samples:
            return True

        avgs = list(moving_average(num_samples, map(lambda x: x['distribution'][1]['value'], data)))
        return (avgs[-1] <= avgs[-2])


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
                    "commits": {
                        "terms": {
                            "field": "commit_uri",
                            "size": 10000000
                        }
                    }
                }
            }
        }
    }

    raw_data = db.search(index, body=query)

    data = map_results(raw_data)
    passed = good_trend(data)
    message = None
    action = None
    if passed:
        message = "You are doing well, the maximum number of files included in commits is not increasing."
    else:
        message = "Be careful, the maximum number of files changed by a single commit is increasing."
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
