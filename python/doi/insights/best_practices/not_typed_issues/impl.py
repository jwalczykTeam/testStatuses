'''
Created on Sep 30, 2016

@author: adminibm
'''

'''
Best practice that checks that you are classifying the issues (assigning a type).
'''


from doi.insights.best_practices.common import BestPracticeResult, get_time_series_of_missing_field, get_distribution_of_missing_field
from doi.util import log_util


logger = log_util.get_logger(__package__)

'''
More than 20% of issues not classified
'''
BAD_ISSUES_THRESHOLD = 20


def assess_time_series(db, index, interval=None, from_time=None, to_time=None, time_format=None):
    logger.info('assess_time_series. index %s, from_time %s, to_time %s, interval %s. Submitting query: %s', index, from_time, to_time, interval)
    data, passed = get_time_series_of_missing_field(db, index, 'issue', 'issue_types.raw', 'issue_types', interval, from_time, to_time, time_format, '-_exists_: pull_request_uri')

    message = None
    action = None
    if passed:
        message = "You are doing well, the number of issues without a type is decreasing over time."
    else:
        message = "Be careful, the number of issues without a type is increasing."
        action = "See best practice documentation to know how to fix this problem."

    logger.info('assess_time_series. index %s, from_time %s, to_time %s, interval %s, passed %s.', index, from_time, to_time, interval, passed)
    return BestPracticeResult(data, passed=passed, message=message, action=action).__dict__


def assess_distribution(db, index, from_time=None, to_time=None):
    '''
    - Build the query
    - Run the query
    - Build the data to be returned
    - Run the check function, which builds the assessment message
    '''
    logger.info('assess_distribution. index %s, from_time %s, to_time %s. Submitting query: %s', index, from_time, to_time)
    data, bad_issues_percentage = get_distribution_of_missing_field(db, index, 'issue', 'issue_types.raw', 'issue_types', from_time, to_time, '-_exists_: pull_request_uri')
    logger.debug('Bad issues percentage: %f', bad_issues_percentage)

    '''
    TODO: need to load msg templates from a best practice config file
    '''
    passed = True
    message = None
    action = None
    if bad_issues_percentage > BAD_ISSUES_THRESHOLD:
        passed = False
        message = "Over {}% of the issues ({}%) do not have a type.".format(BAD_ISSUES_THRESHOLD, round(bad_issues_percentage, 2))
        action = "See best practice documentation to know how to fix this problem."
    else:
        message = "You are doing well, there is a small percentage of issues ({}%) not typed.".format(round(bad_issues_percentage, 2))

    results = BestPracticeResult(data, passed=passed, message=message, action=action).__dict__
    logger.info('assess_distribution. index %s, from_time %s, to_time %s. Results: %s', index, from_time, to_time, results)
    return results


def assess_resources(db, index, resources):
    '''
    Run assessment against a group of resources and returns
    the results
    TODO:
    '''
    pass
