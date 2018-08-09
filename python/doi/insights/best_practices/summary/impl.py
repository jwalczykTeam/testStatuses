'''
Created on Sep 30, 2016

@author: adminibm
'''

'''
A special best practice that produces a single state summary (green, yellow, red) by
assessing all the other best practices into a single state.
This best practice only implements the assess_distribution function.
'''

from doi.insights.best_practices.common import get_packages, get_assessment_function, BestPracticeResult
import doi.insights.best_practices as best_practices
from doi.util import log_util


logger = log_util.get_logger(__package__)


def assess_distribution(db, index, from_time=None, to_time=None):
    logger.info('assess_distribution. index %s, from_time %s, to_time %s.', index, from_time, to_time)

    '''
    List all best practices in the current package that implement the assess_distribution function
    with exception of this one: summary
    '''
    passed = False
    message = 'You are not implementing some best practices.'
    state = 'yellow'
    passeds = 0
    total = 0
    for p in get_packages(best_practices):
        print p.__name__
        if p.__name__ != __package__:
            fn = get_assessment_function(p)
            results = fn(db, index, from_time, to_time)
            logger.debug('Raw assessment result: %s', results)
            total += 1
            if results['passed']:
                passeds += 1

    if passeds==total:
        state = 'green'
        passed = True
        message = 'Your project is shining.'
    elif passeds<total/2:
        message = 'You are not implementing mostly of the coding best practices.'
        state = 'red'

    data = [
        {
            "name": "state",
            "value": state
        },
        {
            "name": "best_practices_passed_count",
            "value": passeds
        },
        {
            "name": "best_practices_total_count",
            "value": total
        }
    ]

    results =  BestPracticeResult(data, passed=passed, message=message, action='None').__dict__
    logger.info('assess_distribution. index %s, from_time %s, to_time %s. Results: %s', index, from_time, to_time, results)
    return results
