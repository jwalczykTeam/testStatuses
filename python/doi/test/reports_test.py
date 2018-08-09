'''
Created on Sep 29, 2016

@author: alberto
'''

from doi.util import cf_util
from doi.util.db import db_util
import json

db = db_util.Database.factory(cf_util.get_db_credentials())

results = db.get_resource_count_distribution(db, 
    "kafka", "issue", "issue_types")
print(json.dumps(results, indent=2))

results = db.get_resource_sum_distribution(db, 
    "kafka", "committed_file", "file_changed_line_count", "commit_issues.issue_types")
print(json.dumps(results, indent=2))

results = db.get_resource_count_time_series(db, 
    "kafka", "issue", "issue_types", "month", time_format='iso8601')
print(json.dumps(results, indent=2))

results = db.get_resource_sum_time_series(db, 
    "kafka", "committed_file", "file_changed_line_count", "commit_issues.issue_types", "month")
print(json.dumps(results, indent=2))
