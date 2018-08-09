'''
Created on Jul 16, 2016

@author: alberto
'''


from doi import constants
from doi.jobs.miner.resourcedb import ResourceDB, ResourceDBBulkReader
from doi.jobs.miner import fields as f
from doi.util import cf_util
from doi.util import log_util
import json
import os

logger = log_util.get_logger("astro.jobs.miner.verifier")


class Verifier(object):
    def __init__(self, resource_db):
        super(Verifier, self).__init__()
        self.resource_db = resource_db
        
    def verify_commit_issues(self):
        n = 1
        commit_failed_count = 0
        commit_successful_count = 0
        reader = ResourceDBBulkReader(
            self.resource_db, ResourceDB.COMMIT_TYPE.name)
        for commit in reader.iter():
            commit_id = commit[f.COMMIT_ID]            
            print("Commit #{}, id='{}' ...".format(n, commit_id))
            if f.COMMIT_ISSUES not in commit:
                print(">>>> Missing {} field in commit record".format(f.COMMIT_ISSUES))
                commit_failed_count += 1
            else:
                commit_successful_count += 1
            n += 1

        n = 1
        file_failed_count = 0
        file_successful_count = 0
        reader = ResourceDBBulkReader(
            self.resource_db, ResourceDB.COMMITTED_FILE_TYPE.name)
        for committed_file in reader.iter():
            commit_id = committed_file[f.COMMIT_ID]
            file_path  = committed_file[f.FILE_PATH]
            print("Committed file #{}, id='{}' file='{}' ...".format(n, commit_id, file_path.encode('utf-8')))
            if f.COMMIT_ISSUES not in committed_file:
                print(">>>> Missing {} field in commit record".format(f.COMMIT_ISSUES))
                file_failed_count += 1
            else:
                file_successful_count += 1
            n += 1

        print("Successful commits: {}".format(commit_successful_count))
        print("Failed commits: {}".format(commit_failed_count))            
        print("Successful files: {}".format(file_successful_count))
        print("Failed files: {}".format(file_failed_count))

    def verify_file_commit_history(self, file_key):
        reader = ResourceDBBulkReader(
            self.resource_db, ResourceDB.COMMITTED_FILE_TYPE.name, {f.FILE_KEY: file_key})
        for committed_file in reader.iter():
            print committed_file[f.COMMIT_ID]


os.environ[constants.ASTRO_DB_ENV_VAR] = json.dumps(cf_util.get_db_credentials())    

#job_name = "a4d0bd70-e487-4955-a7a7-c81bcbd5a666@astro-scheduler-adtech-1-prod"
#job_name = "cassandra@alberto"
#job_name = "missing_commit_issues@alberto"
job_name = "deeplearning@alberto"
with ResourceDB(job_name) as resource_db:
    verifier = Verifier(resource_db)
    verifier.verify_commit_issues()
