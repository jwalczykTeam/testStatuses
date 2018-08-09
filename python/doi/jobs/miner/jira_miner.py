'''
Created on Mar 10, 2016

@author: alberto
'''

from doi.common import StoppedException
from doi.jobs.miner.resourcedb import ResourceDB
from doi.jobs.miner.resourcedb import ResourceDBBulkInserter
from doi.jobs.miner import jira
from doi.jobs.miner.miner import Miner
from doi.jobs.miner.miner import mine_data
from doi.util import log_util


logger = log_util.get_logger("astro.jobs.miner")


class JiraIssueMiner(Miner):
    def __init__(self, params, progress_callback, stop_event):
        super(JiraIssueMiner, self).__init__(params, progress_callback, stop_event)
        self.count = 0
        
    @staticmethod
    def get_source_api_base_url(source):
        return source['api_base_url']

    @staticmethod
    def get_source_project_key(source):
        return source['project_key']

    @staticmethod
    def get_source_username(source):
        return source.get('username')

    @staticmethod
    def get_source_password(source):
        return source.get('password')

    @staticmethod
    def get_source_repo_uri(source, repo_uri_format):
        api_base_url = JiraIssueMiner.get_source_api_base_url(source)
        project_key = JiraIssueMiner.get_source_project_key(source)
        return repo_uri_format.format(api_base_url, project_key)

    def mine_data(self):
        with ResourceDB(self.job_name) as resource_db:
            with ResourceDBBulkInserter(resource_db, ResourceDB.ISSUE_TYPE.name) as writer:
                for source in self.sources:
                    if source['source_type'] != 'jira_project':
                        continue
        
                    self.mine_source(source, writer)
        
                return { 
                    "issue_count": self.count
                }
                    
    def mine_source(self, source, writer):
        # Last_updated_time = max of f.ISSUE_UPDATED_TIME
        # Get source properties
        api_base_url = self.get_source_api_base_url(source)
        username = self.get_source_username(source)
        password = self.get_source_password(source)
        project_key = self.get_source_project_key(source)
        
        logger.info("Mining jira issues: api_base_url='%s', project_key='%s'", api_base_url, project_key)
        reader = jira.JiraIssuesReader(api_base_url, username, password, project_key)
        normalizer = jira.JiraIssuesNormalizer()
        
        for issue in reader.iter():
            if self.stop_event.is_set():
                raise StoppedException

            norm_issue = {}
            normalizer.normalize(issue, norm_issue)
            writer.write(norm_issue)
            self.count += 1
            
        logger.info("Mined jira issues: api_base_url='%s', project_key='%s'", api_base_url, project_key)
    
    def delete_data(self):
        return { 
            "issue_count": 0
        }


def mine_jira_issues(params, progress_callback, stop_event):
    return mine_data(JiraIssueMiner, params, progress_callback, stop_event)
