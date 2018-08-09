'''
Created on Mar 10, 2016

@author: alberto
'''

from doi.common import StoppedException
from doi.jobs.miner.resourcedb import ResourceDB
from doi.jobs.miner.resourcedb import ResourceDBBulkInserter
from doi.jobs.miner.miner import Miner
from doi.jobs.miner.miner import mine_data
from doi.jobs.miner import rtc
from doi.util import log_util


logger = log_util.get_logger("astro.jobs.miner")


class RTCIssueMiner(Miner):

    @staticmethod
    def get_source_api_base_url(source):
        return source['api_base_url']

    @staticmethod
    def get_source_project_title(source):
        return source['project_title']

    @staticmethod
    def get_source_username(source):
        return source.get('username')

    @staticmethod
    def get_source_password(source):
        return source.get('password')

    @staticmethod
    def get_source_auth_method(source):
        return source.get('auth_method', "rtc")

    @staticmethod
    def get_source_verify(source):
        return source.get('verify', False)

    @staticmethod
    def get_source_allow_redirects(source):
        return source.get('allow_redirects', True)

    @staticmethod
    def get_source_repo_uri(source, repo_uri_format):
        api_base_url = RTCIssueMiner.get_source_api_base_url(source)
        project_title = RTCIssueMiner.get_source_project_title(source)
        return repo_uri_format.format(api_base_url, project_title)

    def __init__(self, params, progress_callback, stop_event):
        super(RTCIssueMiner, self).__init__(params, progress_callback, stop_event)
        self.count = 0
                
    def mine_data(self):
        with ResourceDB(self.job_name) as resource_db:
            with ResourceDBBulkInserter(resource_db, ResourceDB.ISSUE_TYPE.name) as writer:
                for source in self.sources:
                    if source['source_type'] != 'rtc_project':
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
        auth_method = self.get_source_auth_method(source)
        verify = self.get_source_verify(source)
        allow_redirects = self.get_source_allow_redirects(source)
        project_title = self.get_source_project_title(source)
        
        logger.info("Mining rtc issues: api_base_url='%s', project_title='%s'", 
            api_base_url, project_title)                   
        reader = rtc.RtcIssuesReader(api_base_url, project_title, username, password, auth_method, verify, allow_redirects)
        normalizer = rtc.RtcIssuesNormalizer(reader.client)
        
        for issue in reader.iter():
            if self.stop_event.is_set():
                raise StoppedException
            
            norm_issue = {}
            normalizer.normalize(issue, norm_issue)
            writer.write(norm_issue)
            self.count += 1
            
        logger.info("Mined rtc issues: api_base_url='%s', project_title='%s'", 
            api_base_url, project_title)                   

    def delete_data(self):
        return { 
            "issue_count": 0
        }

def mine_rtc_issues(params, progress_callback, stop_event):
    return mine_data(RTCIssueMiner, params, progress_callback, stop_event)
