'''
Created on Mar 10, 2016

@author: alberto
'''

from doi.common import StoppedException
from doi.jobs.miner.resourcedb import ResourceDB
from doi.jobs.miner.resourcedb import ResourceDBBulkInserter
from doi.jobs.miner import jenkins
from doi.jobs.miner.miner import Miner
from doi.jobs.miner.miner import mine_data
from doi.util import log_util


logger = log_util.get_logger("astro.jobs.miner")


class JenkinsJobMiner(Miner):
    def __init__(self, params, progress_callback, stop_event):
        super(JenkinsJobMiner, self).__init__(params, progress_callback, stop_event)
        self.count = 0
                
    def mine_data(self):
        with ResourceDB(self.job_name) as resource_db:
            with ResourceDBBulkInserter(resource_db, ResourceDB.BUILD_TYPE.name) as writer:
                for source in self.sources:
                    if source['source_type'] != 'jenkins_job':
                        continue
                    
                    self.mine_source(source, writer)
                
                return { 
                    "build_count": self.count
                }

    def mine_source(self, source, writer):
        # Get source properties
        api_base_url = source.get('api_base_url')
        access_token = source.get('access_token')
        job_name = source['job_name']
        repo_url = source['repo_url']
        
        logger.info("Mining jenkins jobs: api_base_url='%s', job_name='%s', repo_url='%s'",
            api_base_url, job_name, repo_url)
        reader = jenkins.JenkinsBuildsReader(api_base_url, access_token, job_name, repo_url)
        normalizer = jenkins.JenkinsBuildsNormalizer()
        
        for build in reader.iter():
            if self.stop_event.is_set():
                raise StoppedException
            
            norm_build = {}
            normalizer.normalize(build, norm_build)
            writer.write(norm_build)
            self.count += 1
            
        logger.info("Mined jenkins jobs: api_base_url='%s', job_name='%s', repo_url='%s'",
            api_base_url, job_name, repo_url)
    
    def delete_data(self):
        return { 
            "build_count": 0
        }


def mine_jenkins_job(params, progress_callback, stop_event):
    return mine_data(JenkinsJobMiner, params, progress_callback, stop_event)
