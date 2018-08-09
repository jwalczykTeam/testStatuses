'''
Created on Jun 20, 2016

@author: guilherme
'''

from doi.jobs.miner import fields as f
from doi.util import rest
from doi.jobs.miner import pipeline
from doi.util import json_util
from doi.util import log_util
import json
import urllib


logger = log_util.get_logger("astro.jobs.miner.jenkins")


# Here all code to connect to the jenkins server, authentication, etc. 
class JenkinsClient(rest.RestClient):
    BUILDS_URI_FORMAT = "{}/job/{}/api/json"   # params: api_base_url, job_name,
    BUILD_URI_FORMAT = "{}/job/{}/{}/api/json" # params: api_base_url, job_name, build_number
    BUILD_TESTS_URI_FORMAT = "{}/job/{}/{}/testReport/api/json"

    def __init__(self, api_base_url, access_token=None):
        super(JenkinsClient, self).__init__()        
        self.api_base_url = api_base_url
        self.access_token = access_token
        if access_token is not None:
            self.add_token_auth_header(access_token)


# Here all the code common to all jenkins resource readers
class JenkinsBuildsReader(pipeline.Reader):
    def __init__(self, api_base_url, access_token, job_name, repo_url):
        super(JenkinsBuildsReader, self).__init__()
        self.client = JenkinsClient(api_base_url, access_token)
        self.job_name = job_name
        self.repo_url = repo_url
    
    def iter(self):
        for build_number in self.get_all_build_numbers(): 
            build = self.get_build(build_number)
            yield build
        
    def get_build(self, build_number):
        build_uri = JenkinsClient.BUILD_URI_FORMAT.format(self.client.api_base_url, self.job_name, build_number)
        response = self.client.get(build_uri)
        build = json.loads(response.content, strict=False)
        build['RepoUrl'] = self.repo_url
        
        # Add associated tests
        tests_uri = JenkinsClient.BUILD_TESTS_URI_FORMAT.format(self.client.api_base_url, self.job_name, build_number)
        response = self.client.get(tests_uri)
        build_tests = json.loads(response.content, strict=False)
        build_tests[f.TESTS_BUILD_URI] = tests_uri
        build['ALL_TESTS'] = build_tests
        return build

    def get_build_numbers(self):
        headers = { 
            'Accept': "application/json"
        }        
        
        build_numbers = []
        params = {'tree': "allBuilds[number]"}        
        builds_uri = JenkinsClient.BUILDS_URI_FORMAT.format(self.client.api_base_url, self.job_name)
        query_uri = "{}?{}".format(builds_uri, urllib.urlencode(params))
        logger.info("Get '%s'", query_uri)
        response = self.client.get(query_uri, headers=headers)
        content = json.loads(response.content)
        for build in content['allBuilds']:
            build_numbers.append(build['number'])

        return build_numbers

    def close(self):
        self.client.close()


class JenkinsBuildsNormalizer(pipeline.Normalizer):
    def __init__(self):
        super(JenkinsBuildsNormalizer, self).__init__()        

    def normalize(self, build, norm_build):
        norm_build = {}
        json_util.set_value(norm_build, f.BUILD_ID,                 build, 'id')
        json_util.set_value(norm_build, f.BUILD_NAME,               build, 'fullDisplayName')
        json_util.set_value(norm_build, f.BUILD_RESULT,             build, 'result')
        json_util.set_value(norm_build, f.BUILD_STATUS,             build, 'building')
        json_util.set_value(norm_build, f.BUILD_URI,                build, 'url')
        json_util.set_value(norm_build, f.BUILD_DURATION,           build, 'duration')
        json_util.set_value(norm_build, f.BUILD_ESTIMATED_DURATION, build, 'estimatedDuration')
        json_util.set_value(norm_build, f.BUILD_TIMESTAMP,          build, 'timestamp')
        json_util.set_value(norm_build, f.BUILD_BUILT_ON,           build, 'builtOn')
        json_util.set_value(norm_build, f.BUILD_QUEUE_ID,           build, 'queueId')
        json_util.set_value(norm_build, f.BUILD_CULPRITS,           build, 'culprits')
        json_util.set_value(norm_build, f.BUILD_SCM,                build, 'changeSet', 'kind')
    
        json_util.set_first_value(norm_build, f.BUILD_SHORT_DESCRIPTION,   build, 'shortDescription')
        json_util.set_first_value(norm_build, f.BUILD_COMMIT_ID,           build, 'commitId')
        json_util.set_first_value(norm_build, f.BUILD_GIT_URI,             build, 'remoteUrls')
        json_util.set_first_value(norm_build, f.BUILD_TESTS_FAILED_COUNT,  build, 'failCount')
        json_util.set_first_value(norm_build, f.BUILD_TESTS_SKIPPED_COUNT, build, 'skipCount')
        json_util.set_first_value(norm_build, f.BUILD_TESTS_TOTAL_COUNT,   build, 'totalCount')
        json_util.set_first_value(norm_build, f.BUILD_TESTS_URI_NAME,      build, 'urlName')
    
        #norm_build[f.BUILD_TESTS_PASSED_COUNT] = norm_build[f.BUILD_TESTS_TOTAL_COUNT] - norm_build[f.BUILD_TESTS_SKIPPED_COUNT] - norm_build[f.BUILD_TESTS_FAILED_COUNT]
    
        json_util.set_first_value_with_ref_item(norm_build, f.BUILD_COMMIT_AUTHOR_NAME,  build, 'ghprbActualCommitAuthor', 'name', 'value')
        json_util.set_first_value_with_ref_item(norm_build, f.BUILD_COMMIT_AUTHOR_EMAIL, build, 'ghprbActualCommitAuthorEmail', 'name', 'value')
        json_util.set_first_value_with_ref_item(norm_build, f.BUILD_GIT_BRANCH,          build, 'gitBranch', 'name', 'value')
        json_util.set_first_value_with_ref_item(norm_build, f.BUILD_PULL_URI,            build, 'ghprbPullLink', 'name', 'value')
        json_util.set_first_value_with_ref_item(norm_build, f.BUILD_COMMIT_ID,           build, 'lastBuiltRevision', 'name', 'value')
        json_util.set_first_value_with_ref_item(norm_build, f.BUILD_COMMIT_ID,           build, 'ghprbActualCommit', 'name', 'value')
        norm_build[f.BUILD_REPO_URI] = build['RepoUrl']
        
        tests = build.get('ALL_TESTS')
        if tests is not None:
            norm_build[f.BUILD_TESTS] = self.normalize_test(tests)
    
        norm_build[f.TIMESTAMP] = norm_build[f.BUILD_TIMESTAMP]
        norm_build[f.BUILD_SOURCE_TYPE] = 'jenkins'
    
    def normalize_test(self, test):
        norm_test = {}
    
        if 'childReports' in test:
            norm_test[f.TESTS_HAS_CHILDREN] = True
            children = []
            test_children = test['childReports']
            for test_child in test_children:
                child = {}
                child[f.TESTS_CHILD_BUILD_NUMBER] = test_child['child']['number']
                child[f.TESTS_CHILD_BUILD_URI] = test_child['child']['url']
                child.update(self.normalize_test(test_child['result']))
                children.append(child)
            norm_test[f.TESTS_CHILDREN] = children
        else:
            norm_test[f.TESTS_HAS_CHILDREN] = False
            json_util.set_value(norm_test, f.TESTS_TOTAL_DURATION, test, 'duration')
            json_util.set_value(norm_test, f.TESTS_IS_EMPTY, test, 'empty')
            json_util.set_value(norm_test, f.TESTS_SKIPPED_COUNT, test, 'skipCount')
            json_util.set_value(norm_test, f.TESTS_PASSED_COUNT, test, 'passCount')
            json_util.set_value(norm_test, f.TESTS_FAILED_COUNT, test, 'failCount')
            
            norm_test[f.TESTS_TOTAL_COUNT] = norm_test[f.TESTS_FAILED_COUNT] + norm_test[f.TESTS_SKIPPED_COUNT] + norm_test[f.TESTS_PASSED_COUNT]
    
            tests = []
            test_suites = test['suites']
            for suite in test_suites:
                for case in suite['cases']:
                    test = {}
                    json_util.set_value(test, f.TEST_AGE, case, 'age')
                    json_util.set_value(test, f.TEST_CLASS_NAME, case, 'className')
                    json_util.set_value(test, f.TEST_DURATION, case, 'duration')
                    json_util.set_value(test, f.TEST_FAILED_SINCE, case, 'failedSince')
                    json_util.set_value(test, f.TEST_NAME, case, 'name')
                    json_util.set_value(test, f.TEST_SKIPPED, case, 'skipped')
                    json_util.set_value(test, f.TEST_STATUS, case, 'status')
                    tests.append(test)
            
            norm_test[f.ALL_TESTS] = tests
    
        return norm_test

