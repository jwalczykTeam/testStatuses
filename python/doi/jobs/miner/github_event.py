'''
Created on Jun 21, 2016

@author: alberto
'''


'''
GitHub source event examples":

{
  "payload": {
    "size": 1, 
    "head": "d98fb19c18f0122f335e5d810a2f8ff752b98d86", 
    "commits": [
      {
        "distinct": true, 
        "sha": "d98fb19c18f0122f335e5d810a2f8ff752b98d86", 
        "message": "[SPARK-15606][CORE] Use non-blocking removeExecutor call to avoid deadlocks\n\n## What changes were proposed in this pull request?\nSet minimum number of dispatcher threads to 3 to avoid deadlocks on machines with only 2 cores\n\n## How was this patch tested?\n\nSpark test builds\n\nAuthor: Pete Robbins <robbinspg@gmail.com>\n\nCloses #13355 from robbinspg/SPARK-13906.", 
        "url": "https://api.github.com/repos/apache/spark/commits/d98fb19c18f0122f335e5d810a2f8ff752b98d86", 
        "author": {
          "email": "robbinspg@gmail.com", 
          "name": "Pete Robbins"
        }
      }
    ], 
    "distinct_size": 1, 
    "push_id": 1169467977, 
    "ref": "refs/heads/branch-1.6", 
    "before": "abe36c53d126bb580e408a45245fd8e81806869c"
  }, 
  "created_at": "2016-06-21T21:43:14Z", 
  "actor": {
    "url": "https://api.github.com/users/asfgit", 
    "display_login": "asfgit", 
    "avatar_url": "https://avatars.githubusercontent.com/u/1341245?", 
    "gravatar_id": "", 
    "login": "asfgit", 
    "id": 1341245
  }, 
  "id": "4176918758", 
  "repo": {
    "url": "https://api.github.com/repos/apache/spark", 
    "id": 17165658, 
    "name": "apache/spark"
  }, 
  "org": {
    "url": "https://api.github.com/orgs/apache", 
    "login": "apache", 
    "avatar_url": 
    "https://avatars.githubusercontent.com/u/47359?", 
    "id": 47359, 
    "gravatar_id": ""
  }, 
  "type": "PushEvent", 
  "public": true
}
'''

from doi.jobs.miner import fields as f
from doi.jobs.miner.resourcedb import ResourceDB
from doi.jobs.miner import github 
from doi.jobs.miner import pipeline
from doi.util import file_util
from doi.util import log_util
from doi.util import json_util
from doi.util import time_util
import json


logger = log_util.get_logger("astro.jobs.miner.github")


class GitHubEventsReader(github.GitHubReader):
    def __init__(self, api_base_url, access_token, owner, repo, since, from_page=1, to_page=0, per_page=100):
        self.repo_uri = github.get_repo_uri(api_base_url, owner, repo)
        events_uri =  github.GitHubClient.EVENTS_URI_FORMAT.format(self.repo_uri)
        super(GitHubEventsReader, self).__init__(events_uri, access_token, 
                                                 False, 
                                                 since,
                                                 from_page, to_page, per_page)
        self.last_event_time = self.since
        self._done = False

    def next(self):
        if self._done:
            raise StopIteration
        
        page = super(GitHubEventsReader, self).next()
        
        events = []
        for event in page:
            if 'repo' in event:
                event_repo_uri = event['repo']['url']
                if event_repo_uri != self.repo_uri:
                    event_type = event.get('type')
                    event_id = event.get('id')
                    event_created = event.get('created_at')
                    logger.warning("Data consistency error, asked event for repo '%s' and received for repo '%s': event_type='%s', event_id='%s', timestamp='%s'",
                        self.repo_uri, event_repo_uri, 
                        event_type, event_id, event_created)
                    continue
                
            event_date = event.get('created_at')
            if event_date is None:
                continue
            
            event_time = time_util.iso8601_to_utc_millis(event_date)
            if event_time > self.last_event_time:
                self.last_event_time = event_time
                
            if self.since is not None and event_time < self.since:
                self._done = True            
                break
            
            events.append(event)
            
        return events


class GitHubIssueEventsNormalizer(pipeline.Normalizer):
    '''
    Convert issue events to commits
    '''

    def __init__(self, resource_db, label_field_map, publish):
        super(GitHubIssueEventsNormalizer, self).__init__()        
        self.resource_db = resource_db
        self.label_field_map = label_field_map
        self.publish = publish
        
    def transform(self, in_events):
        out_events = []
        for in_event in in_events:
            in_event_type = in_event.get('type')
            if in_event_type is None:
                continue
            
            if in_event_type == 'IssuesEvent':                
                in_payload = in_event['payload']
                if in_payload['action'] == 'opened':
                    out_event = self.normalize_issue_event(in_event)
                    if out_event is not None:
                        out_events.append(out_event)

        return out_events

    def normalize_issue_event(self, in_event):
        created_at = in_event['created_at']
        timestamp = time_util.iso8601_to_utc_millis(created_at)
        payload = in_event['payload']
        in_issue = payload['issue']
        in_issue_uri = in_issue['url']
    
        # Skip if this issue exists in resourcedb
        if self.resource_db.exists(ResourceDB.ISSUE_TYPE.name, { f.ISSUE_URI: in_issue_uri }):
            return None
        
        logger.info("New issue event: timestamp='%s', uri='%s'", created_at, in_issue_uri)
        out_issue = github.normalize_issue(in_issue, self.label_field_map, timestamp)
        out_issue_event = {
            'action': 'created',
            'timestamp': timestamp,
            'resource_type' : ResourceDB.ISSUE_TYPE.name,
            'resource' : out_issue,
            'publish': self.publish 
        }
        
        return out_issue_event
    

class GitHubPushEventsNormalizer(pipeline.Normalizer):
    '''
    Convert push events to commits
    '''

    def __init__(self, client, resource_db, branch_name, 
                 issue_key_finders, publish, file_black_list=None):
        super(GitHubPushEventsNormalizer, self).__init__()        
        self.client = client
        self.resource_db = resource_db
        self.branch_name =  branch_name
        self.issue_key_finders = issue_key_finders
        self.publish = publish
        self.file_black_list = file_black_list
        
    def transform(self, in_events):
        out_events = []
        for in_event in in_events:
            in_event_type = in_event.get('type')
            if in_event_type is None:
                continue
            
            if in_event_type == 'PushEvent':
                events = self.normalize_push_event(in_event)
                out_events.extend(events)

        return out_events

    def normalize_push_event(self, in_event):
        out_events = []    
        in_payload = in_event['payload']
        in_ref = in_payload['ref']
        if not in_ref.endswith('/' + self.branch_name):
            return []
        
        created_at = in_event['created_at']
        timestamp = time_util.iso8601_to_utc_millis(created_at)
        in_event_commits = in_payload['commits']
        for in_event_commit in in_event_commits:
            in_commit_uri = in_event_commit['url']
            
            # Skip if this commit exists in resourcedb
            if self.resource_db.exists(ResourceDB.COMMIT_TYPE.name, 
                                      { f.COMMIT_URI: in_commit_uri }):
                continue        
            
            response = self.client.get(in_commit_uri)
            in_commit = json.loads(response.content)
            
            logger.info("New commit event: timestamp='%s', uri='%s', ref='%s'", created_at, in_commit_uri, in_ref)
            
            # Add commit created event
            out_commit = github.normalize_commit(in_commit, self.issue_key_finders, timestamp)
            out_commit_created_event = {
                'action': 'created',
                'timestamp': timestamp,
                'resource_type' : ResourceDB.COMMIT_TYPE.name,
                'resource' : out_commit,
                'publish': self.publish 
            }
            
            out_events.append(out_commit_created_event)
    
            # Add committed files created events
            if 'files' in in_commit:
                for in_committed_file in in_commit['files']:
                    if self.file_black_list and file_util.file_matches(
                            in_committed_file['filename'], self.file_black_list):
                        logger.info("Skipped black listed file: '%s'", in_committed_file['filename'])
                        continue
                    
                    in_committed_file['lines'] = github.get_file_line_count(self.client, in_committed_file)                    
                    out_committed_file = github.normalize_committed_file(in_committed_file, out_commit)
                    out_committed_file_created_event = {
                        'action': 'created',
                        'timestamp': timestamp,
                        'resource_type' : ResourceDB.COMMITTED_FILE_TYPE.name,
                        'resource' : out_committed_file,
                        'publish': False 
                    }
                    out_events.append(out_committed_file_created_event)
        
        return out_events


class GitHubIssuesEventsReader(github.GitHubReader):
    def __init__(self, api_base_url, access_token, owner, repo, since=None, from_page=1, to_page=0, per_page=100):
        issues_events_uri =  github.GitHubClient.ISSUES_EVENTS_URI_FORMAT.format(github.get_repo_uri(api_base_url, owner, repo))
        super(GitHubIssuesEventsReader, self).__init__(issues_events_uri, access_token, 
                                                       False, 
                                                       since,
                                                       from_page, to_page, per_page)


class GitHubIssuesEventsNormalizer(pipeline.Normalizer):
    def __init__(self):
        super(GitHubIssuesEventsNormalizer, self).__init__()        

    def transform(self, in_events):
        out_events = []
        for in_event in in_events:
            out_event = normalize_issue_event(in_event)
            out_events.append(out_event)            
        return out_events


def normalize_issue_event(in_event):
    out_event = {}        
    json_util.set_value(out_event, f.ISSUE_EVENT_URI,         in_event, 'url')
    json_util.set_value(out_event, f.ISSUE_EVENT_ISSUE_URI,    in_event, 'issue', 'url')        
    json_util.set_value(out_event, f.ISSUE_EVENT_TYPE,        in_event, 'event')
    json_util.set_value(out_event, f.ISSUE_EVENT_COMMIT_URI,   in_event, 'commit_url')
    json_util.set_time (out_event, f.ISSUE_EVENT_CREATED_TIME, in_event, 'created_at')
    return out_event
