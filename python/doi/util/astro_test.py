'''
Created on Feb 11, 2017

@author: alberto
'''

from doi.scheduler.common import Job
from doi.util import log_util
from doi.util import rest
import argparse
import json
import logging
import os
import sys
import time


class AstroTest(object):
    def __init__(self, api_base_url, api_key=None):
        self.rest_client = rest.RestClient()
        self.api_base_url = api_base_url
        self.api_key = api_key
    
    def _get_project(self, project_path):
        with open(project_path, 'r') as f:
            return json.loads(f.read())
        
    def create_projects(self, project_path, n):
        project = self._get_project(project_path)
        project_name = project['name']
        request_url = "{}/{}".format(self.api_base_url, "projects")
        headers = self._get_headers()    
        for i in xrange(1, n +1):
            project['name'] = project_name + str(i)
            data = json.dumps(project)
            self.rest_client.post(request_url, data, headers)
            print "Posted '{}' project to '{}'...".format(project['name'], request_url)
            time.sleep(5)
            
    def update_projects(self, project_path, n):
        pass
    
    def delete_projects(self, project_path, n):
        project = self._get_project(project_path)
        project_name = project['name']
        headers = self._get_headers()    
        for i in xrange(1, n +1):
            request_url = "{}/{}/{}{}".format(self.api_base_url, "projects", project_name, i)
            self.rest_client.delete(request_url, headers)
            print "Deleted project '{}'...".format(request_url)
            time.sleep(5)

    def verify_projects(self, project_path, n):
        project = self._get_project(project_path)
        project_name = project['name']
        headers = self._get_headers()    
        for i in xrange(1, n +1):
            request_url = "{}/{}/{}{}".format(self.api_base_url, "projects", project_name, i)
            response = self.rest_client.get(request_url, headers)
            job = Job.decode(json.loads(response.content))
            print "Verifying project '{}'...".format(job.name)
            for task in job.get_tasks():
                if task.started_count != 1:
                    print("Found task with started_count greater than 1: job_name={}, task_id={}, "
                          "started_count={}".format(
                              job.name, task.id, task.started_count))
                if task.started_count != task.ended_count:
                    print("Found task with started_count different from ended_count: job_name={}, task_id={}, "
                          "started_count={}, ended_count={}".format(
                              job.name, task.id, task.started_count, task.ended_count))

    def report_job_states(self):
        headers = self._get_headers()    
        request_url = "{}/{}".format(self.api_base_url, "projects")
        response = self.rest_client.get(request_url, headers)
        count_by_failed_task_id = {}
        for job_obj in json.loads(response.content):
            job_url = job_obj['url']
            response = self.rest_client.get(job_url, headers)
            job = Job.decode(json.loads(response.content))
            
            if job.started_at and job.started_at[0:10] not in ['2017-02-22', '2017-02-23']:
                continue

            if job.current_state != 'permanently_failed':
                continue
                                    
            print("Job url: {}".format(job_url))
            print("Job started_at: {}".format(job.started_at))
            print("Job state: {}".format(job.current_state))
            for task in job.get_tasks():
                if task.current_state == 'permanently_failed':
                    print("Failed task: {}".format(task.id))
                    if task.id not in count_by_failed_task_id:
                        count_by_failed_task_id[task.id] = 0
                    count_by_failed_task_id[task.id] += 1
                    
                    print("Task error: {}".format(
                        self._get_last_line(task.last_error.detail)))
            print("--------")
        
        print("Count of failed task by id:")
        for task_id in count_by_failed_task_id:
            print("task: {}, count={}".format(task_id, count_by_failed_task_id[task_id]))
            
    def _get_last_line(self, text):
        lines = text.split('\n')
        if len(lines) > 1:
            return lines[0]
        return text
        
    def _get_headers(self):
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        if self.api_key is not None:
            headers['X-Auth-Token'] = self.api_key
        return headers


if __name__ == '__main__':
    logger = log_util.get_logger("astro")
    logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler(sys.stdout)
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)
        
    parser = argparse.ArgumentParser(description='Create, update and delete a project n times.')
    parser.add_argument('action', help='the action to execute')
    parser.add_argument('--project_path', help='the project descriptor file path')
    parser.add_argument('--api_base_url', default="http://localhost:8080", help='the astro api base url')
    parser.add_argument('--api_key', help='the astro api key')
    parser.add_argument('--n', type=int, help='the number of times the action is repeated (default: 10)')
    args = parser.parse_args()
    
    if args.action not in ['create', 'update', 'delete', 'verify', 'report-all']:
        print "Invalid action: {}".format(args.action)
        print "Valid actions are: create, update, delete and verify"
        exit(1)
    
    if args.project_path:
        if not os.path.exists(args.project_path):
            print "File not found: {}".format(args.project_path)
            exit(1)

    if args.n:    
        n = int(args.n)
        if n < 1 or n > 100:
            print "N must be a positive integer not greater than 100: {}".format(args.n)
            exit(1)

    astro_test = AstroTest(args.api_base_url, args.api_key)
    if args.action == 'create':
        astro_test.create_projects(args.project_path, n)
    elif args.action == 'update':
        astro_test.update_projects(args.project_path, n)
    elif args.action == 'delete':
        astro_test.delete_projects(args.project_path, n)
    elif args.action == 'verify':
        astro_test.verify_projects(args.project_path, n)
    elif args.action == 'report-all':
        astro_test.report_job_states()
        
    exit(0)
    