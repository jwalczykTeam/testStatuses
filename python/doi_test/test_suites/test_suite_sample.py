'''
Created on Nov 7, 2016

@author: adminibm
'''

import os, unittest, time, json, urllib2, random
from doi_test.util.common import load_data_file, repeat_request_until
from doi_test.util.factory import get_astro_instance


def _get_astro_for_test(test):
    name = __name__.split('.')[-1]
    conffile = os.path.join(os.path.dirname(__file__), 'data', name)
    conf = load_data_file(conffile)

    return (get_astro_instance(conf, name, test.__class__.__name__), conf)



class MineProject(unittest.TestCase):
    '''
    Example of local test:
    - mine project to the end
    - verify query, stop and restart
    - finally delete
    '''

    def setUp(self):
        '''
        Deploy a new astro instance
        '''
        print ">> setUp"
        astro, conf = _get_astro_for_test(self)

        astro.build()
        astro.start(num_api_servers=1, num_workers=1)

        self.astro = astro
        self.project_to_add = conf['project_to_add']


    def tearDown(self):
        print ">> tearDown"
        self.astro.tear_down()


    def testMineProject(self):
        api = self.astro.astro_api()
        project_name = self.project_to_add['name']

        print ">> Check projects list empty"
        status, data = api.list_projects()
        assert status == 200, 'status: {}, data: {}'.format(status, data)
        assert len(data) == 0, 'Projects list should be empty. Found: {}'.format(data)

        print ">> Add new project"
        status, data = api.post_project(self.project_to_add)
        assert status == 202, 'status: {}, data: {}'.format(status, data)

        print ">> Check project exists"
        for transition in api.wait_project_on_http_status_change(project_name, [200], poll=2, timeout=300):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print ">> Mine to completion, which may take some time, depending on the project size ..."
        for transition in api.wait_project_on_property_change(project_name, 'current_state', ['successful']):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print '>> Verify you can query data. Fetch one commit'
        status, data = api.do_request(method = 'GET', path = '/projects/{}/query?size=1&resource_type=commit'.format(project_name))
        assert status == 200, 'status: {}, data: {}'.format(status, data)
        assert len(data) == 1, 'One commit doc expected, found {}'.format(len(data))

        print '>> Delete the project and wait it is removed from the system'
        status, data = api.delete_project(project_name)
        assert status == 202, 'status: {}, data: {}'.format(status, data)

        for transition in api.wait_project_on_http_status_change(project_name, [404], poll=10, timeout=300):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])


class WorkerTakeover(unittest.TestCase):

    def setUp(self):
        '''
        Deploy a new astro instance
        '''
        print ">> setUp"
        astro, conf = _get_astro_for_test(self)

        astro.build()
        astro.start(num_api_servers=1, num_workers=2)

        self.astro = astro
        self.project_to_add = conf['project_to_add']


    def tearDown(self):
        print ">> tearDown"
        self.astro.tear_down()


    def testWorkerTakeover(self):
        api = self.astro.astro_api()
        project_name = self.project_to_add['name']

        print ">> Add new project"
        status, data = api.post_project(self.project_to_add)
        assert status == 202, 'status: {}, data: {}'.format(status, data)

        print ">> Check project exists"
        for transition in api.wait_project_on_http_status_change(project_name, [200], poll=2, timeout=30):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print ">> Start mining the project, wait it starts, and kill the mining worker ..."
        for transition in api.wait_project_on_property_change(project_name, 'current_state', ['running'], timeout=120):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print '>> Get the worker of the project'
        status, data = api.get_project(project_name)
        assert status == 200, 'status: {}, data: {}'.format(status, data)

        # Get the first worker_id from tasks
        for task in data['tasks']:
            worker_id = task.get('worker_id', None)
            if worker_id:
                break

        assert worker_id, 'worker_id not set'

        print '>> Worker is {}. Kill it!'.format(worker_id)
        assert self.astro.worker_alive(worker_id), 'Worker {} is expected to be alive'.format(worker_id)
        self.astro.kill_worker(worker_id)
        assert not self.astro.worker_alive(worker_id), 'Worker {} is expected to be dead'.format(worker_id)

        print '>> Wait that the project is taken by the other worker, and mine to the end, which may take time ...'
        for transition in api.wait_project_on_property_change(project_name, 'current_state', ['successful'], timeout=600):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print '>> Verify you can query data. Fetch one commit'
        status, data = api.do_request(method = 'GET', path = '/projects/{}/query?size=1&resource_type=commit'.format(project_name))
        assert status == 200, 'status: {}, data: {}'.format(status, data)
        assert len(data) == 1, 'Expected one element in data. Found: {}'.format(data)

        print '>> Delete the project and wait it is removed from the system'
        status, _ = api.delete_project(project_name)
        assert status == 202, 'status: {}, data: {}'.format(status, data)

        for transition in api.wait_project_on_http_status_change(project_name, [404], poll=10, timeout=300):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])


class SchedulerCrashes(unittest.TestCase):

    def setUp(self):
        '''
        Deploy a new astro instance
        '''
        print ">> setUp"
        astro, conf = _get_astro_for_test(self)

        astro.build()
        astro.start(num_api_servers=1, num_workers=1)

        self.astro = astro
        self.project_to_add = conf['project_to_add']


    def tearDown(self):
        print ">> tearDown"
        self.astro.tear_down()


    def testSchedulerCrashes(self):
        api = self.astro.astro_api()
        project_name = self.project_to_add['name']

        print ">> Add new project"
        status, data = api.post_project(self.project_to_add)
        assert status == 202, 'status: {}, data: {}'.format(status, data)

        print ">> Check project exists"
        for transition in api.wait_project_on_http_status_change(project_name, [200], poll=2, timeout=300):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print ">> Start mining the project, wait it starts, and kill the scheduler ..."
        for transition in api.wait_project_on_property_change(project_name, 'current_state', ['running'], timeout=600):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print '>> Get the worker of the project'
        status, data = api.get_project(project_name)
        assert status == 200, 'status: {}, data: {}'.format(status, data)

        # Get the first worker_id from tasks
        for task in data['tasks']:
            worker_id = task.get('worker_id', None)
            if worker_id:
                break

        assert worker_id, 'worker_id not set'
        print ">> Worker {} is running the job".format(worker_id)

        print ">> Kill the scheduler, dude!"
        self.astro.kill_scheduler()
        assert not self.astro.scheduler_alive(), 'Driver is expected to be dead'

        print ">> Wait 2 minutes for mining to complete ..."
        time.sleep(120)

        print ">> Restart the scheduler ..."
        self.astro.start_scheduler()

        print '>> Ensure the project is there in the db'
        status, data = api.get_project(project_name)
        assert status == 200, 'status: {}, data: {}'.format(status, data)

        print '>> Mine to the end, which may take time ...'
        for transition in api.wait_project_on_property_change(project_name, 'current_state', ['successful'], timeout=600):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print '>> Verify you can query data. Fetch one commit'
        status, data = api.do_request(method = 'GET', path = '/projects/{}/query?size=1&resource_type=commit'.format(project_name))
        assert status == 200, 'status: {}, data: {}'.format(status, data)
        assert len(data) == 1, 'Expected one element in data. Found {}'.format(data)

        print '>> Delete the project and wait it is removed from the system'
        status, data = api.delete_project(project_name)
        assert status == 202, 'status: {}, data: {}'.format(status, data)

        print ">> Kill the scheduler again, dude!"
        self.astro.kill_scheduler()
        assert not self.astro.scheduler_alive(), 'Driver is expected to be dead'

        print ">> Wait 2 minutes for worker deleting the project ..."
        time.sleep(60)

        print ">> Restart the scheduler ..."
        self.astro.start_scheduler()

        print '>> Wait for the project being removed from the db ...'
        for transition in api.wait_project_on_http_status_change(project_name, [404], poll=10, timeout=120):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])


class ConcurrentProjects(unittest.TestCase):

    def setUp(self):
        '''
        Deploy a new astro instance
        '''
        print ">> setUp"
        astro, conf = _get_astro_for_test(self)

        astro.build()
        astro.start(num_workers=10)

        self.astro = astro
        self.project_to_add = conf['project_to_add']


    def tearDown(self):
        print ">> tearDown"
        '''
        First delete all the projects, then tear down the instance
        '''
        self.astro.tear_down()


    def testConcurrentProjects(self):
        api = self.astro.astro_api()
        project_name = self.project_to_add['name']
        num_projects = 20

        print ">> Add {} projects and wait they complete".format(num_projects)
        project_names = [project_name + '-' + str(n) for n in range(num_projects)]

        for n in range(num_projects):
            self.project_to_add['name'] = project_names[n]
            status, data = api.post_project(self.project_to_add)
            assert status == 202, 'status: {}, data: {}'.format(status, data)

        print '>> Check there are num_projects projects added'
        def _num_projects_projects(status_data):
            status, data = status_data
            n = len(data)
            return (status == 200 and n == num_projects, n)

        @repeat_request_until(_num_projects_projects, poll=5, timeout=120)
        def all_projects_in_db():
            return api.list_projects()

        for t in all_projects_in_db():
            print "\tTick ({} projects in {} seconds)".format(t[1], t[0])

        projects_data = []
        aggs = {
            "by_type": {
                "terms": {
                    "field": "_type",
                    "size": 10000
                }
            }
        }
        for n in range(num_projects):
            print ">> Waiting for project {} to complete ...".format(project_names[n])
            for transition in api.wait_project_on_property_change(project_names[n], 'current_state', ['successful'], timeout=3600):
                print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

            status, data = api.do_request(method = 'POST', path = '/projects/{}/reports/distribution?size=0'.format(project_names[n]), body=aggs)
            assert status == 200, 'status: {}, data: {}'.format(status, data)
            projects_data.append(data)

        # Verify that all the projects have the same data!
        for n in range(9):
            a = projects_data[n]
            b = projects_data[n+1]
            assert a['hits']['total'] == b['hits']['total'], 'Different number of docs in projects {} and {}. {} != {}'.format(project_names[n],project_names[n+1], a, b)

            ab = a['aggregations']['by_type']['buckets']
            bb = b['aggregations']['by_type']['buckets']
            assert len(ab) == len(bb), 'Different number of doc types in projects {} and {}. {} != {}'.format(project_names[n],project_names[n+1], a, b)

            for i in range(len(ab)):
                assert ab[i] == bb[i], 'Different doc types in projects {} and {}. {} != {}'.format(project_names[n],project_names[n+1], a, b)

        print "Let's kill workers"
        wids = self.astro.get_workers()
        for wid in wids:
            print '>> Killing worker {}'.format(wid)
            self.astro.kill_worker(wid)

        print '>> Delete all the projects'
        for n in range(num_projects):
            status, data = api.delete_project(project_names[n])
            assert status == 202, 'status: {}, data: {}'.format(status, data)

        wid = random.choice(wids)
        print ">> Start only the worker {} and wait for projects being removed from the db".format(wid)
        assert not self.astro.worker_alive(wid), "Worker {} is expected to not be alive".format(wid)
        self.astro.start_worker(wid)
        print ">> Is worker alive? {}".format(self.astro.worker_alive(wid))

        for n in range(num_projects):
            print ">> Waiting for project {} being removed from the db ...".format(project_names[n])
            for transition in api.wait_project_on_http_status_change(project_names[n], [404], poll=10, timeout=1200):
                print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])


class ProjectAggregations(unittest.TestCase):

    def setUp(self):
        '''
        Deploy a new astro instance
        '''
        print ">> setUp"
        astro, conf = _get_astro_for_test(self)

        astro.build()
        astro.start(num_api_servers=1, num_workers=2)

        self.astro = astro
        self.project_to_add = conf['project_to_add']


    def tearDown(self):
        print ">> tearDown"
        self.astro.tear_down()


    def testProjectAggregations(self):
        api = self.astro.astro_api()
        project_name = self.project_to_add['name']

        print ">> Check projects list empty"
        status, data = api.list_projects()
        assert status == 200, 'status: {}, data: {}'.format(status, data)
        assert len(data) == 0, 'Projects list should be empty. Found: {}'.format(data)

        print ">> Add new project"
        status, data = api.post_project(self.project_to_add)
        assert status == 202, 'status: {}, data: {}'.format(status, data)

        print ">> Check project exists"
        for transition in api.wait_project_on_http_status_change(project_name, [200], poll=2, timeout=30):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print ">> Mine to completion, which may take some time, depending on the project size ..."
        for transition in api.wait_project_on_property_change(project_name, 'current_state', ['successful']):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        aggs = {
            "by_authors": {
                "terms": {
                    "field": "commit_author_name.raw",
                    "size": 10,
                    "order": {
                        "author_commits_sum": "desc"
                    }
                },
                "aggs": {
                    "author_commits_sum": {
                        "sum": {
                            "field": "commit_author_commit_count"
                        }
                    },
                    "by_committers": {
                        "terms": {
                            "field": "commit_committer_name.raw",
                            "size": 10,
                            "order": {
                                "committer_commits_sum": "desc"
                            }
                        },
                        "aggs": {
                            "committer_commits_sum": {
                                "sum": {
                                    "field": "commit_committer_commit_count"
                                }
                            }
                        }
                    }
                }
            }
        }

        print '>> Verify you can aggregate data using a POST /reports/distribution'
        status, data = api.do_request(method = 'POST', path = '/projects/{}/reports/distribution?resource_type=commit'.format(project_name), body=aggs)
        assert status == 200, 'status: {}, data: {}'.format(status, data)
        print '>> Aggregations results: {}'.format(json.dumps(data, indent=2))

        print '>> Verify you can search and aggregate data using a POST /reports/distribution'
        status, data = api.do_request(method = 'POST', path = urllib2.quote('/projects/{}/reports/distribution?resource_type=commit&query=commit_author_commit_count:[10 TO 20]'.format(project_name), '/:?&='), body=aggs)
        assert status == 200, 'status: {}, data: {}'.format(status, data)
        print '>> Aggregations results: {}'.format(json.dumps(data, indent=2))

        print '>> Verify you can aggregate data in a time series using a POST /reports/time_series'
        status, data = api.do_request(method = 'POST', path = '/projects/{}/reports/time_series?resource_type=commit&interval=day'.format(project_name), body=aggs)
        assert status == 200, 'status: {}, data: {}'.format(status, data)
        print '>> Aggregations results: {}'.format(json.dumps(data, indent=2))


class AstroHistory(unittest.TestCase):

    def setUp(self):
        '''
        Deploy a new astro instance
        '''
        print ">> setUp"
        astro, conf = _get_astro_for_test(self)

        astro.build()
        astro.start(num_api_servers=1, num_workers=2)

        self.astro = astro
        self.project_to_add = conf['project_to_add']


    def tearDown(self):
        print ">> tearDown"
        self.astro.tear_down()


    def testAstroHistory(self):
        api = self.astro.astro_api()
        project_name = self.project_to_add['name']

        print ">> Check projects list empty"
        status, data = api.list_projects()
        assert status == 200, 'status: {}, data: {}'.format(status, data)
        assert len(data) == 0, 'Projects list should be empty. Found: {}'.format(data)

        print ">> Add new project"
        status, data = api.post_project(self.project_to_add)
        assert status == 202, 'status: {}, data: {}'.format(status, data)

        print ">> Check project exists"
        for transition in api.wait_project_on_http_status_change(project_name, [200], poll=2, timeout=30):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print ">> Wait until Astro starts running a task ..."
        for transition in api.wait_project_on_property_change(project_name, 'current_state', ['running']):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        query_string = '*'
        size = 5

        print '>> Verify you can get history of first 5 messages'
        status, data = api.do_request(method = 'GET', path = '/history?size={}&where={}'.format(size, query_string), as_admin=True)
        assert status == 200 and len(data) == size, 'status: {}, data: {}'.format(status, data)

        query_string = 'job.current_state:waiting'

        print '>> Verify you can get job messages where current state is waiting'
        status, data = api.do_request(method = 'GET', path = '/history?where={}'.format(query_string), as_admin=True)
        assert status == 200 and len(data) > 0, 'status: {}, data: {}'.format(status, data)
        print '>> Number of messages where {}: {}'.format(query_string, len(data))

        query_string = 'job_name:astro*'

        print '>> Verify you can get all messages about astro project'
        status, data = api.do_request(method = 'GET', path = '/history?where={}'.format(query_string), as_admin=True)
        assert status == 200 and len(data) > 0, 'status: {}, data: {}'.format(status, data)
        print '>> Number of messages where {}: {}'.format(query_string, len(data))


class Deploy(unittest.TestCase):
    '''
    Just deploy and tear down
    '''

    def setUp(self):
        '''
        Deploy a new astro instance
        '''
        print ">> setUp"
        astro, _ = _get_astro_for_test(self)

        astro.build()
        astro.start(num_api_servers=2, num_workers=4)

        self.astro = astro


    def tearDown(self):
        print ">> tearDown"
        self.astro.tear_down()


    def testDeploy(self):
        api = self.astro.astro_api()

        print ">> Check projects list empty"
        status, data = api.list_projects()
        assert status == 200, 'status: {}, data: {}'.format(status, data)
        assert len(data) == 0, 'Projects list should be empty. Found: {}'.format(data)


class UpdateProject(unittest.TestCase):
    '''
    Example of local test:
    - mine project to the end
    - verify query, stop and restart
    - finally delete
    '''

    def setUp(self):
        '''
        Deploy a new astro instance
        '''
        print ">> setUp"
        astro, conf = _get_astro_for_test(self)

        astro.build()
        astro.start(num_api_servers=1, num_workers=1)

        self.astro = astro
        self.project_to_add = conf['project_to_add']


    def tearDown(self):
        print ">> tearDown"
        self.astro.tear_down()


    def testUpdateProject(self):
        api = self.astro.astro_api()
        project_name = self.project_to_add['name']

        print ">> Check projects list empty"
        status, data = api.list_projects()
        assert status == 200, 'status: {}, data: {}'.format(status, data)
        assert len(data) == 0, 'Projects list should be empty. Found: {}'.format(data)

        print ">> Add new project"
        status, data = api.post_project(self.project_to_add)
        assert status == 202, 'status: {}, data: {}'.format(status, data)

        print ">> Check project exists"
        for transition in api.wait_project_on_http_status_change(project_name, [200], poll=2, timeout=300):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print ">> Wait for running and then, update the project ..."
        for transition in api.wait_project_on_property_change(project_name, 'current_state', ['running']):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print '>> Update the project '
        updated_project = self.project_to_add.copy()
        label = { "label": "problem", "field": "issue_types", "value": "Defect" }
        updated_project['sources'][0]['labels_map'].append(label)
        status, data = api.update_project(project_name, updated_project)
        assert status == 202, 'status: {}, data: {}'.format(status, data)
        assert data['current_rev'] == 1, 'Unexpected current revision of the project: {}'.format(json.dumps(data, indent=2))
        assert data['desired_rev'] == 2, 'Unexpected desired revision of the project: {}'.format(json.dumps(data, indent=2))
        assert label in data['input']['sources'][0]['labels_map'], 'Cannot find label {} in project {}'.format(label, json.dumps(data, indent=2))

        print '>> Update the project again with the old one'
        status, data = api.update_project(project_name, self.project_to_add)
        assert status == 202, 'status: {}, data: {}'.format(status, data)
        assert data['current_rev'] == 2, 'Unexpected current revision of the project: {}'.format(json.dumps(data, indent=2))
        assert data['desired_rev'] == 3, 'Unexpected desired revision of the project: {}'.format(json.dumps(data, indent=2))
        assert label not in data['input']['sources'][0]['labels_map'], 'Unexpected label in project {}'.format(json.dumps(data, indent=2))

        print ">> Wait till the project mining completes ..."
        for transition in api.wait_project_on_property_change(project_name, 'current_state', ['successful']):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print '>> Get the project and verify the revision again'
        status, data = api.get_project(project_name)
        assert status == 200, 'status: {}, data: {}'.format(status, data)
        assert data['current_rev'] == 2, 'Unexpected current revision of the project: {}'.format(json.dumps(data, indent=2))
        assert data['desired_rev'] == 3, 'Unexpected desired revision of the project: {}'.format(json.dumps(data, indent=2))
        assert label not in data['input']['sources'][0]['labels_map'], 'Unexpected label in project {}'.format(json.dumps(data, indent=2))

        print '>> Delete the project and wait it is removed from the system'
        status, data = api.delete_project(project_name)
        assert status == 202, 'status: {}, data: {}'.format(status, data)

        for transition in api.wait_project_on_http_status_change(project_name, [404], poll=10, timeout=300):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])


class AddProjects(unittest.TestCase):
    '''
    Example of local test:
    - mine project to the end
    - verify query, stop and restart
    - finally delete
    '''

    def setUp(self):
        '''
        Deploy a new astro instance
        '''
        print ">> setUp"
        astro, conf = _get_astro_for_test(self)

        astro.build()
        astro.start(num_api_servers=1, num_workers=1)

        self.astro = astro
        self.project_to_add = conf['project_to_add']


    def tearDown(self):
        print ">> tearDown"
        self.astro.tear_down()


    def testAddProjects(self):
        api = self.astro.astro_api()
        project_name = self.project_to_add['name']

        print ">> Check projects list empty"
        status, data = api.list_projects()
        assert status == 200, 'status: {}, data: {}'.format(status, data)
        assert len(data) == 0, 'Projects list should be empty. Found: {}'.format(data)

        status = 202
        while status == 202:
            print ">> Add the same project two or more times"
            status, data = api.post_project(self.project_to_add)

        print ">> Mine to completion ..."
        for transition in api.wait_project_on_property_change(project_name, 'current_state', ['successful']):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print '>> Delete the project and wait it is removed from the system'
        status, data = api.delete_project(project_name)
        assert status == 202, 'status: {}, data: {}'.format(status, data)

        for transition in api.wait_project_on_http_status_change(project_name, [404], poll=10, timeout=300):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])


class QuiesceWorker(unittest.TestCase):
    '''
    Test that a quiescent worker does not get any more task,
    and that it completes its current work
    '''

    def setUp(self):
        '''
        Deploy a new astro instance
        '''
        print ">> setUp"
        astro, conf = _get_astro_for_test(self)

        astro.build()
        astro.start(num_api_servers=1, num_workers=1)

        self.astro = astro
        self.project_to_add = conf['project_to_add']


    def tearDown(self):
        print ">> tearDown"
        self.astro.tear_down()


    def testQuiesceWorker(self):
        api = self.astro.astro_api()
        project_name = self.project_to_add['name']

        print ">> Check projects list empty"
        status, data = api.list_projects()
        assert status == 200, 'status: {}, data: {}'.format(status, data)
        assert len(data) == 0, 'Projects list should be empty. Found: {}'.format(data)

        print ">> Add new project"
        status, data = api.post_project(self.project_to_add)
        assert status == 202, 'status: {}, data: {}'.format(status, data)

        print ">> Check project exists"
        for transition in api.wait_project_on_http_status_change(project_name, [200], poll=2, timeout=300):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print ">> Wait until the project is assigned to the worker (ready state) ..."
        for transition in api.wait_project_on_property_change(project_name, 'current_state', ['ready']):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print '>> Quiesce the worker'
        assert len(self.astro.get_workers()) == 1, 'Expected only one worker in the env'
        worker_id = self.astro.get_workers()[0]
        status, data = api.do_request(method='PATCH', path='/workers/{}'.format(worker_id), body={'quiescent': True}, as_admin=True)
        assert status == 202, 'Failed to quiesce the worker. status: {}, data: {}'.format(status, data)

        print ">> Wait until the worker is quiesced ..."
        def _check_worker_quiesced(status_data):
            status, data = status_data
            return (status == 200 and data['quiescent'] == True, data['quiescent'])

        @repeat_request_until(_check_worker_quiesced, poll=5, timeout=120)
        def worker_quiesced():
            return api.do_request(method='GET', path='/workers/{}'.format(worker_id), as_admin=True)

        for t in worker_quiesced():
            print "\tTick (quiescent set to {} in {} seconds)".format(t[1], t[0])

        print '>> Worker has been quiesced. Add another project to astro'
        new_project_name = self.project_to_add['name'] + '-1'
        self.project_to_add['name'] = new_project_name
        status, data = api.post_project(self.project_to_add)
        assert status == 202, 'status: {}, data: {}'.format(status, data)

        print ">> Check that new project exists"
        for transition in api.wait_project_on_http_status_change(new_project_name, [200], poll=2, timeout=300):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print ">> Verify that at least one task of the first project is not in waiting state ..."
        status, data = api.get_project(project_name)
        assert status == 200, 'Project {} does not exist'.format(project_name)
        for task in data['tasks']:
            if task['current_state'] != 'waiting':
                okk = True
                break
        assert okk, 'At least one task of the project {} should have been assigned to the worker {}'.format(project_name, worker_id)

        print ">> Verify that the new project remains in the wait state for at least 2 minutes ..."
        try:
            for transition in api.wait_project_on_property_change(new_project_name, 'current_state', ['ready'], timeout=120):
                print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])
        except AssertionError:
            print '>> Good, project remained in wait state since no workers can get new tasks'

        print '>> Resume the worker to accept new tasks'
        status, data = api.do_request(method='PATCH', path='/workers/{}'.format(worker_id), body={'quiescent': False}, as_admin=True)
        assert status == 202, 'Failed to resume the worker. status: {}, data: {}'.format(status, data)

        print ">> Wait until the worker is resumed ..."
        def _check_worker_resumed(status_data):
            status, data = status_data
            return (status == 200 and data['quiescent'] == False, data['quiescent'])

        @repeat_request_until(_check_worker_resumed, poll=5, timeout=120)
        def worker_resumed():
            return api.do_request(method='GET', path='/workers/{}'.format(worker_id), as_admin=True)

        for t in worker_resumed():
            print "\tTick (quiescent set to {} in {} seconds)".format(t[1], t[0])

        print ">> Verify that now the new project runs to completion ..."
        for transition in api.wait_project_on_property_change(new_project_name, 'current_state', ['successful']):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print ">> Verify that now the first project runs to completion ..."
        for transition in api.wait_project_on_property_change(project_name, 'current_state', ['successful']):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print '>> Delete the projects and wait they are removed from the system'
        status, data = api.delete_project(project_name)
        assert status == 202, 'status: {}, data: {}'.format(status, data)

        status, data = api.delete_project(new_project_name)
        assert status == 202, 'status: {}, data: {}'.format(status, data)

        for transition in api.wait_project_on_http_status_change(project_name, [404], poll=10, timeout=300):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        for transition in api.wait_project_on_http_status_change(new_project_name, [404], poll=10, timeout=300):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])
