'''
Created on Nov 7, 2016

@author: adminibm
'''

import os
import unittest
from doi_test.util.common import load_data_file
from doi_test.util.factory import get_astro_instance

def _get_astro_for_test(test):
    name = __name__.split('.')[-1]
    conffile = os.path.join(os.path.dirname(__file__), 'data', name)
    conf = load_data_file(conffile)

    return (get_astro_instance(conf, name, test.__class__.__name__), conf)



class SmokeTest(unittest.TestCase):

    def setUp(self):
        '''
        Deploy a new astro instance
        '''
        print ">> setUp"
        astro, conf = _get_astro_for_test(self)

        astro.build()
        astro.start(num_workers=2)

        self.astro = astro
        self.project_to_add = conf['project_to_add']


    def tearDown(self):
        print ">> tearDown"
        self.astro.tear_down()


    def testMineProject(self):
        '''
        End-to-end test adding a project, running to completion, and deleting it.
        WARNING: performing the test as admin rather than a regular Bluemix user.
        This to avoid the problem of passing a stage1 Bluemix user validating against
        Bluemix YP and vice versa.
        '''
        api = self.astro.astro_api()
        project_name = self.project_to_add['name']

        print ">> Using API endpoint {}".format(api.api_ep)

        print ">> Ensure the project {} is not there".format(project_name)
        status, _ = api.get_project(project_name, as_admin=True)
        if status == 200:
            print '>> Delete the project and wait it is removed from the system'
            status, data = api.delete_project(project_name, as_admin=True)
            '''
            409 is conflict, which means that the project is already deleting,
            so just wait it is removed from the db.
            '''
            assert status in [202, 409], 'status: {}, data: {}'.format(status, data)

            for transition in api.wait_project_on_http_status_change(project_name, [404], poll=10, timeout=300, as_admin=True):
                print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        print ">> Add project {}".format(project_name)
        status, data = api.post_project(self.project_to_add, as_admin=True)
        assert status == 202, 'status: {}, data: {}'.format(status, data)

        print ">> Mine to completion, which may take some time, depending on the project size ..."
        for transition in api.wait_project_on_property_change(project_name, 'current_state', ['successful', 'permanently_failed'], as_admin=True):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])

        assert transition[1] == 'successful', 'Project mining failed'

        print '>> Verify you can query data. Fetch one commit'
        status, data = api.do_request(method='GET', path='/projects/{}/query?size=1&resource_type=commit'.format(project_name), as_admin=True)
        assert status == 200, 'status: {}, data: {}'.format(status, data)
        assert len(data) == 1, 'One commit doc expected, found {}'.format(len(data))

        print '>> Delete the project and wait it is removed from the system'
        status, data = api.delete_project(project_name, as_admin=True)
        assert status == 202, 'status: {}, data: {}'.format(status, data)

        for transition in api.wait_project_on_http_status_change(project_name, [404], poll=10, timeout=300, as_admin=True):
            print "\tTick (from {}, to {} in {} seconds)".format(transition[0], transition[1], transition[2])
