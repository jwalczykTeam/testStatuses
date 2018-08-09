'''
Created on Nov 7, 2016

@author: adminibm
'''

import unittest, logging, sys, os
from doi.util.db.elasticsearch_util import ElasticsearchClient


logger = logging.getLogger("astro")
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler(sys.stdout)
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)


class ElasticsearchTest(unittest.TestCase):

    def setUp(self):
        self.es = ElasticsearchClient({
                "hosts": [ os.environ.get('ES_API', 'http://jetsons.rtp.raleigh.ibm.com:9200') ]
        })

    def tearDown(self):
        pass

    def testQueryWithFields(self):
        results = self.es.query(db_name=os.environ.get('ES_INDEX', 'barabba'), doc_type='commit', size=1, fields='  commit_id ,     commit_committer_name ')
        print 'Results of query with fields: {}'.format(results)
        assert len(results)==1
        assert 'commit_id' in results[0]
        assert 'commit_committer_name' in results[0]

    def testQueryNoFields(self):
        results = self.es.query(db_name=os.environ.get('ES_INDEX', 'barabba'), doc_type='commit', size=1)
        print 'Results of query no fields: {}'.format(results)
        assert len(results)==1
        assert 'commit_id' in results[0]
        assert 'commit_committer_name' in results[0]

    def testQueryLinkedCommit(self):
        results = self.es.query(db_name=os.environ.get('ES_INDEX', 'barabba'), doc_type='commit', size=1, where='+_exists_: commit_issues')
        print 'Results of query complex: {}'.format(results)
        assert len(results)==1
        assert 'commit_id' in results[0]
        assert 'commit_issues' in results[0] and len(results[0]['commit_issues'])>0

    def testQueryNotLinkedCommit(self):
        results = self.es.query(db_name=os.environ.get('ES_INDEX', 'barabba'), doc_type='commit', size=1, where='-_exists_: commit_issues')
        print 'Results of query complex: {}'.format(results)
        assert len(results)==1
        assert 'commit_id' in results[0]
        assert 'commit_issues' in results[0] and len(results[0]['commit_issues'])==0
