'''
Created on Jan 10, 2017

@author: alberto
'''

from doi.jobs.miner import git2repo 
import doi.jobs.miner.fields as f
import pprint
import time

'''
GIT_URL = "https://github.ibm.com/ibmdevopsadtech/astro.git"
GIT_DIR = "/tmp/astro/astro@alberto/in/github.ibm.com/ibmdevopsadtech/astro.git"
GIT_BRANCH = "master"
'''
'''
GIT_URL = "https://github.ibm.com/apache/kafka.git"
GIT_DIR = "/tmp/astro/kafka@alberto/in/github.com/apache/kafka.git"
GIT_BRANCH = "trunk"
'''

GIT_URL = "https://github.ibm.com/apache/cassandra.git"
GIT_DIR = "/home/alberto/dev/packages/cassandra"
GIT_BRANCH = "trunk"



#print "cloning repo '{}' to '{}' ...".format(GIT_URL, GIT_DIR) 
#reader = gitrepo.GitReader(GIT_DIR)
#reader.clone(GIT_URL, "37a6bf182a1e53c5d42e642b0bf73cb2f377a06f")
#print "done"


print "mining commits ..." 

n = 0
start_time = time.time()
commit_ids = []
commit_reader = git2repo.GitCommitsReader(GIT_DIR, GIT_BRANCH)
for commits in commit_reader:
    for commit in commits:
        commit_ids.append(commit[f.COMMIT_ID])
        n += 1

print "total commits: {}".format(n)

print "mining committed files ..." 

n = 0
pp = pprint.PrettyPrinter(indent=2)
committed_files_reader = git2repo.GitCommittedFilesReader(GIT_DIR, commit_ids, mine_blame_info=True)
for committed_files in committed_files_reader:
    for committed_file in committed_files:
#        pp.pprint(committed_file)
        n += 1
end_time = time.time()

print "total committed files: {}".format(n)

print "elapsed time: {}".format(end_time - start_time)
