'''
Created on Oct 10, 2016

@author: alberto
'''

from cStringIO import StringIO
import subprocess

git_dir = "/tmp/astro/astro/in/github.ibm.com/ibmdevopsadtech/astro.git"
commit_id = "80ec748dd930bc556c991390453ae7aaf05c4c1d"

proc = subprocess.Popen(['git', 'show', '-M', 
                         '--pretty=format:%H', 
                         commit_id], 
                        cwd=git_dir,
                        stdout=subprocess.PIPE)
line = proc.stdout.readline().rstrip()
print("commit_id={}".format(line))

buff = None
for line in proc.stdout:
    if line.startswith("diff --git "):
        if buff is not None:
            print("DIFF")
            print(buff.getvalue())
        buff = StringIO()
    buff.write(line)
    
if buff is not None:
    print("DIFF")
    print(buff.getvalue())
