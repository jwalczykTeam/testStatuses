'''
Created on Nov 1, 2016

@author: alberto
'''

from doi.util.file_util import escape_git_diff_path

print(escape_git_diff_path("content/tutorials/tutorial_gm_advocate_otc_step3.md \t"))
print(escape_git_diff_path("envs/dev-mon01/ \tradiant40-dp.yml"))