'''
Created on Feb 8, 2016

@author: alberto
'''

import os
import sys
sys.path.insert(0, os.getcwd())

import threading
from doi import constants
from doi.scheduler import common
from doi.jobs.miner import miner
from doi.util import cf_util
from doi.util import file_util
from doi.util import log_util
import ConfigParser
import json
import logging


logger = log_util.get_logger("astro")
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler(sys.stdout)
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)

def validate_all(conf_dir):
    for conf_dir_path, _, conf_file_names in os.walk(conf_dir):
        for conf_file_name in conf_file_names:
            if conf_file_name.endswith(".conf"):                
                conf_file_path = os.path.join(conf_dir_path, conf_file_name)
                # just check it is a valid json for now ...
                with open(conf_file_path) as f:
                    print("Validating {} ...".format(conf_file_path))
                    json.load(f)
                    print("'{}' OK".format(conf_file_path))


def mine_all(project_config_file_names=None):
    config = ConfigParser.ConfigParser()    
    config.read("mine_all.properties")
    project_configs_dir = config.get("Astro", "ProjectConfigDir")
    project_configs_dir = os.path.abspath(project_configs_dir)
    output_dir = config.get("Astro", "OutputDir")
    mine_current_data = config.getboolean("Astro", "MineCurrentData")
    mine_future_data = config.getboolean("Astro", "MineFutureData")

    os.environ[constants.ASTRO_DB_ENV_VAR] = json.dumps(cf_util.get_db_credentials())    
    os.environ[constants.ASTRO_MB_ENV_VAR] = json.dumps(cf_util.get_mb_credentials())
    os.environ[constants.ASTRO_OUT_DIR_ENV_VAR] = config.get("Astro", "OutputDir")
    
    project_config_file_names = ([os.path.join(project_configs_dir, x) for x in project_config_file_names] 
        if project_config_file_names is not None 
        else file_util.get_dir_file_paths(project_configs_dir))
    for project_config_file_name in project_config_file_names:
        if not project_config_file_name.endswith(".json"):
            continue

        project_file_path = os.path.join(project_configs_dir, project_config_file_name)
        with open(project_file_path) as f:
            print("Fetching {} ...".format(project_file_path))
            project = json.load(f)

            params = common.ExecuteTaskMsg("1",project['name'],"1","create",None,None,{ "sources": project['sources'] },None,"1")

            # miner.mine_github_commits(params, None, threading.Event())

            miner.mine_gitlab_commits(params, None, threading.Event())
            # miner.mine_gitlab_branches(params, None, threading.Event())
            # miner.mine_gitlab_issues(params, None, threading.Event())
            # miner.mine_gitlab_contributors(params, None, threading.Event())

            
if len(sys.argv) != 1:
    print("usage: python mine_all.py")
else:
    #mine_all(["vue.json"]) 
    #mine_all(["astro.json"]) 
    #mine_all(["rudi.json"])     
    #mine_all(["doi-writers.json"])     
    #mine_all(["collab.json"]) 
    #mine_all(["kafka.json"]) 
    #mine_all(["swift.json"]) 
    #mine_all(["jetsons.json"])     
    #mine_all(["otc-ui.json"])     
    #mine_all(["avocado.json"])     
    mine_all(["spamnesty.json"]) 
    #mine_all(["emaas_test.json"])     
    #mine_all(["special_chars_in_file_path.json"])     
    #mine_all(["spark.json"]) 
    