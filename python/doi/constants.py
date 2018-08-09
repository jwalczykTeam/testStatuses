'''
Created on Aug 25, 2016

@author: alberto
'''

# Common Config Env Vars
ASTRO_GROUP_ID_ENV_VAR = "ASTRO_GROUP_ID"
ASTRO_DB_ENV_VAR = "ASTRO_DB"
ASTRO_MB_ENV_VAR = "ASTRO_MB"
ASTRO_SLACK_WEBHOOK_URL_ENV_VAR = "ASTRO_SLACK_WEBHOOK_URL"

# Syslog Config Env Var
ASTRO_SYSLOG_CONFIG_ENV_VAR = "ASTRO_SYSLOG_CONFIG"

# API Server Config Env Vars
ASTRO_API_CONFIG_ENV_VAR = "ASTRO_API_CONFIG"
ASTRO_API_KEY_ENV_VAR = "ASTRO_API_KEY"
ASTRO_BLUEMIX_API_ENDPOINT_ENV_VAR = "ASTRO_BLUEMIX_API_ENDPOINT"

# Scheduler Config Env Vars
ASTRO_SCHEDULER_CONFIG_ENV_VAR = "ASTRO_SCHEDULER_CONFIG"

# Worker Config Env Vars
ASTRO_WORKER_CONFIG_ENV_VAR = "ASTRO_WORKER_CONFIG"
ASTRO_WORKER_ID_ENV_VAR = "ASTRO_WORKER_ID"
ASTRO_OUT_DIR_ENV_VAR = "ASTRO_OUT_DIR"

# Scheduler resources
SCHEDULER_DB_NAME = "astro-scheduler"
SCHEDULER_CLIENT_ID = "astro-scheduler-client"
SCHEDULER_GROUP_ID = "astro-scheduler-group"
SCHEDULER_MESSAGES_TOPIC_NAME = "astro-scheduler-messages"

# API resources
API_CLIENT_ID = "astro-api-client"

# Worker resources
WORKERS_MESSAGES_TOPIC_NAME = "astro-workers-messages"

#Resource DB resources
RESOURCE_DB_EVENTS_TOPIC_NAME = "astro-resource-db-events"
RESOURCE_DB_CLIENT_ID = "astro-resource-db-client"
DEFAULT_GROUP_ID = "default"

# Authentication headers
ASTRO_AUTH_TENANT_ID = 'X-Auth-Tenant-Id'
ASTRO_AUTH_TOKEN = 'X-Auth-Token'
ASTRO_AUTH_USERNAME = 'X-Auth-Username'

# Resource Types
JOB_TYPE    = 'job'
WORKER_TYPE = 'worker'
SCHEDULER_TYPE = 'scheduler'
HISTORY_TYPE = 'history'

# Job Types
PROJECT_JOB_TYPE = 'project'    

# Worker constants
MAX_RETRY_NUMBER = 5
MAX_RUNNING_TASK_COUNT = 4

# Manager resource names
def get_scheduler_db_name(group_id):
    return SCHEDULER_DB_NAME + '@' + group_id

def get_scheduler_client_id(group_id):        
    return SCHEDULER_CLIENT_ID + '.' + group_id

def get_scheduler_group_id(group_id):        
    return SCHEDULER_GROUP_ID + '.' + group_id

def get_scheduler_messages_topic_name(group_id):        
    return SCHEDULER_MESSAGES_TOPIC_NAME + '.' + group_id
            

# Workers resource names
def get_worker_client_id(group_id, worker_id):        
    return worker_id + "-client." + group_id
             
def get_worker_group_id(group_id, worker_id):        
    return worker_id + "-group." + group_id

def get_workers_messages_topic_name(group_id):
    return WORKERS_MESSAGES_TOPIC_NAME + '.' + group_id
            

# API resource names
def get_api_client_id(group_id):        
    return API_CLIENT_ID + '.' + group_id

# Resource DB resource names
def get_resource_db_client_id(group_id):
    return RESOURCE_DB_CLIENT_ID + "." + group_id        

def get_resource_db_events_topic_name(group_id):
    return RESOURCE_DB_EVENTS_TOPIC_NAME + '.' + group_id
        