'''
Cloud Foundry utils to parse VCAP_SERVICES for extracting Astro services
'''


from doi import constants
from doi.helpers.python.lib import get_creds
import os
import json


__MESSAGE_HUB_SERVICE = 'messagehub'
__ELASTICSEARCH_SERVICE = 'compose-elasticsearch'

__DEFAULT_MESSAGE_BROKER_CREDENTIALS = {
    "kafka": {
        "bootstrap.servers": "localhost:9092"
    }
}

__DEFAULT_DATABASE_CREDENTIALS = {
    "elasticsearch": {
        "hosts": ["localhost:9200"]
    }
}


'''
    "messagehub": {
        "mqlight_lookup_url": "https://mqlight-lookup-prod01.messagehub.services.us-south.bluemix.net/Lookup?serviceId=bb446386-ded8-4b26-b0f2-9a1afd545fad",
        "api_key": "JDTeeWPn15w0GRgfUc1ScfJhpL80Ula82mGh5RVmxSJ9t0hv",
        "kafka_admin_url": "https://kafka-admin-prod01.messagehub.services.us-south.bluemix.net:443",
        "kafka_rest_url": "https://kafka-rest-prod01.messagehub.services.us-south.bluemix.net:443",
        "kafka_brokers_sasl": [
            "kafka01-prod01.messagehub.services.us-south.bluemix.net:9093",
            "kafka02-prod01.messagehub.services.us-south.bluemix.net:9093",
            "kafka03-prod01.messagehub.services.us-south.bluemix.net:9093",
            "kafka04-prod01.messagehub.services.us-south.bluemix.net:9093",
            "kafka05-prod01.messagehub.services.us-south.bluemix.net:9093"
        ]
    }
'''

def get_mb_credentials(name=None):
    if constants.ASTRO_MB_ENV_VAR in os.environ:
        return json.loads(os.environ[constants.ASTRO_MB_ENV_VAR])

    credentials = get_creds.get_mh_credentials()
    if credentials:
        return {__MESSAGE_HUB_SERVICE: credentials}
    
    # Return default credentials on localhost
    return __DEFAULT_MESSAGE_BROKER_CREDENTIALS


'''
    "user-provided": [
        {
            "credentials": {
                "password": "****",
                "public_hostname": "sl-us-dal-9-portal1.dblayer.com:10550",
                "username": "jwalczyk%40us.ibm.com"
            },
            "label": "user-provided",
            "name": "Devops-Insights-Elasticsearch",
            "syslog_drain_url": "",
            "tags": []
       },
       {
            "credentials": {
                "password": "****",
                "port": "10738",
                "uri": "sl-us-dal-9-portal.2.dblayer.com",
                "user": "jwalczyk%40us.ibm.com"
            },
            "label": "user-provided",
            "name": "Devops-Insights-Mongo",
            "syslog_drain_url": "",
            "tags": []
        }
    ]

 '''

def get_db_credentials(name=None, vcap_path=None):
    if constants.ASTRO_DB_ENV_VAR in os.environ:
        return json.loads(os.environ[constants.ASTRO_DB_ENV_VAR])

    credentials = get_creds.get_elastic_credentials(vcap_path=vcap_path)
    if credentials:
        return {__ELASTICSEARCH_SERVICE: credentials}
    
    # Return default credentials on localhost
    return __DEFAULT_DATABASE_CREDENTIALS