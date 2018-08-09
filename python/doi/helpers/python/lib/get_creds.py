'''
* These functions obtain VCAP_SERVICES variable for various services.  VCAP_SERVICES is a JSON object available to this application
* whenever it binds to a Bluemix service such as Message Hub and Compose.io ES. The credentials section is
* common to every service and contains all the information you need to connect to that service.
*
* Note:  The 'user-provided' service will be any Compose.io service for this application.  These functions
* assumes Astro will run as a docker container in the IBM Container Service.  However, Compose.io doesn't bind
* directly to a container and it it therefore required to create a 'bridge app' for these bindings as described here:
*
*    https://console.ng.bluemix.net/docs/containers/troubleshoot/ts_ov_containers.html#ts_bridge_app
*
*  Example VCAP_SERVICES payload. Notice that two Compose.io services exist under 'user-provided'..
*
'''
import os
import json
import re
import sys
import logging
import requests

try:
    import confluent_kafka
except ImportError as e:
    pass
'''
{
   "staging_env_json": {
      "BLUEMIX_REGION": "ibm:yp:us-south"
   },
   "running_env_json": {
      "BLUEMIX_REGION": "ibm:yp:us-south"
   },
   "environment_json": {},
   "system_env_json": {
      "VCAP_SERVICES": {
         "spark": [
            {
               "credentials": {
                  "tenant_id": "<bluemix/spark tenant_id>",
                  "tenant_id_full": "<bluemix/spark full tenant id>",
                  "cluster_master_url": "https://spark.bluemix.net",
                  "instance_id": "<bluemix/SAS instance id>",
                  "tenant_secret": "<bluemix/SAS tenant secret>",
                  "plan": "ibm.SparkService.PayGoPersonal"
               },
               "syslog_drain_url": null,
               "label": "spark",
               "provider": null,
               "plan": "ibm.SparkService.PayGoPersonal",
               "name": "Apache Spark-3r",
               "tags": [
                  "big_data",
                  "ibm_created",
                  "ibm_release"
               ]
            }
         ],
         "user-provided": [
            {
               "credentials": {
                  "username": "<composeio_username>",
                  "password": "<composeio_password>",
                  "public_hostname": "<composeio_host_address>"
               },
               "syslog_drain_url": "",
               "label": "user-provided",
               "name": "Elasticsearch by Compose-hg",
               "tags": []
            }
         ]
      }
   },
   "application_env_json": {
      "VCAP_APPLICATION": {
         "limits": {
            "fds": 16384,
            "mem": 128,
            "disk": 1024
         },
         "application_name": "bluemix-bridge-app",
         "application_uris": [
            "bluemix-bridge-app.mybluemix.net"
         ],
         "name": "bluemix-bridge-app",
         "space_name": "dev",
         "space_id": "2c36083a-1a18-4cb6-bff9-3693da5d719e",
         "uris": [
            "bluemix-bridge-app.mybluemix.net"
         ],
         "users": null,
         "version": "009872e0-1f46-4c05-b42e-9ecfb819e121",
         "application_version": "009872e0-1f46-4c05-b42e-9ecfb819e121",
         "application_id": "8b2280b2-cdcc-43f5-a61a-0df01446afd5"
      }
   }
}
'''


logger = logging.getLogger("kc.helpers.get_creds")

__MessageHubService = 'messagehub'
__ElasticService = 'elastic'

CA_PATHS = [
    "/etc/pki/tls/cert.pem",    # RedHat
    "/etc/ssl/certs/",          # Ubuntu
    "/usr/local/etc/openssl/cert.pem",  # OSX
]


def get_ca_path():
    ca_path = None
    for ca_path in CA_PATHS:
        if os.path.exists(ca_path):
            break

    assert ca_path is not None, "Cannot find ca path for this platform"
    return ca_path


def get_bluemix_services(vcap_path=None):
    '''
    To get credentials either from file or environment variable
    vcap_path       Optional file path for VCAP_SERVICES.json
    '''
    services = None
    if ('VCAP_SERVICES' in os.environ):
        logger.info(
            "Bluemix detected. CF app or IBM Container; using VCAP_SERVICES")
        sys.stdout.flush()
        services = json.loads(os.environ['VCAP_SERVICES'])
    elif 'ASTRO_ENDPOINT' in os.environ and 'ASTRO_TOKEN' in os.environ:
        logger.info(
            "Non-Bluemix application/container. Looking for ASTRO endpoint and token.")
        sys.stdout.flush()
        url = "https://" + os.environ['ASTRO_ENDPOINT'] + "/v1/config"
        payload = {'Content-Type': 'application/json',
                   'x-Auth-Token': os.environ['ASTRO_TOKEN']}
        services = json.loads(json.loads(requests.get(
            url, headers=payload).content)["VCAP_SERVICES"])
    elif vcap_path is not None:
        try:
            logger.info("Astro not available. Using VCAP_SERVICES.json")
            sys.stdout.flush()
            with open(vcap_path + "/VCAP_SERVICES.json") as f:
                vcap_data = f.read()
                services = json.loads(vcap_data)
        except IOError as e:
            logger.error("Unexpected error reading VCAP_SERVICES.json")
            raise e
            services = None
    return services


def __get_vcap_service_by_name(services, name, exact_match=False):
    '''
    To extract non user provided service credentials by name
    services        All services in the credentials
    name            Name of the service for which credentials are required
    exact_match     If exact match has to be made with the name
    '''
    service = None
    if exact_match:
        p = re.compile("^" + name + "$")
    else:
        p = re.compile(".*" + name + ".*", re.IGNORECASE)

    for s in services:
        nm = s.get('name', None)
        if nm and p.match(nm):
            service = s
            break

    return service


def get_service_credentials(name, vcap_path=None):
    '''
    To get non user provided service credentials
    name            Optional service name if different from default one
    vcap_path       If VCAP_SERVICES.json file is used to get credentials
    '''
    logger.info("Getting Service Credentials for '%s'", name)
    services = None
    services = get_bluemix_services(vcap_path)
    services = services.get(name, None)
    if services:
        service = __get_vcap_service_by_name(services, name)
        if service:
            return service['credentials']
        else:
            return service
    else:
        return services


def get_user_provided_credentials(name, vcap_path=None, get_env_name=False):
    '''
    Method to retrieve user provided service credentials
    name            Service name
    vcap_path       If VCAP_SERVICES.json file is used to get credentials
    '''
    logger.info("Getting User Provided Credentials for '%s'", name)
    credentials = None
    services = get_bluemix_services(vcap_path)
    if (services is None):
        return None

    for service in services["user-provided"]:
        service_name = service["name"].lower()
        if (name in service_name):
            credentials = service["credentials"]
        if get_env_name:
            env_name = service['name']

    if get_env_name:
        return env_name, credentials
    else:
        return credentials


def get_mh_credentials(vcap_path=None):
    return get_user_provided_credentials(__MessageHubService, vcap_path)


def get_elastic_credentials(vcap_path=None):
    return get_user_provided_credentials(__ElasticService, vcap_path)


def _get_kafka_config(vcap_path=None):
    '''
    To get Kafka Configs
    vcap_path       If VCAP_SERVICES.json file is used to get credentials
    '''
    config = get_user_provided_credentials("messagehub", vcap_path)
    kafka_config = {
        'bootstrap.servers': ",".join(config['kafka_brokers_sasl']),
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': config['api_key'][0:16],
        'sasl.password': config['api_key'][16:48],
        'security.protocol': 'sasl_ssl',
        'log.connection.close': False,
        # 'ssl.ca.location': certifi.where()
        'ssl.ca.location': get_ca_path(),
        'api.version.request': True,
        'broker.version.fallback': '0.10.0.1'
    }
    return kafka_config


def get_kafka_consumer(configs={}, vcap_path=None):
    '''
    To get Kafka Consumer
    configs         Any other configs if required
    vcap_path       If VCAP_SERVICES.json file is used to get credentials
    '''
    consumer_config = _get_kafka_config(vcap_path)
    consumer_config.update(configs)
    # consumer_config['value_deserializer'] = lambda m: json.loads(m.decode('utf-8'))
    consumer_config.update({
        'default.topic.config': {
            # 'auto.offset.reset': 'smallest',
            'enable.auto.commit': False
        }
    })

    return confluent_kafka.Consumer(consumer_config)


def get_kafka_producer(configs={}, vcap_path=None):
    '''
    To get Kafka Producer
    configs         Any other configs if required
    vcap_path       If VCAP_SERVICES.json file is used to get credentials
    '''
    producer_config = _get_kafka_config(vcap_path)
    producer_config.update(configs)

    # producer_config['value_serializer'] = lambda m: json.dumps(m).encode('utf-8')
    return confluent_kafka.Producer(producer_config)
