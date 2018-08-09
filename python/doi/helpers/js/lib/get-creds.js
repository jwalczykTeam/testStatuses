/**
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
{
 "VCAP_SERVICES": {
  "messagehub": [
   {
    "credentials": {
     "api_key": "****",
     "kafka_admin_url": "https://kafka-admin-stage1.messagehub.services.us-south.bluemix.net:443",
     "kafka_brokers_sasl": [
      "kafka01-stage1.messagehub.services.us-south.bluemix.net:9093",
      "kafka02-stage1.messagehub.services.us-south.bluemix.net:9093",
      "kafka03-stage1.messagehub.services.us-south.bluemix.net:9093",
      "kafka04-stage1.messagehub.services.us-south.bluemix.net:9093",
      "kafka05-stage1.messagehub.services.us-south.bluemix.net:9093"
     ],
     "kafka_rest_url": "https://kafka-rest-stage1.messagehub.services.us-south.bluemix.net:443",
     "mqlight_lookup_url": "https://mqlight-lookup-stage1.messagehub.services.us-south.bluemix.net/Lookup?serviceId=40a71bc5-fd90-432e-b624-ceb29b9f36cf",
     "password": "****",
     "user": "08UPuD1gCRkCPR9E"
    },
    "label": "messagehub",
    "name": "Devops-Insights-Kafka",
    "plan": "standard",
    "provider": null,
    "syslog_drain_url": null,
    "tags": [
     "ibm_dedicated_public",
     "web_and_app",
     "ibm_created"
    ]
   }
  ],
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
 }
}
*/
module.exports = {

          get_bluemix_services: function(logger) {

              var services = null;

              if (process.env.VCAP_SERVICES) {
                    logger.info("Bluemix detected.  CF app or IBM Container; using VCAP_SERVICES")
                    services = JSON.parse(process.env.VCAP_SERVICES);
              } else {
                 try {
                    logger.info("Non-Bluemix application / container.  Using VCAP_SERVICES.json")
                    services = require('./VCAP_SERVICES.json');
                  } catch (e) {
                    // error reading VACP_SERVICES.json
                    logger.info("Non-Bluemix application / container.  *** Error reading VCAP_SERVICES.json")
                    services = null;
                  };
              };
              return services;
          },

          get_compose_io_creds: function(logger, service_string) {

              console.log('########################### get_compose_io_creds(%s,%s)',logger,service_string);

              var services = this.get_bluemix_services(logger);
              if (!services) { return null; }

              var creds = null;
              services['user-provided'].forEach(function(service) {
                  var str = service.name;
                  var lower = str.toLocaleLowerCase();
                  if (lower.includes(service_string)) {   // Devops-Insights-Mongo, mongodb, Devops-Insights-Elastic, elasticsearch, etc
                      creds = service.credentials;
                  };
              });
              logger.info('get_compose_io_creds() creds = %s ', JSON.stringify(creds));
              return creds;
          },

          get_bluemix_message_hub_creds: function(logger) {

              var services = this.get_bluemix_services(logger);
              if (!services) { return null; }

                // look for a service starting with 'messagehub'
              var mh_creds = null;
              for (var svcName in services) {
                 if (svcName.match(/^messagehub/)) {
                     mh_creds = services[svcName][0]['credentials'];
                     logger.info('get_bluemix_message_hub_creds(): creds = %s ', JSON.stringify(mh_creds));
                     return mh_creds;
                 };
              };
          },

          print_all: function(logger) {

              var services = this.get_bluemix_services(logger);
              if (!services) {
                logger.info('print_all(): {}');
                return null;
              } else {
                logger.info('print_all(): %s',JSON.stringify(services));
              }
          }
};
