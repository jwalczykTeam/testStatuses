var bluemixcreds = require('../lib/get-creds') 
  , log4js = require('log4js');

// Setup logger
//
var logger = log4js.getLogger(appInfo.name);

var log4jsConfig = { appenders: [ { type: "console", layout: { type: "basic" } } ], replaceConsole: true };
if ( process.env.LOG_CONF ) {
  try {
    log4jsConfig = JSON.parse(process.env.LOG_CONF);
  } catch (e) {
    logger.error(e.message);
  }
} else {
  logger.info('No logger configuration set. Using default.');
}
log4js.configure(log4jsConfig);


// setup ES
var esConf = {
   client_config: {
       hosts: [validatedFetchOptions.esUrl]
   },
   index_name: entity.name,
   index_config_path: validatedFetchOptions.mappingsFile
};

var es_creds = bluemixcreds.get_compose_io_creds(logger,"elastic");

if (!es_creds) {

    logger.info('es_creds is undefined!  I can\'t attach to elasticsearch instance!');
    throw new Error('I can\'t attach to a elasticsearch instance!');

} else {
    
     logger.info('Attaching to elasticsearch instance: %s ', JSON.stringify(es_creds));
     esConf = {
       client_config: {
           hosts: [es_creds.public_hostname],
           username: es_creds.username,
           password: es_creds.password
       },
       username: es_creds.username,
       password: es_creds.password,
       index_name: entity.name,
       index_config_path: validatedFetchOptions.mappingsFile
     };
}

logger.info('kafka instance detected: %s ',JSON.stringify(esConf));

// Set up Kafka
var kafkaConf = {};

if ( validatedFetchOptions.kafkaUrl ) {
      kafkaConf = {
        producer_config: {
            bootstrap_servers: [validatedFetchOptions.kafkaUrl]
        }
      }           
};

var mh_creds = bluemixcreds.get_bluemix_message_hub_creds(logger);

if (!mh_creds) {
    logger.info('mh_creds is undefined!  I can\'t attach to Bluemix Message Hub (Kafka) instance!');
    throw new Error('I can\'t attach to Bluemix Message Hub (Kafka) instance!');
} else {
    logger.info('Attaching to kafka instance: %s ', JSON.stringify(mh_creds));
    kafkaConf = {
        producer_config: {
            bootstrap_servers: mh_creds.kafka_brokers_sasl,
            sasl_mechanism: 'PLAIN',
            sasl_plain_username: mh_creds.user,
            sasl_plain_password: mh_creds.password,
            security_protocol: 'SASL_SSL'
        }
    }
};

logger.info('kafka instance detected: %s ',JSON.stringify(kafkaConf));

// Setup MongoDb
//
var mongoDbUri;
var mongoDatabaseName = 'meteor';

var mongo_creds = bluemixcreds.get_compose_io_creds(logger,"mongo");

if (!mongo_creds) {
      
    // There's no VCAP_SERVICES and not json file, last resort...
    if ( process.env.MONGO_URI ) {         
         mongoDbUri = process.env.MONGO_URI; 
         logger.info('Using process.env.MONGO_URI : %s ',mongoDbUri); 
    } else {
         throw new Error('No VCAP_SERVICES or MONGO_URI defined.  I can\'t attach to a mongodb instance!');
    };           

} else {
        /** We're running in Bluemix; try to build mongodb connection string of format
      *
      *       mongoDbUri = "mongodb://username:password@host:port/database?ssl=true"
      */
       mongoDbUri = "mongodb://" + mongo_creds.user + ":" + mongo_creds.password + "@" + mongo_creds.uri + ":" + mongo_creds.port + "/" + mongoDatabaseName + "?ssl=true"; 
       print_mongoDbUri = "mongodb://" + mongo_creds.user + ":" + "********" + "@" + mongo_creds.uri + ":" + mongo_creds.port + "/" + mongoDatabaseName + "?ssl=true"; 
};

logger.info('mongodb instance detected: %s ',print_mongoDbUri);

