'''
Created on Feb 26, 2017

@author: alberto
'''

import json
import logging
import os
import sys
import time
import confluent_kafka

'''
The following used consumer methods raise KafkaExceptions:
- commit
- subscribe

The following used producer methods raise KafkaExceptions:
- produce

'''

def produce(config, topic_name, retry_interval):
    def on_delivery(error, msg):
        if error is None:
            logger.info("Message delivered: '%s'", msg.value())
        else:
            logger.error("An error was encountered while delivering a kafka message: %s", error.name())
            raise confluent_kafka.KafkaException(error)
    
    producer_config = config.copy()
    producer_config.update({
        'client.id': 'test-producer',
        'api.version.request': True,
        'default.topic.config': {
            'message.timeout.ms': 5000
        }
    })
    
    logger.info("python version: %s", sys.version)
    logger.info("LD_LIBRARY_PATH: %s", os.environ["LD_LIBRARY_PATH"])
    logger.info("confluent-kafka version: %s", confluent_kafka.version())
    logger.info("librdkafka version: %s", confluent_kafka.libversion())
        
    logger.debug("Initializing producer ...")
    producer = confluent_kafka.Producer(producer_config)
    logger.debug("Producer initialized.")

    n = 1    
    while True:
        try: 
            value = {
                "sequence_no": n
            }
                
            logger.info("Producing message: '%s' ...", value)
            producer.produce(topic_name, json.dumps(value), callback=on_delivery)
            logger.info ("Message produced: '%s' ...", value)
            time.sleep(2)
            logger.debug("Flushing messages ...")
            producer.flush()
            logger.debug("Messages flushed.")
            n += 1
            time.sleep(2)
        except confluent_kafka.KafkaException as e:
            logger.warning("An error was encountered while producing a kafka message: %s", e.args[0].name())
            logger.warning("Retrying producing kafka message ...")
            time.sleep(retry_interval)


def _get_kafka_config(config):
    return {
        'bootstrap.servers': ",".join(config['kafka_brokers_sasl']),
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': config['api_key'][0:16],
        'sasl.password': config['api_key'][16:48],
        'security.protocol': 'sasl_ssl',
        'ssl.ca.location': ssl_util.get_ca_path()
    }

if __name__ == '__main__':       
    from doi.util import cf_util
    from doi.util import ssl_util
    
    logger = logging.getLogger("astro")
    logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler(sys.stdout)
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)
         
    mb_config = cf_util.get_mb_credentials()
    produce(_get_kafka_config(mb_config['messagehub']), 'astro_test_requests.alberto-1', 10)
