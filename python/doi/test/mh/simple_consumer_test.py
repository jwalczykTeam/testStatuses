'''
Created on Feb 26, 2017

@author: alberto
'''

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

logger = None

def consume(config, topic, poll_timeout, retry_interval):
    consumer_config = config.copy()
    consumer_config.update({
        'client.id': 'test-consumer',
        'group.id': 'test-consumer-group',
        'metadata.request.timeout.ms': 300000,
        'api.version.request': True,
        'default.topic.config': {
            'enable.auto.commit': False,        #default: True
            'auto.offset.reset': 'smallest',    #default: largest            
        }         
    })
    
    logger.info("Python version: %s", sys.version)
    logger.info("LD_LIBRARY_PATH: %s", os.environ["LD_LIBRARY_PATH"])
    logger.info("confluent-kafka version: %s", confluent_kafka.version())
    logger.info("librdkafka version: %s", confluent_kafka.libversion())
    
    max_retry_count = 10    
    for _ in range(0, max_retry_count):
        logger.debug("Initializing consumer ...")                
        consumer = confluent_kafka.Consumer(consumer_config)
        logger.debug("Consumer initialized.")                
        
        logger.debug("Subscribing consumer to: %s",  topic)
        consumer.subscribe([topic])
        logger.debug("Consumer subscribed to: %s",  topic)
                
        try:
            while True:
                msg = consumer.poll(poll_timeout)           
                if msg is None:
                    # poll timed out
                    logger.debug("POLL TIMED OUT")
                    continue            
    
                if not msg.error():
                    logger.info("Message received: %s", msg.value())
                    logger.info("Committing message: %s", msg.value())
                    consumer.commit(msg)
                    logger.info("Message committed: %s", msg.value())
                else:
                    error_code = msg.error().code()
                    if error_code == confluent_kafka.KafkaError._PARTITION_EOF:
                        continue
                     
                    logger.error("An error was encountered while consuming kafka messages: %s", str(msg.error()))
                    raise confluent_kafka.KafkaException(msg.error())
        except confluent_kafka.KafkaException as e:
            logger.warning("A possibly recoverable error was encountered while consuming kafka messages: %s", str(e.args[0]))
            logger.warning("Retrying consuming kafka messages ...")
            time.sleep(retry_interval)
            
            logger.debug("Unsubscribing consumer ...")                
            consumer.unsubscribe()
            logger.debug("Consumer unsubscribed.")                
                
            logger.debug("Closing consumer ...")                
            consumer.close()
            logger.debug("Consumer closed.")                
            del consumer                 

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
    consume(_get_kafka_config(mb_config['messagehub']), 'astro_test_requests.alberto-1', 30, 10)
    
