'''
Created on Feb 26, 2017

@author: alberto
'''

import logging
import sys
from doi.util.mb import mb_util

'''
The following used consumer methods raise KafkaExceptions:
- commit
- subscribe

The following used producer methods raise KafkaExceptions:
- produce

'''

logger = None

def consume(config, topic_name):
    logger.debug("Initializing consumer ...")
    mb = mb_util.MessageBroker(config)                
    consumer = mb.new_consumer('test-consumer', 'test-consumer-group')
    logger.debug("Consumer initialized.")                
    
    logger.debug("Subscribing consumer to topics ...")                
    consumer.subscribe([topic_name])
    logger.debug("Consumer subscribed to topics.")                
    for msg in consumer.messages():
        logger.info("Message received: %s", msg.value)
        logger.info("Committing message: %s", msg.value)
        consumer.commit(msg)
        logger.info("Message committed: %s", msg.value)


if __name__ == '__main__':
    from doi.util import cf_util
    
    logger = logging.getLogger("astro")
    logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler(sys.stdout)
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)

    import confluent_kafka
    logger.info("Python version: %s", sys.version)
    logger.info("confluent-kafka version: %s", confluent_kafka.version())
    logger.info("librdkafka version: %s", confluent_kafka.libversion())
         
    mb_config = cf_util.get_mb_credentials()
    consume(mb_config, 'astro_test_requests.alberto-1')
    
