'''
Created on Sep 1, 2016

@author: alberto
'''


from doi.util.log_util import mask_sensitive_info
from doi.util.mb.common import Message, Producer, Consumer
from doi.util import log_util
from doi.util import ssl_util
import confluent_kafka
import json
import time


logger = log_util.get_logger("astro.util.mb.kafka")


'''
The following used consumer methods raise KafkaExceptions:
- commit
- subscribe

The following used producer methods raise KafkaExceptions:
- produce
'''

class KafkaMessage(Message):
    def __init__(self, msg):
        super(KafkaMessage, self).__init__()
        self.msg = msg

    @property
    def topic(self):
        return self.msg.topic()
        
    @property
    def partition(self):
        return self.msg.partition()

    @property
    def offset(self):
        return self.msg.offset()
    
    @property
    def key(self):
        return self.msg.key()

    @property
    def value(self):
        return json.loads(self.msg.value())


class KafkaProducer(Producer):
    MAX_RETRY_COUNT = 60
    RETRY_INTERVAL = 5
    
    def __init__(self, config, client_id, **kwargs):
        super(KafkaProducer, self).__init__()
        producer_config = config.copy()
        producer_config.update({
            #'debug': 'protocol,security',
            'api.version.request': True,
            'client.id': client_id
#            'default.topic.config': {
#                'message.timeout.ms': 5000
#            }            
        })
                
        logger.debug("Initializing kafka producer: %s", mask_sensitive_info(producer_config))
        self.producer = confluent_kafka.Producer(producer_config)
        self.max_retry_count = kwargs.get('max_retry_count', KafkaProducer.MAX_RETRY_COUNT)
        self.retry_interval = kwargs.get('retry_interval', KafkaProducer.RETRY_INTERVAL)
        logger.debug("Kafka producer initialized.")

    def close(self):
        pass
    
    def send(self, topic, value):
        def on_delivery(error, msg):
            if error is not None:
                logger.error("An error was encountered while delivering a kafka message: %s", error.name())
                raise confluent_kafka.KafkaException(error)
        
        for retry_count in xrange(0, self.max_retry_count):
            try:
                self.producer.produce(topic, json.dumps(value), callback=on_delivery)
                return self.producer.flush()
            except confluent_kafka.KafkaException as e:
                logger.warning("An error was encountered while sending a kafka message: %s", e.args[0].name())
                logger.warning("Retrying sending the kafka message (%s time) ...", get_ordinal(retry_count +1))
                time.sleep(self.retry_interval)

        raise e


class KafkaConsumer(Consumer):
    MAX_RETRY_COUNT = 60
    RETRY_INTERVAL = 5    
    POLL_TIMEOUT = 30.0

    def __init__(self, config, client_id, group_id, **kwargs):
        super(KafkaConsumer, self).__init__()
        consumer_config = config.copy()
        consumer_config.update({
            #'debug': 'protocol,security',
            'api.version.request': True,
            'client.id': client_id,
            'group.id': group_id,
            'default.topic.config': {
                'enable.auto.commit': False         #default: True
            }            
        })
                
        logger.debug("Initializing kafka consumer: %s", mask_sensitive_info(consumer_config))
        self.consumer = confluent_kafka.Consumer(consumer_config)
        self.poll_timeout = kwargs.get('poll_timeout', KafkaConsumer.POLL_TIMEOUT)
        self.max_retry_count = kwargs.get('max_retry_count', KafkaConsumer.MAX_RETRY_COUNT)
        self.retry_interval = kwargs.get('retry_interval', KafkaConsumer.RETRY_INTERVAL)
        logger.debug("Kafka consumer initialized.")

    def close(self):
        logger.debug("Closing kafka consumer ...")
        self.consumer.close()
        logger.debug("Kafka consumer closed.")

    def subscribe(self, topics):
        for retry_count in xrange(0, self.max_retry_count):
            try:
                return self.consumer.subscribe(topics)
            except confluent_kafka.KafkaException as e:
                logger.warning("An error was encountered while subscribing to kafka topics: %s", str(e.args[0]))
                logger.warning("Retrying subscribing to kafka topics (%s time) ...", get_ordinal(retry_count +1))
                time.sleep(self.retry_interval)

        raise e

    def messages(self, stop_event=None):
        while not stop_event.is_set() if stop_event is not None else True:
            msg = self.consumer.poll(self.poll_timeout)                
            if msg is None:
                # poll timed out
                continue            
            
            error = msg.error()
            if not error:
                yield KafkaMessage(msg)
            else:
                if error.code() == confluent_kafka.KafkaError._PARTITION_EOF:
                    continue
                 
                logger.error(error)
 
    def commit(self, msg):
        for retry_count in xrange(0, self.max_retry_count):
            try:
                return self.consumer.commit(msg.msg, async=False)
            except confluent_kafka.KafkaException as e:
                logger.warning("An error was encountered while committing a kafka message: %s", str(e.args[0]))
                logger.warning("Retrying committing the kafka message (%s time) ...", get_ordinal(retry_count +1))
                time.sleep(self.retry_interval)

        raise e


class MessageHubKafkaProducer(KafkaProducer):
    def __init__(self, config, client_id):
        super(MessageHubKafkaProducer, self).__init__(
            _get_kafka_config(config), client_id)
        
    
class MessageHubKafkaConsumer(KafkaConsumer):
    def __init__(self, config, client_id, group_id):
        super(MessageHubKafkaConsumer, self).__init__(
            _get_kafka_config(config), client_id, group_id)

    
def _get_kafka_config(config):
    return {
        #'debug': 'protocol,security',
        'bootstrap.servers': ",".join(config['kafka_brokers_sasl']),
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': config['api_key'][0:16],
        'sasl.password': config['api_key'][16:48],
        'security.protocol': 'sasl_ssl',
        'ssl.ca.location': ssl_util.get_ca_path()
    }
    
def get_ordinal(n):
    ordinal = lambda n: "%d%s" % (n, "tsnrhtdd"[(n / 10 % 10 != 1) * (n % 10 < 4) * n % 10::4]) 
    return ordinal(n)
