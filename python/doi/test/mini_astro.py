'''
Created on Feb 22, 2017

@author: alberto
'''

import confluent_kafka
import json
import logging
import random
import sys
import threading
import time
from doi.util import cf_util 
from doi.util import log_util 
from doi.util import ssl_util 
from doi.util.mb import manager_util


screen_lock = threading.Lock()
def log(message):
    # with screen_lock:
    current_time = time.strftime("%H:%M:%S", time.localtime())
    print("{} - {}".format(current_time, message))

def get_ordinal(n):
    ordinal = lambda n: "%d%s" % (n, "TSNRHTDD"[(n / 10 %10 != 1) * (n % 10 < 4) * n % 10::4]) 
    return ordinal(n)

def retry(max_retry_count, retry_interval):
    def real_decorator(func):
        def wrapper(*args, **kwargs):
            retry_count = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except confluent_kafka.KafkaException as e:
                    if retry_count >= max_retry_count:
                        raise e
                    
                    log("POSSIBLY RECOVERABLE ERROR: EXCEPTION='{}'".format(e))
                    log("RETRYING {} TIME ...".format(get_ordinal(retry_count +1)))
                    time.sleep(retry_interval)
                    retry_count += 1
        return wrapper
    return real_decorator


class Service(threading.Thread):
    POLL_TIMEOUT = 30.0
    MAX_RETRY_COUNT = 60
    RETRY_INTERVAL = 5

    def __init__(self, id_, group_id, config, requests_topic_name, responses_topic_name):
        super(Service, self).__init__()                
        self.id = id_
        producer_config = self._get_kafka_config(config)
        producer_config.update({
            'api.version.request': True,
            'client.id': '{}-{}P-client'.format(group_id, self.id),
        })        
        self.producer = confluent_kafka.Producer(producer_config)
        self.requests_topic_name = requests_topic_name
        
        consumer_config = self._get_kafka_config(config)
        consumer_config.update({
            'api.version.request': True,
            'client.id': '{}-{}C-client'.format(group_id, self.id),
            'group.id': '{}-{}C-group'.format(group_id, self.id),
            'default.topic.config': {
                'enable.auto.commit': False #default: True
             }            
        })
        self.consumer = confluent_kafka.Consumer(consumer_config)
        self.responses_topic_name = responses_topic_name
        self.traffic_timeout_lock = threading.Lock()
        self.traffic_timeout_count = 0

    @retry(MAX_RETRY_COUNT, RETRY_INTERVAL)
    def subscribe(self, topics):
        return self.consumer.subscribe(topics)

    @retry(MAX_RETRY_COUNT, RETRY_INTERVAL)
    def iter_messages(self, stop_when_no_traffic=False):
        while True:
            msg = self._poll()
            
            if msg is None:
                # poll timed out
                if stop_when_no_traffic: 
                    log(">>>> POLL TIMED OUT")                    
                    if self._increment_traffic_timeout_count() == 10:
                        # we exit the loop after 10 consecutive poll timeouts (5 mins) 
                        # without sending or receiving msgs
                        break
                continue
                            
            if not msg.error():
                yield msg
                self._reset_traffic_timeout_count()
            else:
                error_code = msg.error().code()
                if error_code == confluent_kafka.KafkaError._PARTITION_EOF:
                    continue
                 
                log(">>>> KAFKA POLL ERROR: '{}'".format(msg.error()))
                raise confluent_kafka.KafkaException(msg.error())                

    @retry(MAX_RETRY_COUNT, RETRY_INTERVAL)
    def commit(self, msg):
        return self.consumer.commit(msg, async=False)

    @retry(MAX_RETRY_COUNT, RETRY_INTERVAL)
    def _poll(self):
        return self.consumer.poll(Service.POLL_TIMEOUT)

    @retry(MAX_RETRY_COUNT, RETRY_INTERVAL)
    def _produce(self, topic, value):
        def on_delivery(error, msg):
            if error is not None:
                # raising the kafka exception here triggers the retry decorator on the _flush method
                log(">>>> KAFKA DELIVERY ERROR: '{}'".format(error))
                raise confluent_kafka.KafkaException(error)
        
        self.producer.produce(topic, value, callback=on_delivery)
        self._reset_traffic_timeout_count()
        
    @retry(MAX_RETRY_COUNT, RETRY_INTERVAL)
    def _flush(self):
        return self.producer.flush()
    
    def send(self, topic, value):
        self._produce(topic, value)
        self._flush()
            
    def _increment_traffic_timeout_count(self):
        with self.traffic_timeout_lock:
            self.traffic_timeout_count += 1
            return self.traffic_timeout_count

    def _reset_traffic_timeout_count(self):
        with self.traffic_timeout_lock:
            self.traffic_timeout_count = 0
            return self.traffic_timeout_count

    def _get_kafka_config(self, config):
        return {
            'bootstrap.servers': ",".join(config['kafka_brokers_sasl']),
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': config['api_key'][0:16],
            'sasl.password': config['api_key'][16:48],
            'security.protocol': 'sasl_ssl',
            'ssl.ca.location': ssl_util.get_ca_path()
        }

        
class Driver(Service):
    def __init__(self, id_, group_id, config, requests_topic_name, responses_topic_name, msg_count, worker_count):
        super(Driver, self).__init__(id_, group_id, config, requests_topic_name, responses_topic_name)                
        self.msg_count = msg_count
        self.worker_count = worker_count
    
    def run(self):
        # start receive_responses thread
        thread = threading.Thread(target=self.receive_responses)
        thread.start()
        
        log("DRIVER - Starting producer: id={}, topic='{}'".format(self.id, self.requests_topic_name))        
        for n in xrange(0, self.msg_count):
            value = {
                "sequence_no": n
            }
            
            log("DRIVER - Sending request '{}'".format(value))            
            self.send(self.requests_topic_name, json.dumps(value))
            time.sleep(5)
            n += 1
        
    def receive_responses(self):
        log("DRIVER - Starting consumer: id={}, topic='{}'".format(self.id, self.responses_topic_name))        
        self.subscribe([self.responses_topic_name])
        
        hits = []
        for n in xrange(0, self.msg_count):
            hits.append([])
        
        # receive responses from workers and add them to hit list
        for msg in self.iter_messages(True):
            value = msg.value()
            log("DRIVER - Received response '{}'".format(value))
            response = json.loads(value)
            n = response['sequence_no']
            worker_id = response['worker_id']
            hits[n].append(worker_id)
            self.commit(msg)

        # verify all responses arrived
        log("BEGING FAILURE REPORT")
        for i in xrange(0, self.msg_count):
            if len(hits[i]) != self.worker_count:
                log("Missing echos: msg=#{}, expected={}, found={}".format(i, self.worker_count, len(hits[i])))
                for worker_id in xrange(0, self.worker_count):
                    if worker_id not in hits[i]:
                        log("    {}".format(worker_id))
        log("END FAILURE REPORT")

   
class Worker(Service):
    def __init__(self, id_, group_id, config, requests_topic_name, responses_topic_name):
        super(Worker, self).__init__(id_, group_id, config, requests_topic_name, responses_topic_name)                
        
    def run(self):
        log("WORKER({}) - Starting consumer: topic='{}'".format(self.id, self.requests_topic_name))        
        self.subscribe([self.requests_topic_name])
        
        # receive messages
        for msg in self.iter_messages():
            value = msg.value()
            log("WORKER({}) - Received request '{}'".format(self.id, value))
            # doing something ...
            self.do_something()
            # sending response                
            response = json.loads(value)
            response['worker_id'] = self.id
            self.send(self.responses_topic_name, json.dumps(response))
            log("WORKER({}) - Sent response: '{}'".format(self.id, response))
            # committing request ...
            self.commit(msg)
            log("WORKER({}) - Committed request '{}'".format(self.id, value))

    def do_something(self):
        # sleep one minute sometimes
        # n = random.randint(1, 30)
        # if n == 15:            
        #     log(">>>> WORKER({}) - Do something for one minute ...".format(self.id))
        #     time.sleep(60)
        pass

        
class AstroTest(threading.Thread):
    ECHO_REQUESTS_TOPIC = "astro_test_requests"
    ECHO_RESPONSES_TOPIC = "astro_test_responses"
    
    def __init__(self, config, group_id, msg_count=10, worker_count=10, clear_topics=True):
        self.config = config
        self.group_id = group_id
        self.msg_count = msg_count
        self.worker_count = worker_count
        self.clear_topics = clear_topics
    
    @property
    def requests_topic_name(self):
        return AstroTest.ECHO_REQUESTS_TOPIC + '.' + self.group_id

    @property
    def responses_topic_name(self):
        return AstroTest.ECHO_RESPONSES_TOPIC + '.' + self.group_id
    
    def start(self):
        kafka_manager = manager_util.MessageHubManager(self.config)
        if self.clear_topics:
            log("Deleting topics ...")
            kafka_manager.delete_topic(self.requests_topic_name)
            kafka_manager.delete_topic(self.responses_topic_name)
            log("Recreating topics ...")
        
        kafka_manager.ensure_topic_exists(self.requests_topic_name)
        kafka_manager.ensure_topic_exists(self.responses_topic_name)

        if self.clear_topics:
            log("Waiting for topics to be created ...")
            while True:
                time.sleep(60)
                topics = kafka_manager.get_topics()
                topic_names = [topic['name'] for topic in topics]
                if set([self.requests_topic_name, self.responses_topic_name]).issubset(topic_names):
                    break
            
            log("Topics created.")
        
        for n in range(0, self.worker_count):
            worker = Worker(
                n,
                self.group_id,
                self.config, 
                self.requests_topic_name,
                self.responses_topic_name)
            worker.start()
        
        time.sleep(30)
        
        driver = Driver(
            'D', 
            self.group_id,
            self.config,
            self.requests_topic_name,
            self.responses_topic_name,
            self.msg_count,
            self.worker_count)
        driver.start()


if __name__ == '__main__':
    logger = log_util.get_logger("astro")
    logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler(sys.stdout)
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)
    
    mb_config = cf_util.get_mb_credentials()
    echo_test = AstroTest(mb_config['messagehub'], 'alberto-1', 100, 12, clear_topics=True)
    echo_test.start()
