'''
Created on Sep 1, 2016

@author: alberto
'''


from doi.util import log_util
from doi.util.mb.kafka_util import KafkaProducer
from doi.util.mb.kafka_util import KafkaConsumer
from doi.util.mb.kafka_util import MessageHubKafkaProducer
from doi.util.mb.kafka_util import MessageHubKafkaConsumer
from doi.util.mb.manager_util import KafkaManager
from doi.util.mb.manager_util import MessageHubManager


'''
Config:

    {
        "kafka": {
            "bootstrap_servers": [
            ]
        }
    }

    Or

    {
        "messagehub": {
            "mqlight_lookup_url": "https://mqlight-lookup-prod01.messagehub.services.us-south.bluemix.net/Lookup?serviceId=bb446386-ded8-4b26-b0f2-9a1afd545fad",
            "api_key": "***********",
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
    }
'''


logger = log_util.get_logger("astro.util.mb")


class MessageBroker(object):
    def __init__(self, config):
        self.config = config
        
    def new_producer(self, client_id):
        if 'kafka' in self.config:
            return KafkaProducer(self.config['kafka'], client_id)
        elif 'messagehub':
            return MessageHubKafkaProducer(self.config['messagehub'], client_id)
        else:
            raise ValueError("No or unknown message broker type specified")                    

    def new_consumer(self, client_id, group_id):
        if 'kafka' in self.config:
            return KafkaConsumer(self.config['kafka'], client_id, group_id)
        elif 'messagehub':
            return MessageHubKafkaConsumer(self.config['messagehub'], client_id, group_id)
        else:
            raise ValueError("No or unknown message broker type specified")                    
    
    def new_manager(self):
        if 'kafka' in self.config:
            return KafkaManager(self.config['kafka'])
        elif 'messagehub':
            return MessageHubManager(self.config['messagehub'])
        else:
            raise ValueError("No or unknown message broker type specified")                    
