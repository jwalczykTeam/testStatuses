'''
Created on Dec 23, 2016

@author: alberto
'''

from doi.util.mb.common import Manager
from doi.util import log_util
from doi.util.log_util import mask_sensitive_info
import json
import requests


logger = log_util.get_logger("astro.util.mb.manager")


class KafkaManager(Manager):
    def __init__(self, config):
        super(KafkaManager, self).__init__()
        self.config = config
        pass
    
    def ensure_topic_exists(self, topic_name):
        pass
    
    def delete_topic(self, topic_name):
        pass

    def close(self):
        pass


class MessageHubManager(Manager):
    def __init__(self, config):
        super(MessageHubManager, self).__init__()
        logger.info("Initializing message hub manager: %s", mask_sensitive_info(config))
        self.config = config
        logger.debug("Message hub manager initialized.")

    def ensure_topic_exists(self, topic_name, partitions=1):
        with requests.Session() as s:
            kafka_admin_url = str(self.config['kafka_admin_url'])
            api_key = str(self.config['api_key'])
            topics = MessageHubManager._get_topics(s, kafka_admin_url, api_key)
            if topic_name not in [x['name'] for x in topics]:
                r = s.post('{}/admin/topics'.format(kafka_admin_url),
                           headers={'X-Auth-Token': api_key},
                           data=json.dumps({'name': topic_name,
                                            'partitions': partitions}))
                if r.status_code != 202: # topic creation pending
                    r.raise_for_status()
                # A problem here is that topic creation is accepted but not executed.
                # It could go wrong after ...
            else:
                logger.warning("Topic already exists in message hub: '%s'", topic_name)

    def delete_topic(self, topic_name):
        with requests.Session() as s:
            kafka_admin_url = str(self.config['kafka_admin_url'])
            api_key = str(self.config['api_key'])
            topics = MessageHubManager._get_topics(s, kafka_admin_url, api_key)
            if topic_name in [x['name'] for x in topics]:
                r = s.delete('{}/admin/topics/{}'.format(kafka_admin_url, topic_name),
                             headers={'X-Auth-Token': api_key})
                if r.status_code != 202 :  # topic deletion pending
                    r.raise_for_status()
                # A problem here is that topic deletion is accepted but not executed.
                # It could go wrong after ...
            else:
                logger.warning("Topic does not exists in message hub: '%s'", topic_name)

    def close(self):
        pass

    def get_topics(self):
        with requests.Session() as s:
            kafka_admin_url = str(self.config['kafka_admin_url'])
            api_key = str(self.config['api_key'])
            topics = MessageHubManager._get_topics(s, kafka_admin_url, api_key)
            return topics

    @staticmethod
    def _get_topics(s, kafka_admin_url, api_key):
        r = s.get('{}/admin/topics'.format(kafka_admin_url),
                        headers={'X-Auth-Token': api_key})
        if r.status_code != 200:
            r.raise_for_status()

        topics = json.loads(r.content)
        return topics