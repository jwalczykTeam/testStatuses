'''
Created on Jan 24, 2017

@author: alberto
'''

from doi.util.slack_util import SlackClient


'''
Example:

  array = [
    {
      "channel_type": "slack",
      "webhook_url": "my slack webook url"
    },
    {
      "channel_type": "email",
      "email_address": "agiammar@us.ibm.com"
    }
  ]

  channels = Channels.decode(array)
  channels.notify("hello world")         
'''


class Channel(object):
    def __init__(self):
        pass

    def decode(self, message):
        raise NotImplementedError("notify")


class EmailChannel(Channel):
    TYPE = 'email'

    def __init__(self, email_address):
        super(EmailChannel, self).__init__()
        self.email_address = email_address

    def notify(self, msg):
        pass

    @staticmethod
    def decode(obj):
        channel_type = obj.get('channel_type')
        if channel_type is None:
            return None

        if channel_type != EmailChannel.TYPE:
            return None

        email_address = obj['email_address']
        return EmailChannel(email_address)

    def encode(self):
        return {
            "channel_type": EmailChannel.TYPE,
            "email_address": self.email_address
        }


class SlackChannel(Channel):
    TYPE = 'slack'

    def __init__(self, webhook_url, config):
        super(SlackChannel, self).__init__()
        if not webhook_url.startswith("https://"):
            raise ValueError("Slack webhook_url must start with 'https://")
        self.slack_client = SlackClient(webhook_url, config)

    def notify(self, msg):
        self.slack_client.send(msg)

    @staticmethod
    def decode(obj):
        channel_type = obj.get('channel_type')
        if channel_type is None:
            return None

        if channel_type != SlackChannel.TYPE:
            return None

        webhook_url = obj['webhook_url']
        config = obj.get('config')
        return SlackChannel(webhook_url, config)

    def encode(self):
        return {
            "channel_type": SlackChannel.TYPE,
            "webhook_url": self.slack_client.webhook_url,
            "config": self.slack_client.config
        }


class Channels(object):
    def __init__(self):
        self.channels = []

    def add_channel(self, channel):
        self.channels.append(channel)

    def notify(self, msg):
        for channel in self.channels:
            channel.notify(msg)

    def notify_job_completion(self, job):
        title = job.title if job.title is not None else 'None'
        if job.html_url is not None:
            title = "<{}|{}>".format(job.html_url, title)
        status = job.current_state
        msg = "*_Project:_* " + title + "\n" \
              ":checkered_flag: Mining and analysis for " + title + " has been completed\n" \
              "*_Status:_* " + status
        if job.html_url is not None:
            msg += "\nUse the links above to see the results"
        self.notify(msg)

    def decode(self, array):
        if array is None:
            return

        channel_subclasses = Channel.__subclasses__()

        for obj in array:
            for channel_subclass in channel_subclasses:
                channel = channel_subclass.decode(obj)
                if channel is not None:
                    self.channels.append(channel)
                    break

    def encode(self):
        array = []
        for channel in self.channels:
            array.append(channel.encode())

        return array
