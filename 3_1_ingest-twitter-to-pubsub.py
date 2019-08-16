# MIT License
#
# Copyright (c) 2019 Jaehyeuk Oh, Hyperconnect
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import json
import sys
import argparse

from configs.pypipeline_config import TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET
from tweepy import StreamListener, OAuthHandler, Stream

from google.cloud.pubsub import types
from google.cloud import pubsub


# replace topic with yours
PROJECT_ID = 'qwiklabs-gcp-34125c5e4e40e9e3'
PUBSUB_TOPIC = 'pycon30-tweet'


class PubSubListener(StreamListener):

    def __init__(self):
        super(StreamListener, self).__init__()

        import json
        from google.auth import jwt

        service_account_info = json.load(open("configs/pycon30.json"))  # replace service_account_json file location with yours
        publisher_audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
        credentials = jwt.Credentials.from_service_account_info(
            service_account_info, audience=publisher_audience
        )
        self.publisher = pubsub.PublisherClient(
            batch_settings=types.BatchSettings(max_messages=500),
            credentials = credentials
        )

    def on_data(self, data):
        tweet = json.loads(data)
        # sys.stdout.write(str(tweet))

        # Only publish original tweets
        if 'extended_tweet' in tweet:
            sys.stdout.write('+')
            self.publisher.publish('projects/{}/topics/{}'.format(
                PROJECT_ID,
                PUBSUB_TOPIC
                ),
                data.encode('utf-8')
            )
        else:
            sys.stdout.write('-')
            self.publisher.publish('projects/{}/topics/{}'.format(
                PROJECT_ID,
                PUBSUB_TOPIC
                ),
                data.encode('utf-8')
            )

        sys.stdout.flush()
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('--twitter-topics',
                      dest='twitter_topics',
                      default='bitcoin',
                      help='A comma separated list of topics to track')

    known_args, _ = parser.parse_known_args(sys.argv)

    auth = OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
    auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET)
    stream = Stream(auth=auth, listener=PubSubListener(), tweet_mode='extended')

    stream.filter(track=known_args.twitter_topics.split(','), languages=['en'])
