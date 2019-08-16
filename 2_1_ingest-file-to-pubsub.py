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

from google.auth import jwt
from google.cloud.pubsub import types
from google.cloud import pubsub

from time import sleep
from threading import Timer

import calendar
import time


PROJECT_ID = 'qwiklabs-gcp-34125c5e4e40e9e3'  # replace project_id and topic with yours
PUBSUB_TOPIC = 'pycon30-file'


service_account_info = json.load(open("configs/pycon30.json"))  # replace service_account_json file location with yours
publisher_audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
credentials = jwt.Credentials.from_service_account_info(
    service_account_info, audience=publisher_audience
)
publisher = pubsub.PublisherClient(
    batch_settings=types.BatchSettings(max_messages=500),
    credentials=credentials
)


class CircularReadFile:
    def __init__(self):
        self.open()

    def open(self):
        self.in_file = open("resources/in.txt", "r")

    def read(self):
        line = self.in_file.readline()
        if not line:
            self.open()
            return self.read()
        elif line.strip() == '':
            return self.read()
        return line.strip()


rf = CircularReadFile()


def publish_one_line():
    Timer(1.0, publish_one_line).start()
    # read file line and send to pubsub
    line = rf.read()
    data = {"event_time": calendar.timegm(time.gmtime()), "data": line}
    print(data)
    publisher.publish('projects/{}/topics/{}'.format(PROJECT_ID, PUBSUB_TOPIC), json.dumps(data).encode('utf-8'))


# set timer
publish_one_line()

while True:
    sleep(60.0)
    continue
