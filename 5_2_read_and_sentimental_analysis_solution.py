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

from __future__ import absolute_import

import logging
import json
from sentimental import Sentimental

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.core import DoFn


def run(argv=None):
    pipeline_options = PipelineOptions(["--runner=DirectRunner", "--streaming"])
    p = beam.Pipeline(options=pipeline_options)

    # read
    topic_path = "projects/qwiklabs-gcp-34125c5e4e40e9e3/topics/pycon30-tweet"  # replace topic with yours
    lines = p | 'read' >> beam.io.ReadFromPubSub(topic=topic_path)

    # format message
    def format_message(message, timestamp=beam.DoFn.TimestampParam):
        message = json.loads(message)
        formatted_message = {
            'text': message.get('text'),
            'created_at': message.get('created_at'),
            'timestamp': float(timestamp)
        }
        return formatted_message

    formatted = lines | beam.Map(format_message)

    class CalculateSentimentFn(DoFn):
        senti = None

        def start_bundle(self):
            logging.info('model loading in start_bundle: start')
            if (not self.senti):
                self.senti = Sentimental.load('model.pickle')
            logging.info('model loading in start_bundle: done')

        def setup(self):
            logging.info('model loading in setup: start')
            if (not self.senti):
                self.senti = Sentimental.load('model.pickle')
            logging.info('model loading in setup: done')

        def process(self, element):
            key, value = element

            res = self.senti.sentiment(value.get('text'))
            yield (value, res.get('positive'), res.get('neutral'), res.get('negative'))

    sentimented = (formatted
                    | 'convert to KV' >> beam.Map(lambda x: ('common key', x))
                    | 'calc sentiment' >> (beam.ParDo(CalculateSentimentFn())))

    sentimented | 'out' >> beam.Map(lambda x: logging.info(x))

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
