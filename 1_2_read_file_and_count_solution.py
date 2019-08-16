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

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.core import _ReiterableChain


def run():
    pipeline_options = PipelineOptions(["--runner=DirectRunner"])

    p = beam.Pipeline(options=pipeline_options)

    # read file
    text_filename = "resources/in.txt"
    lines = p | 'read' >> ReadFromText(text_filename)

    # split words
    def find_words(element):
        import re
        return re.findall(r'[A-Za-z\']+', element)

    words = (lines | 'split' >> (beam.FlatMap(find_words)))

    # count words
    def count_ones(word_ones):
        (word, ones) = word_ones
        return word, sum(ones)

    counts = (words
                | 'pair' >> beam.Map(lambda x: (x, 1))
                | 'group' >> beam.GroupByKey()
                | 'count' >> beam.Map(count_ones))

    # aggr to list
    def aggr_to_list(values):
        try:
            if not values:
                return values
            elif isinstance(values, _ReiterableChain):
                return [x for x in values]
            elif len(values) == 1:
                return values[0]
            else:
                if isinstance(values[0], list):
                    return values[0] + [values[1]]
                else:
                    return [x for x in values]
        except Exception:
            print(values)
            pass

    aggred_list = counts | 'sort' >> beam.CombineGlobally(aggr_to_list).without_defaults()

    # out
    aggred_list | 'out' >> beam.Map(lambda x: logging.info(sorted(x, key=lambda x: x[1], reverse=True)))

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
