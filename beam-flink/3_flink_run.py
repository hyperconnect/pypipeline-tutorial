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
from __future__ import print_function

import logging

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText

from apache_beam.options.pipeline_options import PipelineOptions


def run(argv=None):
    pipeline_options = PipelineOptions(
        ["--runner=PortableRunner", "--experiments=beam_fn_api", "--job_endpoint=localhost:8099",
         "--environment_type=DOCKER",
         "--environment_config={docker image url}",  # please replace with your own docker images, ref. https://github.com/apache/beam/blob/master/sdks/CONTAINERS.md
         "--worker_harness_container_image={docker image url}"])

    p = beam.Pipeline(options=pipeline_options)

    lines = p | 'read' >> ReadFromText("gs://dataflow-samples/shakespeare/kinglear.txt")
    lines | 'write' >> WriteToText("gs://pycon30/out1.txt")  # please replace with the output bucket name

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()