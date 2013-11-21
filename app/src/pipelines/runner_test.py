# Copyright 2013 Google Inc. All Rights Reserved.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Pipeline Runner unit tests."""

import collections
import json
import os



import logging
from src import basetest
from src.clients import gcs
from src.pipelines import runner


class PipelineRunnerTest(basetest.TestCase):

  gcs_io = {
      'inputs': [
          {
              'type': 'GcsInput',
              'object': 'gs://bucket/object',
              'sinks': [
                  'gs://bucket/object_in'
              ]
          }
      ],
      'outputs': [
          {
              'type': 'GcsOutput',
              'object': 'gs://bucket/object_out',
              'sources': [
                  'gs://bucket/object_in'
              ]
          }
      ],
      'options': {
          'storage': {
              'bucket': 'bucket',
              'prefix': 'tests/'
          }
      }
  }

  test_null_json1 = """
    {
      "inputs": [{
        "type": "HadoopSetup",
        "project": "test",
        "zone": "us-central2-a",
        "image": "debian-7-wheezy-v20130723",
        "network": "default",
        "machineType": "n1-highcpu-4",
        "numWorkers": 5,
        "sinks": null
      }],
      "outputs": [{
        "type": "HadoopShutdown",
        "project": "test",
        "sources": null
      }]
    }
  """

  test_null_json2 = """
    {
      "inputs": [{
        "type": "HadoopSetup",
        "project": "test",
        "zone": "us-central2-a",
        "image": "debian-7-wheezy-v20130723",
        "network": "default",
        "machineType": "n1-highcpu-4",
        "numWorkers": 5,
        "sinks": null
      },{
        "type": "GcsInput",
        "object": "gs://data_bucket/dataset.csv"
      }],
      "outputs": [{
        "type": "HadoopShutdown",
        "project": "test",
        "sources": null
      },{
        "type": "GcsOutput",
        "object": "gs://results_bucket/results.csv",
        "sources": ["gs://results_bucket/results.csv"]
      }]
    }
  """

  def testScrub(self):
    r = runner.PipelineRunner()
    scrubbed = r.Scrub(self.gcs_io, None)
    self.assertEquals(scrubbed, self.gcs_io)

  def testScrubWithNull1(self):
    config = json.loads(self.test_null_json1)
    r = runner.PipelineRunner()
    scrubbed = r.Scrub(config, None)
    self.assertEquals(scrubbed, config)

  def testScrubWithNull2(self):
    config = json.loads(self.test_null_json2,
                        object_pairs_hook=collections.OrderedDict)
    r = runner.PipelineRunner()
    scrubbed = r.Scrub(config, None)
    config['inputs'][1]['sinks'] = ['gs://results_bucket/results.csv']
    self.assertEquals(scrubbed, config)

  def testScrubFullFiles(self):
    directories = ['src/pipelines/testdata', 'static/examples']

    for directory in directories:
      directory = os.path.join(os.path.dirname(__file__),
                               '../..', directory)
      filenames = [x for x in os.listdir(directory) if x.endswith('.json')]
      logging.info('directory %s files %r', directory, filenames)

      for filename in filenames:
        logging.info('Testing %r from %r',
                     filename, os.path.basename(directory))
        j = open(os.path.join(directory, filename)).read()
        r = runner.PipelineRunner()
        r.Scrub(defn=json.loads(j),
                sink_generator=gcs.Gcs.UrlCreator('test_bucket'))


if __name__ == '__main__':
  basetest.main()
