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

"""ShardStage unit tests."""



import mock

import logging
from src import basetest
from src.pipelines import shardstage


class SimpleShardStage(shardstage.ShardStage):
  """A Simple stage that just keeps track of what config it was started with."""

  def __init__(self, config):
    super(shardstage.ShardStage, self).__init__(config)
    self.simple_shard_stage_config = config


class ShardStageTest(basetest.TestCase):

  def setUp(self):
    super(basetest.TestCase, self).setUp()
    self.gcscompositor_mock = mock.patch(
        'src.'
        'pipelines.stages.gcscompositor.GcsCompositor').start()

  def testNoNeedToShard(self):
    configs = [
        {},
        {'start': 100},
        {'length': 100, 'shardSize': 100},
        {'length': 100, 'shardSize': 400},
        ]

    for config in configs:
      stage = SimpleShardStage(config)
      self.assertEquals(([], []), stage.ShardStage(config))

  def testNeedsToShard(self):
    configs_and_results = [
        ({'length': 100, 'shardSize': 50, 'sinks': ['gs://bucket/name'],
          'shardPrefix': 'shard', 'contentType': 'text/csv'},
         {'shards': [
             {'start': 0, 'length': 50, 'shardSize': 50,
              'shardPrefix': 'shard', 'contentType': 'text/csv',
              'sinks': ['gs://bucket/name/shard0']},
             {'start': 50, 'length': 50, 'shardSize': 50,
              'shardPrefix': 'shard', 'contentType': 'text/csv',
              'sinks': ['gs://bucket/name/shard1']},
             ],
          'compositors': [{
              'sources': ['gs://bucket/name/shard0', 'gs://bucket/name/shard1'],
              'contentType': 'text/csv',
              'deleteSources': True,
              'sinks': ['gs://bucket/name'],
              }]}),
        ({'length': 100, 'shardSize': 99, 'sinks': ['gs://bucket/name'],
          'contentType': 'text/csv'},
         {'shards': [
             {'start': 0, 'length': 50, 'shardSize': 50,
              'contentType': 'text/csv',
              'sinks': ['gs://bucket/name/0']},
             {'start': 50, 'length': 50, 'shardSize': 50,
              'contentType': 'text/csv',
              'sinks': ['gs://bucket/name/1']},
             ],
          'compositors': [{
              'sources': ['gs://bucket/name/0', 'gs://bucket/name/1'],
              'contentType': 'text/csv',
              'deleteSources': True,
              'sinks': ['gs://bucket/name'],
              }]})
        ]
    for config, result in configs_and_results:
      with mock.patch('uuid.uuid4', side_effect=[str(x) for x in range(10)]):
        stage = SimpleShardStage(config)
        (shards, compositors) = stage.ShardStage(config)
        self.assertEquals(len(result['shards']), len(shards))
        self.assertEquals(len(result['compositors']), len(compositors))
        for expected, actual in zip(result['shards'], shards):
          self.assertSameStructure(expected, actual.simple_shard_stage_config)
        for expected, actual in zip(result['compositors'], compositors):
          gcscompositor_config = self.gcscompositor_mock.call_args[0][0]
          self.assertSameStructure(expected, gcscompositor_config)


if __name__ == '__main__':
  basetest.main()
