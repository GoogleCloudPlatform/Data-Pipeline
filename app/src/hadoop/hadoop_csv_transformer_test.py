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

"""Unit tests for hadoop_csv_transformer."""

import json

import mock

from src import basetest
from src.hadoop import datastore
from src.hadoop import hadoop_csv_transformer


class HadoopCsvTransformerTest(basetest.TestCase):
  """Unit test class for HadoopCsvTransformer."""

  def setUp(self):
    # Call super class' setUp().
    super(HadoopCsvTransformerTest, self).setUp()

    self.mock_gcs_new = mock.patch(
        ''
        'src.clients.gcs.Gcs.__new__').start()
    self.mock_gcs = self.mock_gcs_new.return_value
    self.mock_urlopen = mock.patch('urllib2.urlopen').start()
    self.mock_urlopen.return_value.read.return_value = 'mr-id-123456789'
    self.mock_sleep = mock.patch('time.sleep').start()

    self.transform_config = {
        'sinks': [
            'gs://bucket/output',
            'gs://bucket/output_badrows',
        ],
        'sources': ['gs://bucket/input'],
        'hadoopTmpDir': 'gs://bucket/hadoop-tmp',
    }

  def tearDown(self):
    # Call super class' tearDown().
    super(HadoopCsvTransformerTest, self).tearDown()

    mock.patch.stopall()

  def testTransform_NoHadoopCluster(self):
    """Tries to transform, but no Hadoop cluster is available."""
    self.assertRaises(hadoop_csv_transformer.HadoopError,
                      hadoop_csv_transformer.HadoopCsvTransformer,
                      self.transform_config)

  def testTransform(self):
    """Tests transform Hadoop MapReduce initiation."""
    cluster_info = datastore.ClusterInfo()
    cluster_info.put()
    instance_info = datastore.InstanceInfo()
    instance_info.put()
    cluster_info.SetMasterInstance(instance_info)
    cluster_info.SetMasterIpAddress('11.22.33.44')

    transformer = hadoop_csv_transformer.HadoopCsvTransformer(
        self.transform_config)
    transformer.StartTransform()

    self.assertEqual(1, self.mock_gcs_new.call_count)
    self.mock_gcs.OpenObject.assert_any_call('gs://bucket/input')
    self.mock_gcs.OpenObject.assert_any_call('gs://bucket/output', mode='w')
    self.mock_gcs.OpenObject.assert_any_call(
        'gs://bucket/hadoop-tmp/inputs/input.csv', mode='w')
    self.assertEqual(2, self.mock_urlopen.call_count)
    # First urlopen() to initiate asynchronous MapReduce.
    request = self.mock_urlopen.call_args_list[0][0][0]
    self.assertEqual('http://11.22.33.44/mapreduceasync',
                     request.get_full_url())
    self.assertEqual(
        0, request.headers['Content-type'].find('multipart/form-data'))
    # Make sure transform configuration is embedded and passed as a mapper.
    body = self.mock_urlopen.call_args_list[0][0][1]
    self.assertNotEqual(-1, body.find(json.dumps(self.transform_config)))
    # Second urlopen() to check the status with given ID.
    self.assertEqual('http://11.22.33.44/mapreduceresult?id=mr-id-123456789',
                     self.mock_urlopen.call_args_list[1][0][0])

    self.assertEqual(1, self.mock_sleep.call_count)


if __name__ == '__main__':
  basetest.main()
