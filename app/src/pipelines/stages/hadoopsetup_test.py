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

"""Unit tests for hadoopsetup.py."""

import mock



from src import basetest
from src.pipelines.stages import hadoopsetup


class SetupHadoopClusterTest(basetest.TestCase):
  """Unit test class for SetupHadoopCluster."""

  def setUp(self):
    self.mock_hadoop_cluster_class = mock.patch(
        'src.'
        'hadoop.hadoop_cluster.HadoopCluster').start()
    self.mock_hadoop_cluster = self.mock_hadoop_cluster_class.return_value

  def tearDown(self):
    mock.patch.stopall()

  def testStartCluster(self):
    """Starts Hadoop cluster with default parameters."""
    hadoopsetup.SetupHadoopCluster({
        'project': 'project-foo',
        'prefix': 'prefix-bar',
    })

    self.assertEqual(1, self.mock_hadoop_cluster_class.call_count)
    args = self.mock_hadoop_cluster_class.call_args
    self.assertEqual('project-foo', args[1]['project'])
    self.assertEqual('prefix-bar', args[1]['prefix'])
    self.assertEqual('prefix-bar', args[1]['name'])
    self.assertTrue(args[1]['zone'])
    self.assertTrue(args[1]['machinetype'])
    self.mock_hadoop_cluster.StartHadoopCluster.assert_called_once_with()

  def testStartClusterWithFullParams(self):
    """Starts Hadoop cluster with custom parameters."""
    hadoopsetup.SetupHadoopCluster({
        'project': 'project-foo',
        'prefix': 'prefix-bar',
        'zone': 'zone-hoge',
        'image': 'image-fuga',
        'network': 'network-piyo',
        'machineType': 'machinetype-boo',
        'numWorkers': 999,
    })

    self.assertEqual(1, self.mock_hadoop_cluster_class.call_count)
    args = self.mock_hadoop_cluster_class.call_args
    self.assertEqual('project-foo', args[1]['project'])
    self.assertEqual('prefix-bar', args[1]['prefix'])
    self.assertEqual('prefix-bar', args[1]['name'])
    self.assertEqual('zone-hoge', args[1]['zone'])
    self.assertEqual(999, args[1]['num_workers'])
    self.assertIn('machinetype-boo', args[1]['machinetype'])
    self.assertIn('image-fuga', args[1]['image'])
    self.assertIn('network-piyo', args[1]['network'])
    self.mock_hadoop_cluster.StartHadoopCluster.assert_called_once_with()

  def testProjectRequired(self):
    """Parameter 'project' is required."""
    self.assertRaises(hadoopsetup.HadoopSetupError,
                      hadoopsetup.SetupHadoopCluster,
                      {'prefix': 'prefix-bar'})

  def testPrefixRequired(self):
    """Parameter 'prefix' is required."""
    self.assertRaises(hadoopsetup.HadoopSetupError,
                      hadoopsetup.SetupHadoopCluster,
                      {'project': 'project-foo'})


if __name__ == '__main__':
  basetest.main()
