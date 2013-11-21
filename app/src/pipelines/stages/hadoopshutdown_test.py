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

"""Unit tests for hadoopshutdown.py."""

import mock



from src import basetest
from src.hadoop import datastore
from src.pipelines.stages import hadoopshutdown


class ShutdownHadoopClusterTest(basetest.TestCase):
  """Unit test class for ShutdownHadoopCluster."""

  def setUp(self):
    super(basetest.TestCase, self).setUp()
    self.mock_hadoop_cluster_class = mock.patch(
        'src.'
        'hadoop.hadoop_cluster.HadoopCluster').start()
    self.mock_hadoop_cluster = self.mock_hadoop_cluster_class.return_value

  def tearDown(self):
    mock.patch.stopall()
    super(basetest.TestCase, self).tearDown()

  def _PrepareClusterInfo(self):
    """Prepares ClusterInfo in Datastore."""
    cluster_info = datastore.ClusterInfo(
        project='project-foo', prefix='prefix-bar')
    cluster_info.put()
    return cluster_info

  def testShutdownCluster(self):
    """Shuts down Hadoop cluster."""
    # Prepare ClusterInfo in Datastore.
    cluster_info = self._PrepareClusterInfo()

    hadoopshutdown.ShutdownHadoopCluster({
        'project': 'project-foo',
        'prefix': 'prefix-bar',
    })

    self.assertEqual(1, self.mock_hadoop_cluster_class.call_count)
    args = self.mock_hadoop_cluster_class.call_args
    self.assertEqual(cluster_info.key.id(), args[1]['cluster_id'])
    self.mock_hadoop_cluster.TeardownCluster.assert_called_once_with()

  def testShutdownNonExistingCluster(self):
    """Tries to shutdown non-existing cluster."""
    self.assertRaises(hadoopshutdown.HadoopShutdownError,
                      hadoopshutdown.ShutdownHadoopCluster,
                      {'project': 'project-foo', 'prefix': 'prefix-bar'})

  def testProjectRequired(self):
    """Parameter 'project' is required."""
    self._PrepareClusterInfo()

    self.assertRaises(hadoopshutdown.HadoopShutdownError,
                      hadoopshutdown.ShutdownHadoopCluster,
                      {'prefix': 'prefix-bar'})

  def testPrefixRequired(self):
    """Parameter 'prefix' is required."""
    self._PrepareClusterInfo()

    self.assertRaises(hadoopshutdown.HadoopShutdownError,
                      hadoopshutdown.ShutdownHadoopCluster,
                      {'project': 'project-foo'})


if __name__ == '__main__':
  basetest.main()
