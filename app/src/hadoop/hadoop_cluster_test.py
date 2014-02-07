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

"""Unit tests of hadoop_cluster.py."""


import datastore
import hadoop_cluster
import mock

from src import basetest


class HadoopClusterTest(basetest.TestCase):
  """Unit test class for HadoopCluster."""

  def tearDown(self):
    mock.patch.stopall()

  def _SetUpMocksForClusterStart(self):
    """Sets up mocks for cluster start tests.

    Returns:
      Parent mock that enables calls of other mocks.
    """
    # Patch functions.
    mock_gce_api_class = mock.patch(
        'src.hadoop.'
        'gce_api.GceApi').start()

    # Create parent mock and attach other mocks to it, so that we can
    # track call order of all mocks.
    parent_mock = mock.MagicMock()
    parent_mock.attach_mock(mock_gce_api_class, 'GceApi')
    parent_mock.attach_mock(
        mock_gce_api_class.return_value.CreateInstanceWithNewPersistentBootDisk,
        'CreateInstanceWithNewPersistentBootDisk')
    parent_mock.attach_mock(
        mock_gce_api_class.return_value.ListInstances,
        'ListInstances')
    parent_mock.attach_mock(
        mock_gce_api_class.return_value.GetInstance,
        'GetInstance')
    parent_mock.attach_mock(
        mock_gce_api_class.return_value.DeleteInstance,
        'DeleteInstance')
    parent_mock.attach_mock(
        mock_gce_api_class.return_value.ListDisks,
        'ListDisks')
    parent_mock.attach_mock(
        mock_gce_api_class.return_value.GetDisk,
        'GetDisk')
    parent_mock.attach_mock(
        mock_gce_api_class.return_value.DeleteDisk,
        'DeleteDisk')

    parent_mock.attach_mock(mock.patch('time.sleep').start(), 'sleep')

    mock_urlfetch = mock.patch('google.appengine.api.urlfetch.fetch').start()
    mock_urlfetch_response = mock.MagicMock()
    mock_urlfetch_response.status_code = 200
    mock_urlfetch_response.content = 'success'
    mock_urlfetch.return_value = mock_urlfetch_response

    mock_gce_api_class.return_value.ListInstances.return_value = [
        {'name': 'hadoop-master', 'status': 'RUNNING'},
        {'name': 'hadoop-worker-000', 'status': 'RUNNING'},
        {'name': 'hadoop-worker-001', 'status': 'RUNNING'},
    ]

    return parent_mock

  def testStartHadoopCluster(self):
    """Unit test of StartHadoopCluster()."""
    parent_mock = self._SetUpMocksForClusterStart()

    parent_mock.GetInstance.return_value = {
        'networkInterfaces': [
            {
                'accessConfigs': [
                    {
                        'natIP': '12.34.56.78'
                    }
                ]
            }
        ]
    }

    hadoop_cluster.HadoopCluster(
        'DUMMY HTTP', project='project-hoge', name='foo',
        zone='zone', machinetype='machine_type', image='image',
        network='default', num_workers=2).StartHadoopCluster()

    # Make sure internal calls are made with expected order with
    # expected arguments.
    method_calls = parent_mock.method_calls.__iter__()
    # Create GceApi.
    call = method_calls.next()
    self.assertEqual('GceApi', call[0])
    # Create master instance.
    call = method_calls.next()
    self.assertEqual('CreateInstanceWithNewPersistentBootDisk', call[0])
    self.assertEqual('managed-hadoop-master', call[1][0])
    # Create worker instance #000.
    call = method_calls.next()
    self.assertEqual('CreateInstanceWithNewPersistentBootDisk', call[0])
    self.assertEqual('managed-hadoop-worker-000', call[1][0])
    # Create worker instance #001.
    call = method_calls.next()
    self.assertEqual('CreateInstanceWithNewPersistentBootDisk', call[0])
    self.assertEqual('managed-hadoop-worker-001', call[1][0])

  def testTeardownCluster(self):
    """Unit test of TeardownCluster()."""
    parent_mock = self._SetUpMocksForClusterStart()

    # Create a sequence of return values for mock method calls.
    # The dummy list with two values: the first value is a list of resources and
    # the second value is an empty list, which indicates that the list of
    # resources have been deleted in this unit test.
    dummy_list = [
        [
            {'name': 'fugafuga'},
            {'name': 'hogehoge'},
            {'name': 'piyopiyo'}
        ],
        []
    ]
    parent_mock.ListInstances.side_effect = dummy_list
    parent_mock.GetInstance.return_value = None
    parent_mock.ListDisks.side_effect = dummy_list
    parent_mock.GetDisk.return_value = None

    cluster_info = datastore.ClusterInfo(
        prefix='managed', project='project-hoge', zone='zone-fuga')
    cluster_info.put()

    hadoop_cluster.HadoopCluster(
        'DUMMY HTTP', cluster_id=cluster_info.key.id()).TeardownCluster()

    parent_mock.GceApi.assert_called_once_with(
        'project-hoge', 'zone-fuga', authorized_http='DUMMY HTTP')
    parent_mock.ListInstances.assert_called_with(
        'name eq "managed-hadoop-master|^managed-hadoop-worker-\\d+$"')
    # Make sure DeleteInstance() is called for each instance.
    self.assertEqual(
        [mock.call('fugafuga'), mock.call('hogehoge'),
         mock.call('piyopiyo')],
        parent_mock.DeleteInstance.call_args_list)

    parent_mock.ListDisks.assert_called_with(
        'name eq "managed-hadoop-master|^managed-hadoop-worker-\\d+$"')
    # Make sure DeleteDisk() is called for each disk.
    self.assertEqual(
        [mock.call('fugafuga'), mock.call('hogehoge'),
         mock.call('piyopiyo')],
        parent_mock.DeleteDisk.call_args_list)

  def testTeardownCluster_NoInstance(self):
    """Unit test of TeardownCluster() with no instance returned by list."""
    parent_mock = self._SetUpMocksForClusterStart()

    # ListInstances() returns empty list.
    parent_mock.ListInstances.return_value = []
    parent_mock.GetInstance.return_value = None

    cluster_info = datastore.ClusterInfo(
        prefix='managed', project='project-hoge', zone='zone-fuga')
    cluster_info.put()
    hadoop_cluster.HadoopCluster(
        'DUMMY HTTP', cluster_id=cluster_info.key.id()).TeardownCluster()

    parent_mock.GceApi.assert_called_once_with(
        'project-hoge', 'zone-fuga', authorized_http='DUMMY HTTP')
    parent_mock.ListInstances.assert_called_with(
        'name eq "managed-hadoop-master|^managed-hadoop-worker-\\d+$"')
    # Make sure DeleteInstance() is not called.
    self.assertFalse(parent_mock.DeleteInstance.called)


if __name__ == '__main__':
  basetest.main()
