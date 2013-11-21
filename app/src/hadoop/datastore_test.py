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

"""Unit tests of datastore.py."""

import datastore

from src import basetest


class InstanceInfoTest(basetest.TestCase):
  """Unit tests of InstanceInfo."""

  def testStatusChange(self):
    instance_info = datastore.InstanceInfo()
    instance_info.put()

    instance_info.SetStatus(datastore.InstanceStatus.RUNNING)

    self.assertEqual(datastore.InstanceStatus.RUNNING, instance_info.status)
    all_instances = datastore.InstanceInfo.query().fetch()
    self.assertEqual(1, len(all_instances))
    instance_info = all_instances[0]
    self.assertEqual(datastore.InstanceStatus.RUNNING, instance_info.status)


class ClusterInfoTest(basetest.TestCase):
  """Unit tests of ClusterInfo."""

  def testStatusChange(self):
    cluster_info = datastore.ClusterInfo()
    cluster_info.put()
    cluster_info.SetStatus(datastore.ClusterStatus.READY)

    self.assertEqual(datastore.ClusterStatus.READY, cluster_info.status)
    all_clusters = datastore.ClusterInfo.query().fetch()
    self.assertEqual(1, len(all_clusters))
    cluster_info = all_clusters[0]
    self.assertEqual(datastore.ClusterStatus.READY, cluster_info.status)

  def testCascadeDelete(self):
    cluster_info = datastore.ClusterInfo()
    cluster_info.put()
    instance_info = datastore.InstanceInfo(parent=cluster_info.key)
    instance_info.put()

    all_instances = datastore.InstanceInfo.query().fetch()
    self.assertEqual(1, len(all_instances))

    cluster_info.key.delete()

    all_instances = datastore.InstanceInfo.query().fetch()
    self.assertEqual(0, len(all_instances))

  def testGetMasterIpAddress(self):
    cluster_info = datastore.ClusterInfo()
    cluster_info.put()
    instance_info = datastore.InstanceInfo(parent=cluster_info.key,
                                           external_ip='1.2.3.4')
    instance_info.put()
    cluster_info.master = instance_info.key
    cluster_info.put()

    self.assertEqual('1.2.3.4', cluster_info.GetMasterIpAddress())


if __name__ == '__main__':
  basetest.main()
