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

"""Datastore module for Managed Hadoop."""

from google.appengine.ext import ndb


class InstanceStatus(object):
  PROVISIONING = 'PROVISIONING'
  STAGING = 'STAGING'
  POSTPROCESSING = 'POSTPROCESSING'
  RUNNING = 'RUNNING'
  STOPPING = 'STOPPING'
  TERMINATED = 'TERMINATED'


class InstanceInfo(ndb.Model):
  """Datastore model to represent single Compute Engine instance."""
  name = ndb.StringProperty()
  role = ndb.StringProperty()
  rpckey = ndb.StringProperty()
  external_ip = ndb.StringProperty()
  internal_ip = ndb.StringProperty()
  status = ndb.StringProperty(choices=set([
      InstanceStatus.PROVISIONING,
      InstanceStatus.STAGING,
      InstanceStatus.POSTPROCESSING,
      InstanceStatus.RUNNING,
      InstanceStatus.STOPPING,
      InstanceStatus.TERMINATED]))
  created_at = ndb.DateTimeProperty(auto_now_add=True)
  last_modified = ndb.DateTimeProperty(auto_now=True)

  @classmethod
  def GetByName(cls, name):
    return cls.query(cls.name == name).fetch(1)[0]

  def SetStatus(self, status):
    """Sets the status of the instance.

    Saves the new status of Compute Engine instance in Datastore.

    Args:
      status: New status in string.
    """
    self.status = status
    self.put()


class ClusterStatus(object):
  STARTING_INSTANCES = 'STARTING INSTANCES'
  WARMING_UP = 'WARMING UP'
  POSTPROCESSING = 'POSTPROCESSING'
  STARTING_HADOOP = 'STARTING HADOOP'
  READY = 'READY'
  ERROR = 'ERROR'
  TEARING_DOWN = 'TEARING DOWN'


class ClusterInfo(ndb.Model):
  """Datastore model to represent Hadoop cluster.

  Parent entity of InstanceInfo.
  """
  name = ndb.StringProperty()
  project = ndb.StringProperty()
  prefix = ndb.StringProperty()
  zone = ndb.StringProperty()
  machinetype = ndb.StringProperty()
  image = ndb.StringProperty()
  network = ndb.StringProperty()
  num_workers = ndb.IntegerProperty()
  master = ndb.KeyProperty(kind=InstanceInfo)
  custom_command = ndb.StringProperty()
  status = ndb.StringProperty()
  created_at = ndb.DateTimeProperty(auto_now_add=True)
  last_modified = ndb.DateTimeProperty(auto_now=True)

  @classmethod
  def _pre_delete_hook(cls, key):
    """Hook to delete InstaceInfo in Datastore that belongs to this cluster."""
    for instance in InstanceInfo.query(ancestor=key):
      instance.key.delete()

  def SetStatus(self, status):
    """Sets the status of the cluster."""
    self.status = status
    self.put()

  def SetMasterInstance(self, master_instance):
    """Specifies master instance of the cluster.

    Args:
      master_instance: InstanceInfo of the master instance.
    """
    self.master = master_instance.key
    self.put()

  def SetMasterIpAddress(self, master_ip):
    """Sets external IP address of the master of the cluster.

    Args:
      master_ip: External IP address of the master instance.
    """
    master = self.master.get()
    master.external_ip = master_ip
    master.put()

  def GetMasterIpAddress(self):
    """Returns external IP address of the master of the cluster.

    Returns:
      External IP address of the master instance in string.
    """
    return self.master and self.master.get().external_ip or ''
