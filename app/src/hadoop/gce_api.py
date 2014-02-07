#!/usr/bin/python
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


"""Module to provide Google Client API wrapper for Google Compute Engine."""

import logging
import time

import apiclient


class ResourceZoning(object):
  """Constants to indicate which zone type the resource belongs to."""
  NONE = 0
  GLOBAL = 1
  ZONE = 2


class GceApi(object):
  """Google Client API wrapper for Google Compute Engine."""

  COMPUTE_ENGINE_SCOPE = 'https://www.googleapis.com/auth/compute'
  COMPUTE_ENGINE_API_VERSION = 'v1'
  OPERATION_WAIT_INTERVAL = 3
  DEFAULT_OPERATION_WAIT_TIMEOUT = 120
  PERSISTENT_DISK_SIZE_GB = 100

  def __init__(self, project, zone=None, authorized_http=None):
    """Constructor.

    Args:
      project: Project ID.
      zone: Zone name e.g. 'us-central2-a'
      authorized_http: Optional authorized HTTP object.
    """
    self._project = project
    self._zone = zone.split('/')[-1]
    self._authorized_http = authorized_http

    self._api = None

  def GetApi(self):
    """Returns Google Compute Engine API.

    The API object is cached.

    Returns:
      Google Client API object for Google Compute Engine.
    """
    if not self._api:
      self._api = apiclient.discovery.build(
          'compute', self.COMPUTE_ENGINE_API_VERSION,
          http=self._authorized_http)

    return self._api

  @staticmethod
  def IsNotFoundError(http_error):
    """Checks if HttpError reason was 'not found'.

    Args:
      http_error: HttpError
    Returns:
      True if the error reason was 'not found', otherwise False.
    """
    return http_error.resp['status'] == '404'

  @classmethod
  def ResourceUrlFromPath(cls, path):
    return 'https://www.googleapis.com/compute/%s/%s' % (
        cls.COMPUTE_ENGINE_API_VERSION, path)

  def ResourceUrl(self, resource_type, resource_name,
                  zoning=ResourceZoning.ZONE):
    """Creates URL to indicate Google Compute Engine resource.

    Args:
      resource_type: Resource type.
      resource_name: Resource name.
      zoning: Which zone type the resource belongs to.
    Returns:
      URL in string to represent the resource.
    """
    if zoning == ResourceZoning.NONE:
      resource_path = 'projects/%s/%s/%s' % (
          self._project, resource_type, resource_name)
    elif zoning == ResourceZoning.GLOBAL:
      resource_path = 'projects/%s/global/%s/%s' % (
          self._project, resource_type, resource_name)
    else:
      resource_path = 'projects/%s/zones/%s/%s/%s' % (
          self._project, self._zone, resource_type, resource_name)

    return self.ResourceUrlFromPath(resource_path)

  def _ParseOperation(self, operation, title):
    """Parses operation result and log warnings and errors if any.

    Args:
      operation: Operation object as result of operation.
      title: Title used for log.
    Returns:
      Boolean to indicate whether the operation was successful.
    """
    if 'error' in operation and 'errors' in operation['error']:
      for e in operation['error']['errors']:
        logging.error('%s: %s: %s',
                      title, e.get('code', 'NO ERROR CODE'),
                      e.get('message', 'NO ERROR MESSAGE'))
      return False

    if 'warnings' in operation:
      for w in operation['warnings']:
        logging.warning('%s: %s: %s',
                        title, w.get('code', 'NO WARNING CODE'),
                        w.get('message', 'NO WARNING MESSAGE'))
    return True

  def GetZoneOperation(self, operation_name):
    """Gets information of zone specific operation.

    Args:
      operation_name: Name of the operation to get information about.
    Returns:
      Google Compute Engine operation resource.  None if error.
      https://developers.google.com/compute/docs/reference/latest/zoneOperations
    """
    return self.GetApi().zoneOperations().get(
        project=self._project, zone=self._zone,
        operation=operation_name).execute()

  def GetInstance(self, instance_name):
    """Gets instance information.

    Args:
      instance_name: Name of the instance to get information about.
    Returns:
      Google Compute Engine instance resource.  None if not found.
      https://developers.google.com/compute/docs/reference/latest/instances
    Raises:
      HttpError on error, except for 'resource not found' error.
    """
    try:
      return self.GetApi().instances().get(
          project=self._project, zone=self._zone,
          instance=instance_name).execute()
    except apiclient.errors.HttpError as e:
      if self.IsNotFoundError(e):
        return None
      raise e

  def ListInstances(self, filter_string=None):
    """Lists instances that matches filter condition.

    Format of filter string can be found in the following URL.
    http://developers.google.com/compute/docs/reference/latest/instances/list

    Args:
      filter_string: Filtering condition.
    Returns:
      List of compute#instance.
    """
    result = self.GetApi().instances().list(
        project=self._project, zone=self._zone,
        filter=filter_string).execute()
    return result.get('items', [])

  def CreateInstanceWithNewPersistentBootDisk(
      self, instance_name, network, machine_type, image, startup_script='',
      service_accounts=None, metadata=None):
    """Creates Google Compute Engine instance.

    The boot disk is created with the same name as the instance. When the disk
    is ready, an instance is created with the disk as its boot disk.

    Args:
      instance_name: Name of the new instance.
      network: Network which the instance belongs to.  e.g. 'default'
      machine_type: Machine type.  e.g. 'n1-standard-2'
      image: Machine image name with its project name.
          e.g. 'projects/debian-cloud/global/images/debian-7-wheezy-v20131120'
      startup_script: Content of start up script to run on the new instance.
      service_accounts: List of scope URLs to give to the instance with
          the service account.
      metadata: Additional key-value pairs in dictionary to add as
          instance metadata.
    Returns:
      Boolean to indicate whether the instance creation was successful.
    """
    # Use the instance name as the disk name.
    disk_name = instance_name
    if not self._CreatePersistentBootDisk(disk_name, image):
      return False
    # Wait until the new persistent disk is READY.
    for _ in xrange(self.DEFAULT_OPERATION_WAIT_TIMEOUT):
      logging.info('Waiting for boot disk %s getting ready...', disk_name)
      time.sleep(self.OPERATION_WAIT_INTERVAL)
      disk_status = self.GetDisk(disk_name)
      if disk_status.get('status', None) == 'READY':
        logging.info('Disk %s created successfully.', disk_name)
        break
    else:
      logging.error('Persistent disk %s creation timed out.', disk_name)
      return False

    params = {
        'kind': 'compute#instance',
        'name': instance_name,
        'zone': self.ResourceUrl('zones', self._zone, ResourceZoning.NONE),
        'machineType': machine_type,
        'disks': [
            {
                'boot': True,
                'deviceName': disk_name,
                'kind': 'compute#attachedDisk',
                'mode': 'READ_WRITE',
                'source': self.ResourceUrl('disks', disk_name),
                'type': 'PERSISTENT',
                'zone':
                    self.ResourceUrl('zones', self._zone, ResourceZoning.NONE),
            },
        ],
        'metadata': {
            'kind': 'compute#metadata',
            'items': [
                {
                    'key': 'startup-script',
                    'value': startup_script,
                },
            ],
        },
        'networkInterfaces': [
            {
                'kind': 'compute#instanceNetworkInterface',
                'accessConfigs': [
                    {
                        'kind': 'compute#accessConfig',
                        'type': 'ONE_TO_ONE_NAT',
                        'name': 'External NAT',
                    }
                ],
                'network': network,
            },
        ],
        'serviceAccounts': [
            {
                'kind': 'compute#serviceAccount',
                'email': 'default',
                'scopes': service_accounts or [],
            },
        ],
    }

    # Add metadata.
    if metadata:
      for key, value in metadata.items():
        params['metadata']['items'].append({'key': key, 'value': value})

    operation = self.GetApi().instances().insert(
        project=self._project, zone=self._zone,
        body=params).execute()

    return self._ParseOperation(
        operation, 'Instance creation: %s' % instance_name)

  def DeleteInstance(self, instance_name):
    """Deletes Google Compute Engine instance.

    Args:
      instance_name: Name of the instance to delete.
    Returns:
      Boolean to indicate whether the instance deletion was successful.
    """
    operation = self.GetApi().instances().delete(
        project=self._project, zone=self._zone,
        instance=instance_name).execute()

    return self._ParseOperation(
        operation, 'Instance deletion: %s' % instance_name)

  def ListDisks(self, filter_string=None):
    """Lists disks that matches filter condition.

    Format of filter string can be found in the following URL.
    http://developers.google.com/compute/docs/reference/latest/disks/list

    Args:
      filter_string: Filtering condition.
    Returns:
      List of compute#disk.
    """
    result = self.GetApi().disks().list(
        project=self._project, zone=self._zone,
        filter=filter_string).execute()
    return result.get('items', [])

  def GetDisk(self, disk_name):
    """Gets persistent disk information.

    Args:
      disk_name: Name of the persistent disk to get information about.
    Returns:
      Google Compute Engine disk resource.  None if not found.
      https://developers.google.com/compute/docs/reference/latest/disks
    Raises:
      HttpError on error, except for 'resource not found' error.
    """
    try:
      return self.GetApi().disks().get(
          project=self._project, zone=self._zone,
          disk=disk_name).execute()
    except apiclient.errors.HttpError as e:
      if self.IsNotFoundError(e):
        return None
      raise e

  def DeleteDisk(self, disk_name):
    """Deletes Google Compute Engine persistent disk.

    Args:
      disk_name: Name of the disk to delete.
    Returns:
      Boolean to indicate whether the disk deletion was successful.
    """
    operation = self.GetApi().disks().delete(
        project=self._project, zone=self._zone,
        disk=disk_name).execute()

    return self._ParseOperation(
        operation, 'Disk deletion: %s' % disk_name)

  def _CreatePersistentBootDisk(self, disk_name, image):
    """Creates persistent disk in the zone of this API.

    Args:
      disk_name: Name of the persistent disk.
      image: Machine image name with its project name.
          e.g. 'projects/debian-cloud/global/images/debian-7-wheezy-v20131120'
    Returns:
      Boolean to indicate whether a new persistent disk creation was successful.
    """
    if self.GetDisk(disk_name):
      logging.error('Disk %s already exists', disk_name)
      return False

    logging.info('Create persistent disk %s', disk_name)
    params = {
        'kind': 'compute#disk',
        'sizeGb': '%d' % self.PERSISTENT_DISK_SIZE_GB,
        'name': disk_name,
    }
    source_image = self.ResourceUrlFromPath(image) if image else None
    operation = self.GetApi().disks().insert(
        project=self._project, zone=self._zone,
        body=params, sourceImage=source_image).execute()
    return self._ParseOperation(
        operation, 'Persistent disk creation %s' % disk_name)

  def ListZones(self, project=None, filter_string=None):
    result = self.GetApi().zones().list(
        project=project or self._project, filter=filter_string).execute()
    return result.get('items', [])

  def ListMachineTypes(self, project=None, filter_string=None):
    result = self.GetApi().machineTypes().list(
        project=project or self._project, zone=self._zone,
        filter=filter_string).execute()
    return result.get('items', [])

  def ListImages(self, project=None, filter_string=None):
    result = self.GetApi().images().list(
        project=project or self._project, filter=filter_string).execute()
    return result.get('items', [])

  def ListNetworks(self, project=None, filter_string=None):
    result = self.GetApi().networks().list(
        project=project or self._project, filter=filter_string).execute()
    return result.get('items', [])
