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
  COMPUTE_ENGINE_API_VERSION = 'v1beta15'
  OPERATION_WAIT_INTERVAL = 3
  DEFAULT_OPERATION_WAIT_TIMEOUT = 120

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

  def _PersistentDiskUrl(self, disk):
    """Creates URL to represent persistent disk.

    Args:
      disk: Name of the persistent disk.
    Returns:
      URL in string to represent the persistent disk.
    """
    return self.ResourceUrl('disks', disk)

  def _ParseOperationInternal(self, operation, title):
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

  def _WaitForOperationCompletion(self, operation_name,
                                  timeout=DEFAULT_OPERATION_WAIT_TIMEOUT):
    """Waits for zone-level operation to finish.

    Zone-level operation is an operation for zone-level resource, such as
    instance and persistent disk.

    Args:
      operation_name: Name of the operation to get information about.
      timeout: Maximum seconds to wait.
    Returns:
      Google Compute Engine operation resource.  None if timeout.
      https://developers.google.com/compute/docs/reference/v1beta15/operations
    """
    total_wait = 0
    while total_wait < timeout:
      logging.info('Wait for %d seconds for operation %s to complete.',
                   self.OPERATION_WAIT_INTERVAL, operation_name)
      time.sleep(self.OPERATION_WAIT_INTERVAL)
      total_wait += self.OPERATION_WAIT_INTERVAL
      operation = self.GetZoneOperation(operation_name)
      if operation['status'] == 'DONE':
        return operation
    return None

  def _ParseOperation(self, operation, title, wait=False,
                      timeout=DEFAULT_OPERATION_WAIT_TIMEOUT):
    """Waits for the operation completion if necessary, and parses the result.

    Args:
      operation: Operation object as result of operation.
      title: Title used for log.
      wait: Whether to wait for the operation to finish.
      timeout: Maximum seconds to wait.
    Returns:
      Boolean to indicate whether the operation was successful.
    """
    if wait:
      complete_operation = self._WaitForOperationCompletion(
          operation['name'], timeout)
      if complete_operation is None:
        logging.error('Operation %s (%s) time out.', title, operation['name'])
        return False
      return self._ParseOperationInternal(complete_operation, title)
    else:
      return self._ParseOperationInternal(operation, title)

  def GetZoneOperation(self, operation_name):
    """Gets information of zone specific operation.

    Args:
      operation_name: Name of the operation to get information about.
    Returns:
      Google Compute Engine operation resource.  None if error.
      https://developers.google.com/compute/docs/reference/v1beta15/operations
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
      https://developers.google.com/compute/docs/reference/v1beta15/instances
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
    http://developers.google.com/compute/docs/reference/v1beta15/instances/list

    Args:
      filter_string: Filtering condition.
    Returns:
      List of compute#instance.
    """
    result = self.GetApi().instances().list(
        project=self._project, zone=self._zone,
        filter=filter_string).execute()
    return result.get('items', [])

  def CreateInstance(self, instance_name, network, machine_type, image,
                     persistent_disk=None, startup_script='',
                     service_accounts=None, metadata=None, wait=False):
    """Creates Google Compute Engine instance.

    Args:
      instance_name: Name of the new instance.
      network: Network which the instance belongs to.  e.g. 'default'
      machine_type: Machine type.  e.g. 'n1-standard-2'
      image: Machine image name with its project name.
          e.g. 'projects/debian-cloud/global/images/debian-7-wheezy-v20130515'
      persistent_disk: Name of the additional persistent disk.  The disk must
          preexist and must sit in the same zone as the instance.  None if the
          new instance does not have persistent disk attached.
      startup_script: Content of start up script to run on the new instance.
      service_accounts: List of scope URLs to give to the instance with
          the service account.
      metadata: Additional key-value pairs in dictionary to add as
          instance metadata.
      wait: Whether to wait for the operation to finish.
    Returns:
      Boolean to indicate whether the instance creation was successful.
    """
    params = {
        'kind': 'compute#instance',
        'name': instance_name,
        'zone': self.ResourceUrl('zones', self._zone, ResourceZoning.NONE),
        'machineType': machine_type,
        'image': image,
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

    # Attach persistent disk.
    if persistent_disk:
      params['disks'] = [{
          'kind': 'compute#attachedDisk',
          'deviceName': persistent_disk,
          'zone': self.ResourceUrl('zones', self._zone, ResourceZoning.NONE),
          'source': self._PersistentDiskUrl(persistent_disk),
          'type': 'PERSISTENT',
          'mode': 'READ_WRITE',
      }]

    # Add metadata.
    if metadata:
      for key, value in metadata.items():
        params['metadata']['items'].append({'key': key, 'value': value})

    operation = self.GetApi().instances().insert(
        project=self._project, zone=self._zone,
        body=params).execute()

    if wait:
      return self._WaitForOperationCompletion(
          operation['name'], 'Instance creation: %s' % instance_name)
    else:
      return self._ParseOperation(
          operation, 'Instance creation: %s' % instance_name, wait=wait)

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

  def GetDisk(self, disk_name):
    """Gets persistent disk information.

    Args:
      disk_name: Name of the persistent disk to get information about.
    Returns:
      Google Compute Engine disk resource.  None if not found.
      https://developers.google.com/compute/docs/reference/v1beta15/disks
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

  def CreatePersistentDisk(self, name, size_gb, wait=False):
    """Creates persistent disk in the zone of this API.

    Args:
      name: Name of the new persistent disk.
      size_gb: Size of the new persistent disk in GB.
      wait: Whether to wait for the operation to finish.
    Returns:
      Boolean to indicate whether persistent disk creation was successful.
    """
    params = {
        'kind': 'compute#disk',
        'sizeGb': '%d' % size_gb,
        'name': name,
    }
    operation = self.GetApi().disks().insert(
        project=self._project, zone=self._zone,
        body=params).execute()
    return self._ParseOperation(
        operation, 'Persistent disk creation %s' % name, wait=wait)

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
