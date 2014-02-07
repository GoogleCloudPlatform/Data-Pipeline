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

"""Compute Engine Client Library for Key APIs."""

import logging

from src import auth


class ComputeEngine(object):
  """A class for accessing Compute Engine API."""

  AUTH_SCOPE = 'https://www.googleapis.com/auth/compute'
  API_VERSION = 'v1'

  def __init__(self, project_id):
    """Create a Compute Engine class.

    Args:
      project_id: The Compute Engine project id.
    """
    self.project_id = project_id
    self.computeengine = auth.Service.FromServiceAccount(
        'compute', self.API_VERSION, self.AUTH_SCOPE)

  def ListInstances(self, zone, fields=None):
    """Query the Compute Engine API and return all instances in a zone.

    Args:
      zone: The Compute Engine zone (e.g. 'us-central1-a')
      fields: Selector specifying which fields to include in the response.

    Returns:
      reply: List of JSON API resonses
    """
    request = self.computeengine.instances().list(
        project=self.project_id, filter=None, zone=zone, fields=fields)
    response = request.execute()

    reply = []

    if response and 'items' in response:
      reply = response['items']
    else:
      logging.info('No Instances Found in Zone %s.', zone)

    return reply

  def ListDisks(self, zone, fields=None):
    """Query the Compute Engine API and return all disks in a zone.

    Args:
      zone: The Compute Engine zone (e.g. 'us-central1-a')
      fields: Selector specifying which fields to include in the response.

    Returns:
      reply: List of JSON API resonses
    """
    request = self.computeengine.disks().list(
        project=self.project_id, filter=None, zone=zone, fields=fields)
    response = request.execute()

    reply = []

    if response and 'items' in response:
      reply = response['items']
    else:
      logging.info('No Disks Found in Zone %s.', zone)

    return reply

  def ListZoneOperations(self, zone, fields=None, filter_expression=None,
                         page_token=None, max_results=500):
    """Query the Compute Engine API and return all operations in a zone.

    Args:
      zone: The Compute Engine zone (e.g. 'us-central1-a')
      fields: Selector specifying which fields to include in the response.
      filter_expression: Filter expression for filtering listed resources.
      page_token: Page token for requesting next page of results.
      max_results: Max results to request from API call.

    Returns:
      reply: List of JSON API resonses
      next_page_token: Next page token if more pages exist, otherwise None.
    """
    request = self.computeengine.zoneOperations().list(
        project=self.project_id, filter=filter_expression, zone=zone,
        maxResults=max_results, pageToken=page_token, fields=fields)
    response = request.execute()

    reply = []
    next_page_token = None

    if response and 'items' in response:
      reply = response['items']
      next_page_token = response.get('nextPageToken', None)
    else:
      logging.debug('No Operations Found in Zone %s.', zone)

    return reply, next_page_token
