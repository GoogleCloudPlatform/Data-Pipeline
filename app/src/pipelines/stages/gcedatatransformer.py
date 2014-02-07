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

"""Transforms Stage for Compute Engine resources."""

import contextlib
import json
import logging
import re
import time

from src.clients import gcs
from src.pipelines import pipeline


class GceDataTransformer(pipeline.Pipeline):
  """Pipeline stage that transforms Compute Engine resources to BigQuery format.

  This stage will transforms resource data into format that is compatible
  for BigQuery table schema. For example, the resource data returned by
  the Compute Engine API may include an array of string. This cannot be
  directly mapped to BigQuery. It has to be converted into either a comma
  separated string or a repeated field.

  This stage will also add a snapshot id to all the data to indicate that
  they are from the same snapshot. The snapshot id can be used to join across
  table if necessary.
  """

  @staticmethod
  def GetHelp():
    return """Transforms Compute Engine resources data to BigQuery format.

This stage transforms the instances, disks and operations data to a format that
is compatible with the BigQuery schema. Specifically, the resource data returned
by the Compute Engine API may include an array of string. This cannot map
directly to BigQuery's JSON input format. It has to be converted into either a
comma separated string or a repeated field.

This stage will also add a snapshot id to all the data to indicate that
they are from the same snapshot. The snapshot id can be used to join across
table if necessary.

The stage config should look like this:

```python
{
  "sources":[source_object_url1, source_object_url2, ...],
  "type": "GceDataTransformer",
  "sinks":[destination_object_url1, destination_object_url2, ...]
}
```

* If sources and sinks are omitted, the data pipeline framework will
  automatically wire the input and output using temporary Cloud Storage
  objects.
* The order of the sources to the sinks are mapped. That is, the fisrt
  source will be transformed and piped to the first sink.
"""

  # Regular expression for extracting the last part of the compute engine URL.
  COMPUTE_ENGINE_API_URL_PREFIX = r'https://www\.googleapis\.com/compute/'
  URL_PATH_RE = re.compile(r'^%s.*/(.*)$' % COMPUTE_ENGINE_API_URL_PREFIX)

  @staticmethod
  def _AddNamePropertyToUrlProperty(properties, property_name):
    """Adds a property with the url path removed.

    For the properties that are returned in the URL format, this methods strips
    the URL and adds another property to the properties dictionary. For example,
    given the network property below:
      "https://www.googleapis.com/compute/v1/projects/cloud-solutions-gce/
       global/networks/aspera"
    properties['networkName'] = aspera will be added to properties.

    Args:
      properties: a resource object
      property_name: name of the URLized property
    """
    if property_name in properties:
      match = GceDataTransformer.URL_PATH_RE.match(properties[property_name])
      if match:
        properties['%sName' % property_name] = match.group(1)

  @staticmethod
  def _TransformInstanceData(instance):
    """Transforms the instance resource data.

    This method transforms the instance resource object to a format that can be
    loaded into BigQuery.

    Args:
      instance: A JSON dictionary instance object.
    """
    # Converts the scopes from an arry to a comma separated list.
    try:
      for service_account in instance['serviceAccounts']:
        service_account['scopes'] = ','.join(service_account['scopes'])
    except KeyError:
      logging.debug('serviceAccount or scopes is not found')

    # Converts the items in tags from an array to a repeated field.
    try:
      items = []
      for item in instance['tags']['items']:
        items.append({'item': item})
      instance['tags']['items'] = items
    except KeyError:
      logging.debug('tags or items is not found')

    GceDataTransformer._AddNamePropertyToUrlProperty(instance, 'machineType')
    GceDataTransformer._AddNamePropertyToUrlProperty(instance, 'zone')
    GceDataTransformer._AddNamePropertyToUrlProperty(instance, 'image')

    if 'disks' in instance:
      for disk in instance['disks']:
        GceDataTransformer._AddNamePropertyToUrlProperty(disk, 'source')

    if 'networkInterfaces' in instance:
      for network_interface in instance['networkInterfaces']:
        GceDataTransformer._AddNamePropertyToUrlProperty(network_interface,
                                                         'network')

  @staticmethod
  def _TransformDiskData(disk):
    """Transforms the disk resource data.

    This method transforms the disk resource object to a format that can be
    loaded into BigQuery.

    Args:
      disk: A JSON dictionary disk object.
    Returns:
      the given disk object transformed.
    """

    # for each disk, extract Names for: zone, sourceImage, sourceSnapshot
    GceDataTransformer._AddNamePropertyToUrlProperty(disk, 'zone')
    GceDataTransformer._AddNamePropertyToUrlProperty(disk, 'sourceImage')
    GceDataTransformer._AddNamePropertyToUrlProperty(disk, 'sourceSnapshot')
    return disk

  @staticmethod
  def _TransformOperationData(operation):
    """Transforms the zone operation resource data.

    This method transforms the zone resource object to a format that can be
    loaded into BigQuery.

    Args:
      operation: A JSON dictionary of the operation object.
    Returns:
      the given operation object transformed.
    """
    # Extract zone and targetLink for each operation.
    GceDataTransformer._AddNamePropertyToUrlProperty(operation, 'zone')
    GceDataTransformer._AddNamePropertyToUrlProperty(operation, 'targetLink')
    return operation

  # A dictionary that maps the type of resource to the transform function.
  _transform_func = {
      'compute#instance': _TransformInstanceData.__func__,
      'compute#disk': _TransformDiskData.__func__,
      'compute#operation': _TransformOperationData.__func__,
  }

  def run(self, config):
    """Run the stage.

    Args:
      config: Specifies the source objects and sinks.
    """
    storage = gcs.Gcs()

    # Adds a snapshot id to all the records so they can be joined if necessary.
    snapshot_id = int(time.mktime(time.localtime()))

    for (source, sink) in zip(config['sources'], config['sinks']):
      logging.debug('Transforming %s to %s', source, sink)

      with contextlib.closing(storage.OpenObject(source)) as source_file:
        with contextlib.closing(storage.OpenObject(sink, mode='w')
                               ) as sink_file:
          for line in source_file:
            if line:
              json_obj = json.loads(line)
              json_obj['snapshotId'] = snapshot_id

              # Transforms the resource.
              try:
                GceDataTransformer._transform_func[json_obj['kind']](json_obj)
              except KeyError:
                logging.warning('Unrecognized resource %r', line)

            sink_file.write('%s\n' % json.dumps(json_obj))

