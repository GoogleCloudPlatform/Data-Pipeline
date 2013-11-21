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

"""Pipeline to set up Hadoop Cluster on Google Compute Engine."""

import logging

from src import auth
from src.hadoop import gce_api
from src.hadoop import hadoop_cluster
from src.pipelines import pipeline


SCOPE = ['https://www.googleapis.com/auth/compute',
         'https://www.googleapis.com/auth/devstorage.read_write']

DEFAULT_ZONE = 'us-central2-a'
DEFAULT_IMAGE = 'projects/debian-cloud/global/images/debian-7-wheezy-v20130723'
DEFAULT_NETWORK = 'default'
DEFAULT_MACHINETYPE = 'n1-highcpu-4'
DEFAULT_NUM_WORKERS = 5


class HadoopSetupError(Exception):
  """Error on Hadoop cluster set up."""


def SetupHadoopCluster(config):
  """Function that actually sets up Hadoop cluster on Compute Engine instances.

  Split as a function for unit test purpose.

  Args:
    config: Hadoop setup parameter created by Datapipeline in Python dictionary.
  Raises:
    HadoopSetupError: Incorrect parameters that prevents from setting up
        Hadoop cluster.
  """
  logging.debug('Start Hadoop cluster: %s', str(config))

  # Required parameters.
  try:
    project = config['project']
    prefix = config['prefix']
  except KeyError as e:
    raise HadoopSetupError('Hadoop Setup: Missing required parameter: %s' %
                           str(e))

  # Optional parameters with default values.
  zone = config.get('zone', DEFAULT_ZONE)
  image = config.get('image', DEFAULT_IMAGE)
  network = config.get('network', DEFAULT_NETWORK)
  machinetype = config.get('machineType', DEFAULT_MACHINETYPE)
  num_workers = int(config.get('numWorkers', DEFAULT_NUM_WORKERS))

  # GCE API object used to convert parameters to resource URLs.
  gce = gce_api.GceApi(project, zone)

  cluster = hadoop_cluster.HadoopCluster(
      auth.Service.HttpFromServiceAccount(SCOPE),
      project=project,
      name=prefix,  # Use prefix as a cluster name.
      prefix=prefix,
      zone=zone,
      machinetype=gce.ResourceUrl('machineTypes', machinetype),
      image=gce.ResourceUrlFromPath(image),
      network=gce.ResourceUrl('networks', network,
                              gce_api.ResourceZoning.GLOBAL),
      num_workers=num_workers)
  cluster.StartHadoopCluster()


class HadoopSetup(pipeline.Pipeline):
  """Pipeline class to set up Hadoop cluster."""

  @staticmethod
  def GetHelp():
    return """Setup a hadoop cluster.

The stage config should look like this:

```python
{
  "project": "hadoop project name",
  "prefix": "",
  "zone": "us-central2-a",
  "image": "projects/debian-cloud/global/images/debian-7-wheezy-v20130723",
  "network": "default",
  "machineType": "n1-highcpu-4",
  "numWorkers": 5
}
```
  * everything except project and prefix is optional.
"""

  def run(self, config):
    """Starts Google Compute Engine instances and sets up Hadoop cluster."""
    SetupHadoopCluster(config)
