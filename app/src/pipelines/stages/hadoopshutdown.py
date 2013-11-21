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

"""Pipeline to shut down Hadoop Cluster on Google Compute Engine."""

import logging

from src import auth
from src.hadoop import datastore
from src.hadoop import hadoop_cluster
from src.pipelines import pipeline


SCOPE = ['https://www.googleapis.com/auth/compute']


class HadoopShutdownError(Exception):
  """Error on Hadoop cluster shut down."""


def ShutdownHadoopCluster(config):
  """Function that actually shuts down Hadoop cluster.

  Split as a function for unit test purpose.

  Args:
    config: Hadoop shutdown parameter created by Datapipeline
        in Python dictionary.
  Raises:
    HadoopShutdownError: Error on shutting down Hadoop cluster, such as
        incorrect parameters.
  """
  logging.debug('Shutdown Hadoop cluster: %s', str(config))

  # Required parameters.
  try:
    project = config['project']
    prefix = config['prefix']
  except KeyError as e:
    raise HadoopShutdownError(
        'Hadoop Shutdown: Missing required parameter: %s' % str(e))

  # Query by project name and prefix.  Since the query filters with 2 fields,
  # the query requires Datastore index.
  clusters = datastore.ClusterInfo.query(
      datastore.ClusterInfo.project == project,
      datastore.ClusterInfo.prefix == prefix).fetch()

  if not clusters:
    raise HadoopShutdownError(
        'Hadoop Shutdown: No cluster found in project "%s" with prefix "%s"',
        project, prefix)

  for cluster_info in clusters:
    logging.info('Shutdown Hadoop cluster: %s', str(cluster_info.key.id()))
    cluster = hadoop_cluster.HadoopCluster(
        auth.Service.HttpFromServiceAccount(SCOPE),
        cluster_id=cluster_info.key.id())
    cluster.TeardownCluster()


class HadoopShutdown(pipeline.Pipeline):
  """Pipeline class to shut down Hadoop cluster."""

  @staticmethod
  def GetHelp():
    return """Shutdown the hadoop cluster.

The stage config should look like this:

```python
{
  "project": "hadoop project name",
  "prefix": ""
}
```
"""

  def run(self, config):
    """Deletes Google Compute Engine instances of the Hadoop cluster."""
    ShutdownHadoopCluster(config)
