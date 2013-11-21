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

"""Split a pipeline stage into smaller stages and combine results."""

import copy
import logging
import math
import pprint


from src.clients import gcs
from src.pipelines import pipeline
from src.pipelines.stages import gcscompositor


class ShardStage(pipeline.Pipeline):
  """Splits a pipeline stage into smaller stages and combines results.

  If you have a pipeline stage that has a start and length parameter
  in the config you can specify a shardSize parameter and this class
  will help you split the work up into shardSize chunks, process each
  in parallel and then composite the results together into a final
  result file.

  A common usage of this would be:

  class EasilyParallelizableJob(shardstage.ShardStage):

    def run(self, config):
      (shards, compositors) = self.ShardStage(config)
      if shards and compositors:
        with pipeline.After(*[(yield shard) for shard in shards]):
          _ = [(yield compositor) for compositor in compositors]
      else:
        # Here is where you would do the work you normally would do.
  """

  DEFAULT_CONTENT_TYPE = 'binary/octet-stream'

  def ShardStage(self, config):
    """If length > shardSize split this task up and composite the results.

    Args:
      config: the pipeline stage config.
          config must have length and shardSize to be sharded.

    Returns:
      Tuple of Shard Stages and Compositor stages.
    """
    length = config.get('length', 0)
    shard_size = config.get('shardSize', -1)

    if shard_size < 1 or length <= shard_size:
      return ([], [])

    start = config.get('start', 0)
    position = start
    final_position = start + length
    shards = []
    shard_sinks = []

    # Now we adjust shard size to split the work evenly.
    # TODO(user) maybe even make this be divisible by the number of backends?
    shard_size = int(math.ceil(float(length) /
                               math.ceil(float(length) / shard_size)))
    config['shardSize'] = shard_size
    shard_prefix = config.get('shardPrefix', '')
    sinks = config.get('sinks')

    while position < final_position:
      shard_config = copy.deepcopy(config)
      for i in range(len(sinks)):
        (bucket, obj) = gcs.Gcs.UrlToBucketAndName(sinks[i])
        shard_config['sinks'][i] = gcs.Gcs.UrlCreator(
            bucket, '%s/%s' % (obj, shard_prefix))()
      shard_config['start'] = position
      shard_config['length'] = min(shard_size, final_position - position)
      logging.info('making shard of job position: %r, length: %r, start: %r, '
                   'final_position: %r, source_length: %r',
                   position, shard_config['length'],
                   start, final_position, length)
      shard_sinks.append(shard_config['sinks'])
      shards.append(self.__class__(shard_config))
      position += shard_size

    compositors = []
    for i in range(len(shard_sinks[0])):
      compositor_config = {
          'contentType': config.get('contentType', self.DEFAULT_CONTENT_TYPE),
          'deleteSources': True,
          'sources': [sink[i] for sink in shard_sinks],
          'sinks': [sinks[i]]
          }
      compositors.append(gcscompositor.GcsCompositor(compositor_config))
      logging.info('compositor:\n%s', pprint.pformat(compositor_config))

    logging.info('sharding with %d shards and %d compositors',
                 len(shards), len(compositors))
    return (shards, compositors)
