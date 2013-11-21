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

"""Pipeline stages."""

import copy


from mapreduce.lib.pipeline import common

from src.clients import gcs
from src.clients import s3
from src.pipelines import pipeline
from src.pipelines import shardstage
from src.pipelines.stages import gcscompositor


class S3Input(shardstage.ShardStage):
  """Provides data from S3 as input to a pipeline."""
  CHUNK_SIZE_8MB = 1 << 23
  CHUNK_SIZE_32MB = 1 << 25
  REQUEST_CHUNK_SIZE = CHUNK_SIZE_8MB
  MAX_SHARD_SIZE = CHUNK_SIZE_32MB  # App Engine's response size limit.
  DEFAULT_CONTENT_TYPE = 'binary/octet-stream'

  @staticmethod
  def GetHelp():
    return """Read files from S3.

The stage config should look like this:

```python
{
  "object": "bucket_name/object_name",
  "objects":{
    "bucket": "bucket_name",
    "prefix": "object_name_prefix",
  },
  "start": first_byte,
  "length": number_of_bytes,
  "s3Credentials": {
    "accessKey": "...",
    "accessSecret" "...",
  },
  "shardPrefix": "...",
  "sinks":[destination_object_url]
}
```

* At least one of 'object' and 'objects' must be provided.
* Within 'objects', 'bucket' is required.
* 'shardPrefix' can be used to organize the temporary objects, if any,
  created during the chunked transfer (and recomposition) of the object
  in GCS.
* Any 'sources' for this stage config will be ignored.
  """

  def run(self, config):
    """Runs the stage.

    Args:
      config: Specifies the source object(s) and sinks.
    Yields:
      If necessary, a pipeline future.
    """
    storage = s3.S3(config=config.get('s3Credentials'))

    s3_objects = []
    if 'object' in config:
      s3_objects.append(config['object'])

    if 'objects' in config:
      objects = config['objects']
      for s3_obj in storage.ListBucket(objects['bucket'],
                                       objects.get('prefix')):
        s3_objects.append(s3.S3.MakeUrl(objects['bucket'], s3_obj))

    s3_objects = zip(s3_objects, config['sinks'])
    # do the first one here
    (s3_obj, gcs_obj) = s3_objects.pop()

    # fan out any others
    sub_stages = []
    if 'objects' in config:
      config.pop('objects')
    for (next_s3_obj, next_gcs_obj) in s3_objects:
      cfg = copy.deepcopy(config)
      cfg['object'] = next_s3_obj
      cfg['sinks'] = [next_gcs_obj]
      s = yield S3Input(cfg)
      sub_stages.append(s)

    start = config.get('start')
    if not start:
      start = 0
      config['start'] = 0

    length = config.get('length')
    if not length:
      length = storage.StatObject(s3_obj)['size']
      config['length'] = length

    if 'shardSize' not in config:
      config['shardSize'] = self.REQUEST_CHUNK_SIZE

    (shards, compositors) = self.ShardStage(config)
    if shards and compositors:
      with pipeline.After(*[(yield shard) for shard in shards]):
        _ = [(yield compositor) for compositor in compositors]
    else:
      handler = _S3ReadBufferHandler(s3_obj,
                                     gcs_obj,
                                     config.get('shardPrefix'))

      storage.ReadObject(url=s3_obj,
                         handler=handler.Handle,
                         start=start,
                         length=length)

      comp_stage = common.Ignore()
      if handler.chunk_urls:
        comp_config = {'contentType': handler.content_type,
                       'sources': handler.chunk_urls,
                       'sinks': [gcs_obj]}
        comp_stage = yield gcscompositor.GcsCompositor(comp_config)
        sub_stages.append(comp_stage)

      yield common.Append(*sub_stages)

  def Lint(self, linter):
    """Stage-specific configuration linting."""
    linter.AtLeastOneFieldRequiredCheck(['object', 'objects'])
    linter.FieldCheck('object', validator=s3.S3.UrlToBucketAndName)
    linter.FieldCheck('objects', validator=s3.S3.UrlToBucketAndName)
    linter.FieldCheck('s3Credentials', field_type=dict, required=True)
    linter.FieldCheck('s3Credentials.accessKey', required=True)
    linter.FieldCheck('s3Credentials.accessSecret', required=True)
    linter.FieldCheck('shardSize', validator=self.ValidateShardSize)

  def ValidateShardSize(self, size):
    if size > self.MAX_SHARD_SIZE:
      raise ValueError('Size exceeds App Engine response limit.')


class _S3ReadBufferHandler(object):
  """Helper for the object reader."""

  def __init__(self, s3_obj, gcs_obj, shard_prefix):
    self.s3_obj = s3_obj
    self.gcs_obj = gcs_obj
    self.gcs_storage = gcs.Gcs()
    (gcs_bucket, _) = gcs.Gcs.UrlToBucketAndName(gcs_obj)
    self.url_gen = gcs.Gcs.UrlCreator(gcs_bucket, shard_prefix)
    self.chunk_urls = []
    self.content_type = None
    self.once = True

  def Handle(self, buf, unused_bytes_read, bytes_remaining, ct):
    """The wrapped handler."""
    self.content_type = ct
    if bytes_remaining == 0 and self.once:
      # got it all in one chunk
      self.gcs_storage.InsertObject(buf, self.gcs_obj)
    else:
      # write chunk to temp gcs obj
      tmp = self.url_gen()
      self.chunk_urls.append(tmp)
      self.gcs_storage.InsertObject(buf, tmp)
    self.once = False
