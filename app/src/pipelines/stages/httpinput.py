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

import contextlib
import cStringIO as StringIO
import logging
import urllib2
import urlparse


from src.clients import gcs
from src.pipelines import pipeline
from src.pipelines import shardstage


class HttpInput(shardstage.ShardStage):
  """Provides data from an arbitrary URL as input to a pipeline.

  Works with HTTP/S and FTP. Will download resource in chunks concurrently
  as substages.
  """
  CHUNK_SIZE_8MB = 1 << 23
  CHUNK_SIZE_32MB = 1 << 25
  REQUEST_CHUNK_SIZE = CHUNK_SIZE_8MB
  MAX_SHARD_SIZE = CHUNK_SIZE_32MB  # App Engine's response size limit.
  DEFAULT_CONTENT_TYPE = 'binary/octet-stream'

  @staticmethod
  def GetHelp():
    return """Load Data from a URL.

The stage config should look like this:

```python
{
  "url": "...",
  "start": first_byte,
  "length": number_of_bytes,
  "shardSize": maximum_number_of_bytes,
  "shardPrefix": "...",
}
```

If the contents of the url are larger than shardSize then multiple
requests will be made in parallel to load parts of the url and then
compisited together into the resulting file.

* start and length are optional.
* 'shardPrefix' can be used to organize the temporary objects, if any,
  created during the chunked transfer (and recomposition) of the
  object in GCS.
* Any 'sources' for this stage config will be ignored.
"""

  def run(self, config):
    """Runs the stage.

    Args:
      config: Specifies the source object(s) and sinks.
    Yields:
      If necessary, a pipeline future for a GcsCompositor stage
    """
    start = config.get('start')
    if not start:
      start = 0
      config['start'] = 0

    if 'length' not in config:
      # hit the resource with a one-byte range GET to find out length
      # this is necessary as App Engine will strip the Content-Length header
      # from a HEAD request
      req = urllib2.Request(config['url'])
      req.add_header('Range', 'bytes=0-0')
      meta_inf = None
      with contextlib.closing(urllib2.urlopen(req)) as resp:
        meta_inf = resp.info()

      range_len = meta_inf.getheaders('Content-Range')
      if range_len:
        range_len = long(range_len[0].split('/')[1])
        config['length'] = range_len - start
      else:
        logging.warning('Cannot determine resource length.')

    if 'shardSize' not in config:
      config['shardSize'] = self.REQUEST_CHUNK_SIZE

    (shards, compositors) = self.ShardStage(config)
    if shards and compositors:
      with pipeline.After(*[(yield shard) for shard in shards]):
        _ = [(yield compositor) for compositor in compositors]
    else:
      gcs_obj = config['sinks'][0]
      gcs_storage = gcs.Gcs()

      start = config.get('start', 0)
      length = config.get('length')

      req = urllib2.Request(config['url'])
      range_bytes = 'bytes=%s-%s'
      if length:
        range_bytes %= (start, start + length - 1)
      else:
        range_bytes %= (start, '')
      req.add_header('Range', range_bytes)
      with contextlib.closing(urllib2.urlopen(req, timeout=300)) as resp:
        with contextlib.closing(StringIO.StringIO(resp.read())) as resp_buf:
          gcs_storage.InsertObject(resp_buf, url=gcs_obj)

  def Lint(self, linter):
    """Stage-specific configuration linting."""
    linter.FieldCheck('url', validator=self.ValidateUrl)
    linter.FieldCheck('shardSize', validator=self.ValidateShardSize)

  def ValidateUrl(self, url):
    parsed = urlparse.urlparse(url)
    if not parsed.scheme or not parsed.netloc or not parsed.path:
      raise ValueError('Invalid format.')

  def ValidateShardSize(self, size):
    if size > self.MAX_SHARD_SIZE:
      raise ValueError('Size exceeds App Engine response limit.')

