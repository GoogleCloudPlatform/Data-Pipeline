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

"""GCSInput stage."""

import logging


from src.clients import gcs
from src.pipelines import pipeline


class GcsInput(pipeline.Pipeline):
  """Provides one or more GCS objects as input to a pipeline.

  This stage will match up specified object urls to sinks. If an object url
  differs from its sink, the object will be copied to that location.
  """

  @staticmethod
  def GetHelp():
    return """Input GCS objects into a pipeline.

  This stage will match up specified object urls to sinks. If an object url
  differs from its sink, the object will be copied to that location.

```python
{
  "object": "gs://bucket_name/object_name",
  "objects": {
    "bucket": "bucket_name",
    "prefix": "object_name_prefix",
    "glob": "glob_string"
  },
  "sinks":[destination_object_url]
}
```
  * At least one of 'object' and 'objects' must be provided.
  * Within 'objects':
    * 'bucket' is required.
    * 'prefix' and 'glob' are optional.
  * Any 'sources' for this stage config will be ignored.
"""

  def run(self, config):
    """Runs the stage.

    Args:
      config: Specifies the source object(s) and sinks.
    """
    storage = gcs.Gcs()
    objs = []
    if 'object' in config:
      objs.append(config['object'])
    if 'objects' in config:
      objects = config['objects']
      for o in storage.ListBucket(objects['bucket'],
                                  objects.get('prefix'),
                                  objects.get('glob')):
        objs.append(gcs.Gcs.MakeUrl(objects['bucket'], o))

    diff = len(objs) - len(config['sinks'])
    if diff < 0:
      msg = ''.join(['More sinks than objects available.',
                     'Ignoring: %s',
                     str(config['sinks'][diff:])])
      logging.warning(msg)
    elif diff > 0:
      logging.info('Found more objects than available sinks.')

    # copy any objects to sinks if the urls differ
    to_copy = zip(objs, config['sinks'])
    res = [storage.CopyObject(o[0], o[1]) for o in to_copy if o[0] != o[1]]
    for r in res:
      logging.info('Copied to %s', r['selfLink'])

  def Lint(self, linter):
    """Stage-specific configuration linting."""
    linter.AtLeastOneFieldRequiredCheck(['object', 'objects'])
    linter.FieldCheck('object', validator=gcs.Gcs.UrlToBucketAndName)
    linter.FieldCheck('objects', validator=gcs.Gcs.UrlToBucketAndName)
