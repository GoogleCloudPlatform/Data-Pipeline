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


from src.clients import gcs
from src.pipelines import pipeline
from src.pipelines.stages import gcsdeleter


class GcsCompositor(pipeline.Pipeline):
  """Pipeline stage that composes source objects into one destination object.

  This stage will use the GCS object composition feature to combine all given
  sources into one object that will be stored at the URL of the first sink.
  """

  @staticmethod
  def GetHelp():
    return """Compose many objects into a single GCS object.

Object composition requires that all source objects reside in the same
bucket as the destination object. This stage will strip any sources
with buckets that do not match the destination as taken as the first
sink URL.
Optionally, sources can be deleted after composition.

The stage config should look like this:
```python
{
  "contentType": "...",
  "deleteSources": True,
}
```
  * contentType is optional.
"""

  def run(self, config):
    """Runs the stage.

    Object composition requires that all source objects reside in the same
    bucket as the destination object. This stage will strip any sources with
    buckets that do not match the destination as taken as the first sink URL.
    Optionally, sources can be deleted after composition.

    Args:
      config: Specifies the source object(s) and sink.

    Yields:
      Possible deleter stage future.
    """
    (dest_bucket, dest_obj) = gcs.Gcs.UrlToBucketAndName(config['sinks'][0])

    sources = config['sources']
    src_objects = [gcs.Gcs.UrlToBucketAndName(s)[1] for s in sources]

    storage = gcs.Gcs()
    storage.ComposeObjects(dest_bucket,
                           src_objects,
                           dest_obj,
                           config['contentType'])

    if config.get('deleteSources', False):
      yield gcsdeleter.GcsDeleter({'sources': sources})

  def Lint(self, linter):
    """Stage-specific configuration linting."""
    linter.FieldCheck('contentType', validator=self.ValidateContentType)

  def ValidateContentType(self, content_type):
    # super simple for now, for illustration
    if len(content_type.split('/')) != 2:
      raise ValueError('Invalid format.')
