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

import logging


from src.clients import gcs
from src.pipelines import pipeline


class GcsOutput(pipeline.Pipeline):
  """Provides a GCS object as from a pipeline.

  This stage will take the first source and copy it's data, if necessary,
  to the object specified in the config.
  """

  @staticmethod
  def GetHelp():
    return """Copy the first source file to a gcs file.

The stage config should look like this:

```python
{
  "object": "gs://bucket/name",
}
```
  * Any 'sinks' for this stage config will be ignored.
"""

  def run(self, config):
    """Runs the stage.

    Args:
      config: Specifies the destination object and source.
    """
    storage = gcs.Gcs()
    src = config['sources'][0]
    dest = config['object']
    if src is not dest:
      res = storage.CopyObject(src, dest)
      logging.info('Copied %s to %s', src, res['selfLink'])

  def Lint(self, linter):
    """Stage-specific configuration linting."""
    linter.FieldCheck('object', validator=gcs.Gcs.UrlToBucketAndName)


