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

"""Gets Compute Engine instances data across all given zones."""

import contextlib
import cStringIO as StringIO
import json
import logging
import time

from src.clients import computeengine
from src.clients import gcs
from src.pipelines import pipeline


class GceInstancesInput(pipeline.Pipeline):
  """Provides data from Compute Engine API as input to a pipeline."""

  @staticmethod
  def GetHelp():
    return """Get Data from Compute Engine List Instances API.

The stage config should look like this:

```python
{
  "type": "GceInstancesInput",
  "apiInput": {
    "projectId": "{{ app.id }}"
   },
  "zones": [
    "us-central1-a",
    "us-central1-b",
    "europe-west1-b",
    "europe-west1-b"
  ],
  "fields": "API fields selector"
}
```

* Provide all zones containing instances to capture complete snapshot.
* Any 'sources' for this stage config will be ignored.
"""

  def run(self, config):
    """Runs the stage.

    Args:
      config: Specifies the projectID and required zones.
    """
    logging.info('GceInstancesInput.Pipeline start\n%s',
                 json.dumps(config, indent=4, separators=(',', ': ')))

    storage = gcs.Gcs()
    gce = computeengine.ComputeEngine(config['apiInput']['projectId'])

    replies = []

    start_time = int(time.mktime(time.localtime()))
    for zone in config['zones']:
      replies.append(gce.ListInstances(zone=zone, fields=config['fields']))
    end_time = int(time.mktime(time.localtime()))

    with contextlib.closing(StringIO.StringIO()) as buf:
      for reply in replies:
        for instance in reply:
          #  Insert start and end snapshot timestamps
          instance['snapshotStartTime'] = start_time
          instance['snapshotEndTime'] = end_time
          json.dump(instance, buf)
          buf.write('\n')
      buf.seek(0)
      storage.InsertObject(buf, url=config['sinks'][0])

  def Lint(self, linter):
    """Stage-specific configuration linting."""
    linter.AtLeastOneFieldRequiredCheck(['apiInput', 'zones'])
    linter.FieldCheck('apiInput', field_type=dict)
    linter.FieldCheck('apiInput.projectId')
    linter.FieldCheck('zones', field_type=list, list_min=1)
    linter.FieldCheck('fields', required=True)



