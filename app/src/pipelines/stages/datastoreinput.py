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
import csv


from google.appengine.ext import ndb

from src.clients import gcs
from src.pipelines import pipeline


class DatastoreInput(pipeline.Pipeline):
  """Provides data from Datastore as input to a pipeline."""

  @staticmethod
  def GetHelp():
    return """Get Data from App Engine Datastore.

The stage config should look like this:

```python
{
  "gql": "embedded GQL query string",
  "object": "gs://bucket_name/object_name",
  "params: {
    "values": {},
    "consistency": "strong"|"eventual"
    "keysOnly": boolean,
    "projection": [properties],
  },
}
```

* Either a gql string or an object that contains the gql query is required.
* If both are provided the inline GQL will be used.
* Any 'sources' for this stage config will be ignored.
"""

  def run(self, config):
    """Runs the stage.

    Args:
      config: Specifies the either an inline GQL query or a GCS object that
        contains a GQL query.
    """
    storage = gcs.Gcs()
    gql = config.get('gql')
    if not gql:
      with contextlib.closing(
          storage.OpenObject(url=config['object'])) as stream:
        with contextlib.closing(StringIO.StringIO()) as gql_buf:
          while True:
            buf = stream.read(gcs.Gcs.READ_CHUNK_SIZE)
            if buf and len(buf):
              gql_buf.write(buf)
            else:
              break
          gql = gql_buf.getvalue()

    qkwargs = {}
    consistency = None
    keys_only = False
    projection = None
    if 'params' in config:
      params = config['params']
      qkwargs = params.get('values', {})
      consistency = params.get('consistency')
      if 'consistency' in params and params['consistency'] is 'eventual':
        consistency = ndb.EVENTUAL_CONSISTENCY
      keys_only = params.get('keysOnly', False)
      projection = params.get('projection')

    # for now just emit a dumb CSV
    # TODO(user): better way to decide output type.
    # TODO(user): make robust - don't always want to spool into memory...
    writer = None
    with contextlib.closing(StringIO.StringIO()) as buf:
      query = ndb.gql(gql, **qkwargs)
      for entity in query.iter(read_policy=consistency,
                               keys_only=keys_only,
                               projection=projection):
        if not projection:
          # pylint: disable=protected-access
          projection = entity._properties.keys()
        if not writer:
          writer = csv.DictWriter(buf, projection)
          headers = dict((p, p) for p in projection)
          writer.writerow(headers)
        writer.writerow(entity.to_dict())

      # TODO(user): what to do with multiple sinks?
      buf.seek(0)
      storage.InsertObject(buf, url=config['sinks'][0])

  def Lint(self, linter):
    """Stage-specific configuration linting."""
    linter.AtLeastOneFieldRequiredCheck(['gql', 'object'])
    linter.FieldCheck('gql')
    linter.FieldCheck('object', validator=gcs.Gcs.UrlToBucketAndName)
    linter.FieldCheck('params', field_type=dict)
    linter.FieldCheck('params.values', field_type=dict)
    linter.FieldCheck('params.projection', field_type=list, list_min=1)
    linter.FieldCheck('params.consistency', validator=self.ValidateConsistency)

  def ValidateConsistency(self, consistency):
    if consistency != 'strong' and consistency != 'eventual':
      raise ValueError(
          'Expected "strong" or "eventual" but got %r' % consistency)

