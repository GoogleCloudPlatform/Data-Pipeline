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

"""Output data to Big Query."""

import json
import logging

from src.clients import bigquery
from src.pipelines import pipeline


class BigQueryOutput(pipeline.Pipeline):
  """Output data to Big Query."""

  @staticmethod
  def GetHelp():
    return """Load Data into BigQuery.

The stage config should look like this:

```python
{
  "type": "BigQueryOutput",
  "destinationTable": {
    "projectId": "...",
    "tableId": "...",
    "datasetId": "..."
  },
  "sourceFormat": "CSV",
  "schema": {
    "fields": [{"type": "STRING", "name": "col_1"},
               ...
              ]
  },
  "createDisposition": "CREATE_IF_NEEDED",
  "writeDisposition": "WRITE_APPEND"
}
```

You may notice this syntax is very similar to the
[BigQuery Jobs
Syntax](https://developers.google.com/bigquery/docs/reference/v2/jobs).
"""

  def run(self, config):
    """Load data into a Big Query table.

    Args:
      config: Specifies where the data will be loaded.
    """

    logging.info('BigQueryOutput.Pipeline start\n%s',
                 json.dumps(config, indent=4, separators=(',', ': ')))

    bq = bigquery.BigQuery(config['destinationTable']['projectId'])

    # make sure the dataset exists (this is fine if it already exists).
    bq.CreateDataset(config['destinationTable']['datasetId'])

    bq.CreateTable(config['destinationTable']['datasetId'],
                   config['destinationTable']['tableId'],
                   config['schema']['fields'],
                   config['sources'][0],
                   source_format=config.get('sourceFormat'))

  def Lint(self, linter):
    """Stage-specific configuration linting."""
    linter.FieldCheck('destinationTable', field_type=dict, required=True)
    linter.FieldCheck('destinationTable.projectId', required=True)
    linter.FieldCheck('destinationTable.tableId', required=True)
    linter.FieldCheck('destinationTable.datasetId', required=True)
    linter.FieldCheck('schema', field_type=dict, required=True)
    linter.FieldCheck('schema.fields', field_type=list, required=True)


