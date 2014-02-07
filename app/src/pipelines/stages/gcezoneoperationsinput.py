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

"""Gets Compute Engine zone operations data."""

import contextlib
import cStringIO as StringIO
import datetime
import json
import logging
import re
import sets
import time

from src.clients import bigquery
from src.clients import computeengine
from src.clients import gcs
from src.pipelines import pipeline


class GceZoneOperationsInputException(Exception):
  """Error on getting GCE zone operations."""


class GceZoneOperationsInput(pipeline.Pipeline):
  """Provides data from Compute Engine API as input to a pipeline."""

  @staticmethod
  def GetHelp():
    return """Get Data from Compute Engine Zone Operations.list API.

The stage config should look like this:

```python
{
  "type": "GceZoneOperationsInput",
  "destinationTable": {
        "projectId": "{{ app.id }}",
        "tableId": "workingoperationstable",
        "datasetId": "examples"
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
* The destinationTable specifies the BigQuery table used to store all
  operations and is queried initially to obtain currently stored operations.
* API fields selector is used to specify the fields to be returned by
  the Compute Engine API call. As an example, only nextPageToken and
  items.status and items.sourceSnapshot will be returned by the API call
  with this filter - 'nextPageToken,items(status,sourceSnapshot)'.
"""

  # Regular expression for validating BigQuery dataset and table id.
  VALID_BIGQUERY_ID = re.compile(r'^\w+$')

  def run(self, config):
    """Runs the stage.

    Args:
      config: Specifies the projectID and required zones.
    """
    logging.info('GceZoneOperationsInput.Pipeline start\n%s',
                 json.dumps(config, indent=4, separators=(',', ': ')))

    # To obtain new operations that are not added to BigQuery, all
    # operations in BigQuery over the past 3 days are obtained.
    # The Compute Engine Operations API is called for the past 2 days.
    # All operations from the API are checked against the BigQuery
    # data to verify they are already recorded. If they are new, they
    # are added to a list which will be ingested to BigQuery.
    # 2 days of operations are obtained from Operations API to guarantee
    # operations which occured at the end of the day will be obtained.
    # 3 days of BigQuery data is obtained to handle the timezone difference.

    today_date = datetime.date.today()
    filter_str = GceZoneOperationsInput._ListZoneOperationsFilter(
        today_date, num_days=2)
    logging.info('Filter string for operations: %s', filter_str)

    gce = computeengine.ComputeEngine(config['destinationTable']['projectId'])
    new_operations = []
    start_time = int(time.mktime(time.localtime()))
    for zone in config['zones']:
      stored_operations = GceZoneOperationsInput._GetStoredOperations(
          zone=zone, config=config, date=today_date, number_days=3)
      logging.debug('Found %d operation ids already stored for zone %s.',
                    len(stored_operations), zone)

      while True:
        operations, next_page_token = gce.ListZoneOperations(
            zone, filter_expression=filter_str)
        for operation in operations:
          if operation['id'] not in stored_operations:
            new_operations.append(operation)
        if not next_page_token:
          break
    end_time = int(time.mktime(time.localtime()))
    logging.info('All pages of operation reviewed, adding %d operations.',
                 len(new_operations))

    storage = gcs.Gcs()
    with contextlib.closing(StringIO.StringIO()) as buf:
      for operation in new_operations:
        #  Inserts start and end snapshot timestamps.
        operation['snapshotStartTime'] = start_time
        operation['snapshotEndTime'] = end_time
        # Indicates that this is a zone resource.
        operation['resourceType'] = 'zone'
        json.dump(operation, buf)
        buf.write('\n')
      buf.seek(0)
      storage.InsertObject(buf, url=config['sinks'][0])

  @staticmethod
  def _GetStoredOperations(zone, config, date, number_days=3):
    """Returns a Set of operation ids for the specified dates.

    Args:
      zone: String of zone name.
      config: Specifies the projectID and required zones.
      date: datetime.date of day to obtain operations.
      number_days: Integer for the number of previous days to query, inclusive
        of date.

    Returns:
      Set of operation ids already in BigQuery

    Raises:
      GceZoneOperationsInputException: Incorrect parameter or BigQuey table
          does not exist.
    """
    if number_days < 1:
      raise GceZoneOperationsInputException(
          'GetOperationsForDays number_days is less than one: %d' % number_days)

    operation_ids = sets.Set()
    if not GceZoneOperationsInput._BigQueryTableExists(config):
      # Table does not exist, return a blank set.
      logging.info('%s does not exist in dataset %s.',
                   config['destinationTable']['datasetId'],
                   config['destinationTable']['tableId'])
      return operation_ids

    bq = bigquery.BigQuery(config['destinationTable']['projectId'])
    query_str = GceZoneOperationsInput._StoredOperationsQueryString(
        config['destinationTable']['datasetId'],
        config['destinationTable']['tableId'], zone, date, number_days)
    logging.debug('BigQuery Query: %s', query_str)

    start_index = 0
    total_rows = 1   # total_rows will be set later after the first query.
    while start_index < total_rows:
      result = bq.Query(query_str, table_info=None, offset=start_index,
                        max_results=100)
      if len(result) != 3:
        raise GceZoneOperationsInputException.error(
            'Big Query query quit. Please review your Big Query query for '
            'query syntax.')

      _, _, reply = result
      total_rows = int(reply['totalRows'])
      reply_rows = reply.get('rows')

      if not reply_rows:
        logging.info('No BigQuery operation rows found for zone: %s. '
                     'for date %s', zone, date)
      else:
        # Add all operation ids from the reply
        operation_ids.update([row['f'][0]['v'] for row in reply_rows])
        start_index += len(reply_rows)

    return operation_ids

  @staticmethod
  def _BigQueryTableExists(config):
    """Checks if the BigQuery table as specified in the config exsits or not.

    Args:
      config: Specifies the projectID, dataset and the table id.

    Returns:
      The table specification if exits and None if it does not.
    """
    bq = bigquery.BigQuery(config['destinationTable']['projectId'])
    return bq.GetTable(config['destinationTable']['datasetId'],
                       config['destinationTable']['tableId'])

  @staticmethod
  def _StoredOperationsQueryString(dataset, table, zone, date, num_days):
    """Returns the SQL query string to get the set of operations in BigQuery.

    Args:
      dataset: dataset that contains the zone operations.
      table: table id of the zone operations.
      zone: zone for the operations.
      date: A datetime object for operations that were inserted on this date.
      num_days: Includes the number of days prior to date, inclusive of date.

    Returns:
      The BigQuery SQL string to get the operations for the given zone and
      date ranges.
    """
    date_sql = 'DATE(insertTime) = \'%s\'' % date.strftime('%Y-%m-%d')
    for day_increment in range(1, num_days):
      query_date = date - datetime.timedelta(days=day_increment)
      date_sql += (' OR DATE(insertTime) = \'%s\'' %
                   query_date.strftime('%Y-%m-%d'))
    return ('SELECT id, insertTime FROM [%s.%s] '
            'WHERE zoneName = \'%s\' AND %s '
            'ORDER BY insertTime DESC' % (dataset, table, zone, date_sql))

  @staticmethod
  def _ListZoneOperationsFilter(date, num_days=2):
    """Returns the filter for Compute Engine API for getting zone operations.

    When calling the Compute Engine API to ist zone operations, a filter can
    be specified to limit the returned result. This method creates the filter
    for limiting the result to the given date range.

    Args:
      date: A datetime object for operations that were inserted on this date.
      num_days: Includes the number of days prior to date, inclusive of date.

    Returns:
      The filter string to get the operations for the given zone and
      date ranges.
    """
    filter_str = 'insertTime eq %s.*' % date.strftime('%Y-%m-%d')
    for day_increment in range(1, num_days):
      filter_date = date - datetime.timedelta(days=day_increment)
      filter_str += '|%s.*' % filter_date.strftime('%Y-%m-%d')
    return filter_str

  def Lint(self, linter):
    """Stage-specific configuration linting.

    Datapipeline calls this to validate the pipeline configuration for this
    stage.

    Args:
      linter: A linter object that checks this stage configuation.
    """
    linter.AtLeastOneFieldRequiredCheck(['zones'])
    linter.FieldCheck('destinationTable', field_type=dict, required=True)
    linter.FieldCheck('destinationTable.projectId', required=True)
    linter.FieldCheck('destinationTable.tableId', required=True,
                      validator=self.ValidateBigQueryId)
    linter.FieldCheck('destinationTable.datasetId', required=True,
                      validator=self.ValidateBigQueryId)
    linter.FieldCheck('zones', field_type=list, list_min=1)
    linter.FieldCheck('fields', required=True)

  def ValidateBigQueryId(self, bigquery_id):
    if not GceZoneOperationsInput.VALID_BIGQUERY_ID.match(bigquery_id):
      raise ValueError('%s is not a valid BigQuery ID.' % bigquery_id)
