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

"""Bigquery client library."""

import collections
import json
import logging
import pprint
import string
import time

from apiclient.errors import HttpError
from oauth2client.client import AccessTokenRefreshError

from src import auth


class ColumnTypes(object):
  """The various types of Bigquery Fields."""

  EMPTY = 0
  STRING = 1
  INTEGER = 2
  FLOAT = 3
  BOOLEAN = 4
  TIMESTAMP = 5

  strings = collections.OrderedDict(((EMPTY, 'Empty'),
                                     (STRING, 'STRING'),
                                     (INTEGER, 'INTEGER'),
                                     (FLOAT, 'FLOAT'),
                                     (BOOLEAN, 'BOOLEAN'),
                                     (TIMESTAMP, 'TIMESTAMP')))

  @staticmethod
  def ToString(idx):
    return ColumnTypes.strings.get(idx, 'UNKNOWN_TYPE')


class SourceFormatTypes(object):
  """The types of BigQuery Input source formats."""

  CSV_FORMAT = 'CSV'
  JSON_FORMAT = 'NEWLINE_DELIMITED_JSON'
  DEFAULT_FORMAT = CSV_FORMAT

  strings = collections.OrderedDict(
      ((CSV_FORMAT, 'CSV'),
       (JSON_FORMAT, 'NEWLINE_DELIMITED_JSON')))

  @staticmethod
  def ToString(idx):
    return SourceFormatTypes.strings.get(idx, 'UNKNOWN_TYPE')


class BigQueryError(Exception):
  pass


class BigQuery(object):
  """A class for accessing bigquery."""
  AUTH_SCOPE = 'https://www.googleapis.com/auth/bigquery'
  DATASTORE = 'DATASTORE_BACKUP'

  def __init__(self, project_id):
    """Create a BigQuery class.

    Args:
      project_id: The bigquery project id.
    """
    self.project_id = project_id
    self.bigquery = auth.Service.FromServiceAccount('bigquery', 'v2',
                                                    self.AUTH_SCOPE)

  def CreateDataset(self, dataset_id, email=None):
    """Makes a dataset in BigQuery and shares it with email."""
    try:
      datasets = self.bigquery.datasets()
      body = {'description': 'Data Pipeline Dataset',
              'datasetReference': {
                  'projectId': self.project_id,
                  'datasetId': dataset_id}}
      if email:
        body['access'] = [{'role': 'READER', 'userByEmail': email}]
      reply = datasets.insert(projectId=self.project_id, body=body).execute()
      logging.info('datasets.insert():\n %s', pprint.pformat(reply))
    except HttpError as err:
      if '409' in str(err) and 'Already Exists' in str(err):
        logging.info('dataset (%r) already exists: %r', dataset_id, err)
        return
      logging.error('ERROR creating dataset (%r): %r', dataset_id, err)
      raise err

  def GetDatasets(self):
    try:
      datasets = self.bigquery.datasets()
      list_reply = datasets.list(projectId=self.project_id).execute()
      logging.info('Datasets: %r', pprint.pformat(list_reply))
      return list_reply
    except HttpError as err:
      logging.error('ERROR getting datasets: %r', err)
    return None

  def CreateTable(self, dataset_id, table_name, fields, src_file,
                  source_format=None,
                  skip_leading_rows=0):
    """Create a table in a dataset in BigQuery."""
    try:
      job_collection = self.bigquery.jobs()

      verified_source_format = None

      if source_format is None:
        verified_source_format = SourceFormatTypes.DEFAULT_FORMAT
      else:
        verified_source_format = SourceFormatTypes.ToString(
            source_format.upper())
        if verified_source_format == 'UNKNOWN_TYPE':
          logging.error('Invalid BigQuery source format: %s.',
                        source_format)
          raise BigQueryError('BigQuery import abandoned: '
                              'invalid source format.')

      logging.info('table fields are: %r', fields)

      job_data = {
          'projectId': self.project_id,
          'configuration': {
              'load': {
                  'sourceUris': [src_file],
                  'sourceFormat': verified_source_format,
                  'schema': {
                      'fields': fields,
                      },
                  'destinationTable': {
                      'projectId': self.project_id,
                      'datasetId': dataset_id,
                      'tableId': table_name,
                      },
                  'maxBadRecords': 10000,
                  }
              }
          }
      if skip_leading_rows:
        job_data['configuration']['load']['skipLeadingRows'] = skip_leading_rows

      insert_response = job_collection.insert(projectId=self.project_id,
                                              body=job_data).execute()

      logging.info('insert_response is : %s', pprint.pformat(insert_response))

      # Ping for status until it is done, with a short pause between calls.
      while True:
        job_id = insert_response['jobReference']['jobId']
        job = job_collection.get(projectId=self.project_id,
                                 jobId=job_id).execute()
        logging.info('job is : %s', pprint.pformat(job))

        if 'DONE' == job['status']['state']:
          logging.info('Done Loading! Stats: %r', job.get('statistics'))
          return True
        if 'errorResult' in job['status']:
          logging.error('Error loading table: %s', pprint.pformat(job))
          return False

        logging.info('Waiting for loading to complete...')
        # TODO(user) update the Data Pipeline Stage about the loading...
        time.sleep(10)

    except HttpError as err:
      logging.error('Error in loadTable: %r', err)
      raise err
    return False

  def DeleteTable(self, dataset_id, table_id):
    """Delete a table from bigquery."""
    try:
      self.bigquery.tables().delete(projectId=self.project_id,
                                    datasetId=dataset_id,
                                    tableId=table_id).execute()
      return True
    except HttpError as err:
      logging.error('Error in DeleteTable: %s', pprint.pformat(err.content))
    return False

  def GetTable(self, dataset_id, table_id):
    """Get a table's definition (or None) from bigquery."""
    try:
      table = self.bigquery.tables().get(projectId=self.project_id,
                                         datasetId=dataset_id,
                                         tableId=table_id).execute()
      return table
    except HttpError as err:
      error = json.loads(err.content).get('error')
      if error and error.get('code') == 404:
        logging.info('GetTable: %s.%s not found', dataset_id, table_id)
        return None
      else:
        logging.error('Error in GetTable: %s', pprint.pformat(err.content))
        raise err

  def Query(self, query,
            table_info=None, offset=0, max_results=100, timeout=0):
    """Query a table, pagination is supported with optional args.

    Args:
      query: The query string.
      table_info: A dict that contains dataset, project and table ID.
      offset: Starting index from where to read results from.
      max_results: Maximum results to query for a page.
      timeout: Time to wait while polling for completeness.

    Returns:
      A tuple of table ID, table schema and query reply.
    """
    try:
      if table_info is None:
        logging.info('timeout:%d', timeout)
        job_collection = self.bigquery.jobs()
        query_data = {'query': query, 'timeoutMs': timeout}

        query_reply = job_collection.query(projectId=self.project_id,
                                           body=query_data).execute()

        job_reference = query_reply['jobReference']
        job_config = job_collection.get(**job_reference).execute()

        while job_config['status']['state'] != 'DONE':
          if 'errorResult' in job_config['status']:
            logging.error('Job has failed.')
            return
          time.sleep(1)
          job_config = job_collection.get(**job_reference).execute()

        # query results are stored in a temp table, get the temp table config.
        # This will let us paginate the results.
        table_info = job_config['configuration']['query']['destinationTable']

      # TODO(user): Temp tables expire after 24 hours.
      # Recreate temp table if query is 24 hours old by finding out
      # creation time from table stats.
      schema = self.GetTable(table_info['datasetId'], table_info['tableId'])

      reply = self.bigquery.tabledata().list(
          maxResults=max_results, startIndex=offset, **table_info).execute()
      return (table_info, schema, reply)
    except AccessTokenRefreshError as err:
      logging.error('The credentials have been revoked or expired, please '
                    're-run the application to re-authorize: %r',
                    pprint.pformat(err.content))
    except HttpError as err:
      logging.error('Error in RunSyncQuery: %s', pprint.pformat(err.content))

      err_dict = json.loads(err.content)
      raise BigQueryError(err_dict['error']['message'])
    except Exception as err:
      logging.error('Undefined error %r', err)


def MakeValidFieldName(header):
  """Turn a header name into a valid bigquery field name.

  https://developers.google.com/bigquery/docs/tables

  Field names are any combination of uppercase and/or lowercase
  letters (A-Z, a-z), digits (0-9) and underscores, but no spaces. The
  first character must be a letter.

  Args:
    header: the possible header value
  Returns:
    a sanitized version of header with only letters, digits or _
    If the result doesn't start with a letter it's prefixed with col_
    This could return an empty string if header has no valid characters.
  """
  valid_chars = string.ascii_letters + string.digits + '_'
  header = ''.join(x in valid_chars and x or '_' for x in header)
  header = header.strip('_')
  while '__' in header:
    header = header.replace('__', '_')
  if header and header[0] not in string.ascii_letters:
    header = 'col_' + header
  return header


def MakeValidTableName(name):
  """Turn name string into a valid bigquery table name.

  Table names (or tableId) can be any letter or number or _ but cannot
  start with a number.

  Args:
    name: the table name you wish to sanitize.
  Returns:
    a sanitized version of name with only letters, digits or _
  """
  allowed_characters = string.ascii_letters + string.digits + '_'
  name = ''.join(x for x in name if x in allowed_characters)
  if not name or name[0] in string.digits:
    name = '_' + name
  return name
