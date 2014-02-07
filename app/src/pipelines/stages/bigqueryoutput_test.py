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

"""Pipeline stage unit tests."""

import mock



import logging
from src import basetest
from src.clients import bigquery
from src.pipelines.stages import bigqueryoutput


class BigQueryOutputTest(basetest.TestCase):

  def testRun(self):
    config = {
        'destinationTable': {
            'projectId': '99',
            'datasetId': 'ark',
            'tableId': 'arktable'
            },
        'schema': {
            'fields': []
            },
        'sources': ['gs://test/example']
        }
    mock_bq = mock.MagicMock()
    bigquery.BigQuery.return_value = mock_bq

    mock_bq.CreateDataset.return_value(None)
    mock_bq.CreateTable.return_value(None)

    with mock.patch.object(bigquery,
                           'BigQuery',
                           return_value=mock_bq):
      bqo = bigqueryoutput.BigQueryOutput(config)
      bqo.start_test()

    mock_bq.CreateDataset.assert_called_once_with('ark')
    mock_bq.CreateTable.assert_called_once_with('ark', 'arktable', [],
                                                'gs://test/example',
                                                source_format=None)

  def testRunSourceFormat(self):
    config = {
        'destinationTable': {
            'projectId': '99',
            'datasetId': 'ark',
            'tableId': 'arktable'
            },
        'sourceFormat': 'ChrisSpecialValues',
        'schema': {
            'fields': []
            },
        'sources': ['gs://test/example']
        }
    mock_bq = mock.MagicMock()
    bigquery.BigQuery.return_value = mock_bq

    mock_bq.CreateDataset.return_value(None)
    mock_bq.CreateTable.return_value(None)

    with mock.patch.object(bigquery,
                           'BigQuery',
                           return_value=mock_bq):
      bqo = bigqueryoutput.BigQueryOutput(config)
      bqo.start_test()

    mock_bq.CreateDataset.assert_called_once_with('ark')
    mock_bq.CreateTable.assert_called_once_with(
        'ark', 'arktable', [], 'gs://test/example',
        source_format='ChrisSpecialValues')


if __name__ == '__main__':
  basetest.main()
