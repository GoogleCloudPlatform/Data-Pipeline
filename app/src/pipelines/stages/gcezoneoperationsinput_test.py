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

"""Zone operations stage unit tests."""

import datetime


from src import basetest
from src.pipelines.stages.gcezoneoperationsinput import GceZoneOperationsInput


class GceZoneOperationsInputTest(basetest.TestCase):

  def testStoredOperationsQueryStringOneDay(self):
    date = datetime.datetime(2014, 1, 16)
    query_str = GceZoneOperationsInput._StoredOperationsQueryString(
        'ds', 'tb', 'zn', date, 1)
    self.assertEqual('SELECT id, insertTime FROM [ds.tb] WHERE '
                     'zoneName = \'zn\' AND DATE(insertTime) = \'2014-01-16\' '
                     'ORDER BY insertTime DESC', query_str)

  def testStoredOperationsQueryStringTwoDay(self):
    date = datetime.datetime(2014, 1, 16)
    query_str = GceZoneOperationsInput._StoredOperationsQueryString(
        'ds', 'tb', 'zn', date, 2)
    self.assertEqual('SELECT id, insertTime FROM [ds.tb] WHERE '
                     'zoneName = \'zn\' AND DATE(insertTime) = \'2014-01-16\' '
                     'OR DATE(insertTime) = \'2014-01-15\' '
                     'ORDER BY insertTime DESC', query_str)

  def testListZoneOperationsFilterOneDay(self):
    date = datetime.datetime(2014, 1, 16)
    filter_str = GceZoneOperationsInput._ListZoneOperationsFilter(
        date, num_days=1)
    self.assertEqual('insertTime eq 2014-01-16.*', filter_str)

  def testListZoneOperationsFilterTwoDays(self):
    date = datetime.datetime(2014, 1, 16)
    filter_str = GceZoneOperationsInput._ListZoneOperationsFilter(
        date, num_days=2)
    self.assertEqual('insertTime eq 2014-01-16.*|2014-01-15.*', filter_str)

  def testListZoneOperationsFilterThreeDays(self):
    date = datetime.datetime(2014, 1, 16)
    filter_str = GceZoneOperationsInput._ListZoneOperationsFilter(
        date, num_days=3)
    self.assertEqual('insertTime eq 2014-01-16.*|2014-01-15.*|2014-01-14.*',
                     filter_str)

  def testValidateBigQueryId(self):
    test_input = GceZoneOperationsInput({})
    # If valid, function did not raise an exception and return None.
    self.assertIsNone(test_input.ValidateBigQueryId('table'))
    self.assertIsNone(test_input.ValidateBigQueryId('test_table'))
    self.assertIsNone(test_input.ValidateBigQueryId('test_table_1'))

  def testValidateBigQueryIdBadId(self):
    test_input = GceZoneOperationsInput({})
    self.assertRaises(ValueError, test_input.ValidateBigQueryId, 'table$id')
    self.assertRaises(ValueError, test_input.ValidateBigQueryId, '123-table')
    self.assertRaises(ValueError, test_input.ValidateBigQueryId, '123:table')
    self.assertRaises(ValueError, test_input.ValidateBigQueryId, '')


if __name__ == '__main__':
  basetest.main()
