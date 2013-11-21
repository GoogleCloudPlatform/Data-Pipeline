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

"""Unit tests for the Model utilities."""

import json

import logging
from src import basetest
from src.clients import bigquery
from src.csvmatchreplace import transform


class TransformTest(basetest.TestCase):
  """Run all the ModelTests."""

  def testTransformRow(self):
    row1 = ['true', 'hue', 'blue']

    config = {'columns': [{'type': bigquery.ColumnTypes.STRING,
                           'wanted': True,
                           'transformations': [
                               {'match': 'ue', 'replace': 'oo'},
                               {'match': 'lo', 'replace': 'ol'}]},
                          {'type': bigquery.ColumnTypes.STRING,
                           'wanted': False},
                          {'type': bigquery.ColumnTypes.STRING,
                           'wanted': True,
                           'transformations': [
                               {'match': 'b', 'replace': 'k'}]}]}

    # perform transformations that include removing a column
    (transformed_row, bad_columns) = transform.TransformRow(row1, config)
    self.assertEqual(len(bad_columns), 0)
    # transformed_row should only have 2 cells
    self.assertEqual(len(transformed_row), 2)
    # transformed_row should equal ['troo', 'klue']
    self.assertEqual(transformed_row, ['troo', 'klue'])

    config['columns'][1]['wanted'] = True

    # perform transformations that don't include removing columns
    (transformed_row2, bad_columns2) = transform.TransformRow(row1, config)
    self.assertEqual(len(bad_columns2), 0)
    # transformed_row should all 3 cells
    self.assertEqual(len(transformed_row2), 3)
    # transformed_row should equal ['troo', 'hue', 'klue']
    self.assertEqual(transformed_row2, ['troo', 'hue', 'klue'])

  def NOPEtestTransformRowWithBadData(self):
    # TODO(user) get this test working again.
    row1 = ['true', 'hue', 'blue']

    t = {}  # table.Table()
    t.column_types = [bigquery.ColumnTypes.STRING,
                      bigquery.ColumnTypes.INTEGER,
                      bigquery.ColumnTypes.INTEGER]
    t.transformations = json.dumps([
        # col 1 has 2 transformations
        [{'match': 'ue', 'replace': 'oo'},
         {'match': 'lo', 'replace': 'ol'}],
        # col 2 has no transformation)
        [],
        # col 3 has 1 transformation
        [{'match': 'b', 'replace': 'k'}]])

    # transformations = t.GetAllTransformations()
    t.column_wanted = [True, True, True]

    # perform transformations that don't include removing columns
    (bad_columns, _) = t.TransformRow(row1)
    # there should be two errors in bad columns
    self.assertEqual(len(bad_columns), 2)
    # index of first error should be 1
    self.assertEqual(bad_columns[0].index, 1)
    # index of second error should be 2
    self.assertEqual(bad_columns[1].index, 2)


class TestNormalizeCellByType(basetest.TestCase):

  def testNormalizeCellByType(self):
    types = bigquery.ColumnTypes

    # string tests
    self.assertEqual('', transform.NormalizeCellByType('', 0, types.STRING))

    # int tests
    self.assertEqual('', transform.NormalizeCellByType('', 1, types.INTEGER))
    self.assertEqual('1', transform.NormalizeCellByType('1', 1, types.INTEGER))
    self.assertRaises(transform.CellError,
                      transform.NormalizeCellByType, 'ark', 1, types.INTEGER)

    # float tests
    self.assertEqual('', transform.NormalizeCellByType('', 2, types.FLOAT))
    self.assertEqual('1.1',
                     transform.NormalizeCellByType('1.1', 2, types.FLOAT))
    self.assertRaises(transform.CellError,
                      transform.NormalizeCellByType, 'ark', 2, types.FLOAT)

    # bool tests
    self.assertEqual('', transform.NormalizeCellByType('', 3, types.BOOLEAN))
    self.assertEqual('True',
                     transform.NormalizeCellByType('true', 3, types.BOOLEAN))
    self.assertEqual('True',
                     transform.NormalizeCellByType('TRUE', 3, types.BOOLEAN))
    self.assertEqual('True',
                     transform.NormalizeCellByType('1', 3, types.BOOLEAN))
    self.assertEqual('False',
                     transform.NormalizeCellByType('False', 3, types.BOOLEAN))
    self.assertRaises(transform.CellError,
                      transform.NormalizeCellByType, 'ark', 3, types.BOOLEAN)

    # timestamp tests
    self.assertEqual('', transform.NormalizeCellByType('', 4, types.TIMESTAMP))
    self.assertEqual('2013-06-06 00:00:00.000000 ',
                     transform.NormalizeCellByType('2013-06-06', 4,
                                                   types.TIMESTAMP))
    self.assertRaises(transform.CellError,
                      transform.NormalizeCellByType, 'ark', 4, types.TIMESTAMP)

  def testCellsAsString(self):
    tests = (('', []),
             ('a', ['a']),
             ('a,b', ['a', 'b']),
             ('"a""b",c', ['a"b', 'c']),
             ('a\'b,c', ['a\'b', 'c']),
             ('"a,b",c', ['a,b', 'c']),
            )
    for expected, row in tests:
      self.assertEquals(expected, transform.CellsToCsvString(row))


if __name__ == '__main__':
  basetest.main()
