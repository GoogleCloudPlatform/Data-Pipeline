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

"""Functions to transform data."""

import cStringIO as StringIO
import csv
import json
import logging
import re

from src.clients import bigquery
from src.csvmatchreplace import timestamp


class TableError(Exception):
  """An error related to the Table class."""


class CellError(TableError):
  """An error with a cell."""

  def __init__(self, message, value=None, index=None):
    """Make a Cell Error.

    Args:
      message: the message about this error
      value: ptional value we had a problem with.
      index: optional index into the row where this cell error came from.
    """
    super(CellError, self).__init__(message)
    self.value = value
    self.index = index


def TransformRow(row, config):
  """Performs transformations on row.

  Args:
    row: an array of string values.
    config: the config for transform from table.AsDataPipelineJsonDict.

  Returns:
    A tuple of new transformed values that is the result of
    performing operations based on the contents transformations and
    wanted_cols and an array of bad_column errors.
  """

  transformed_row = []
  bad_columns = []
  columns = config['columns']

  if len(row) != len(columns):
    bad_columns.append(CellError(
        'Invalid number of elements in row. Found %d, expected %d' %
        (len(row), len(columns))))
  for i in range(min(len(row), len(columns))):
    if columns[i]['wanted']:
      try:
        cell_data = TransformCell(row[i], i, columns[i])
        transformed_row.append(cell_data)
        # logging.info('Transform phase: Column %d = %s', i, cell_data)
      except CellError as err:
        logging.warning('Transform phase: Bad data @ Column %d = %r', i,
                        err)
        bad_columns.append(err)  # save error
        transformed_row.append(err.value)
        # possible partial transformation

  return (transformed_row, bad_columns)


def TransformCell(cell, index, column):
  """Performs transformation(s) on an individual cell.

  Args:
    cell: A unit of data to be processed.
    index: which column are we transforming.
    column: The column dict from columns from the AsDataPipelineJsonDict
  Returns:
    A new cell that is the result of performing operations based on
    the contents of the transformation at the provided index.
  Raises:
    CellError if there is an error with this cell.
  """
  output = cell
  for pattern in column.get('transformations', []):
    output = re.sub(pattern['match'], pattern['replace'], output)
  output = NormalizeCellByType(output, index, column['type'])
  return output


def NormalizeCellByType(cell, index, column_type):
  """Make sure the cell value is valid for the column_type."""
  if not cell:
    return ''
  try:
    if column_type == bigquery.ColumnTypes.INTEGER:
      cell = int(cell)
    elif column_type == bigquery.ColumnTypes.FLOAT:
      cell = float(cell)
    elif column_type == bigquery.ColumnTypes.BOOLEAN:
      if str(cell).lower() in ('true', '1'):
        cell = 'True'
      elif str(cell).lower() in ('false', '0'):
        cell = 'False'
      else:
        raise ValueError('invalid value')
    elif column_type == bigquery.ColumnTypes.TIMESTAMP:
      cell = timestamp.NormalizeTimeStamp(cell)

  except ValueError as err:
    raise CellError('Invalid value %r for column type %s: %r' %
                    (cell, bigquery.ColumnTypes.strings[column_type], err),
                    str(cell), index)
  return str(cell)


def WriteErrors(writer, row_value, errors):
  """Write out row and errors with it for later _badrows table creation.

  Args:
    writer: an object we can write to.
    row_value: the entire row we had a problem with.
    errors: an array of CellError objects.
  """
  row = {'row_value': row_value,
         'errors': [{'message': err.message,
                     'value': err.value,
                     'index': err.index} for err in errors]}
  writer.write(json.dumps(row) + '\r\n')


def CellsToCsvString(row):
  """Convert a row of cell strings into a csv joined string."""
  o = StringIO.StringIO()
  csv_writer = csv.writer(o)
  csv_writer.writerow(row)
  return o.getvalue().splitlines()[0]  # strip off the trailing \r\n
