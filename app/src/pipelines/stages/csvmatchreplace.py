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

"""Transforms csv files."""

import csv
import json
import logging


import cloudstorage
from src.clients import gcs
from src.csvmatchreplace import transform
from src.pipelines import pipeline
from src.pipelines import shardstage


class CsvMatchReplace(shardstage.ShardStage):
  """Match and replace strings in csv files (Also remove columns)."""

  @staticmethod
  def GetHelp():
    return """**CsvMatchReplace** will process a csv file.

It can remove columns, convert datatypes and parse datestrings.
It can also run a search/replace on column values using regular expressions.

The stage config should look like this:
```python
{
  "type": "CsvMatchReplace",
  "fieldDelimiter": ",",
  "columns": [{
    "wanted": true,
    "type": "STRING",
    "name": "col_1",
    "transformations": [{
      "match": "a",
      "replace": "pp"
    }, ...],
  }, ...],
  "skipLeadingRows": 0,
  "start": first_byte,
  "length": number_of_bytes,
  "shardSize": number_of_bytes,
  "sinks": ["gs://bucket_name/results", "gs://bucket_name/badrows"]
}
```

* The second sink is optional and will contain all the bad (unprocessed)
rows from the input.
* start and length are also optional and used when this task is sharded.
* If shardSize is specified this stage will be split up into jobs
that are that big and then the results composited together.
"""

  CHUNK_SIZE_1MB = 1 << 20
  CHUNK_SIZE_2MB = 1 << 21
  CHUNK_SIZE_4MB = 1 << 22
  SHARD_CHUNK_SIZE = CHUNK_SIZE_4MB

  def run(self, config):
    """Transform data according to some search/replace patterns from config.

    Args:
      config: Specifies what to replace and which columns are wanted.
    Yields:
      possibly yields some sharded stages.
    """

    # quick check to skip the leading rows
    skip_leading_rows = config.get('skipLeadingRows', 0)
    start = config.get('start', 0)
    source_url = config['sources'][0]

    if 'length' not in config:
      config['length'] = gcs.Gcs().StatObject(
          url=source_url)['size'] - start

    if skip_leading_rows > 0 and start == 0:
      # We're skipping these rows by using the start parameter.
      config['skipLeadingRows'] = 0
      bytes_to_skip = FindStartAfterSkippingRows(skip_leading_rows,
                                                 source_url)
      # We start one byte back since we find the start of the next
      # line when we start processing (to avoid jumping in at the
      # start of a line.
      config['start'] = max(0, bytes_to_skip - 1)
      if 'length' in config:
        config['length'] -= bytes_to_skip

    if 'shardSize' not in config:
      config['shardSize'] = self.SHARD_CHUNK_SIZE

    (shards, compositors) = self.ShardStage(config)
    if shards and compositors:
      with pipeline.After(*[(yield shard) for shard in shards]):
        _ = [(yield compositor) for compositor in compositors]
    else:
      # TODO(user) handle this task dying halfway through and resuming.
      # TODO(user) handle some way to update progress (memcache!?)

      # TODO(user) if the input file/blob is over 10M split it into
      # chunks and run over those and then merge it. Should be easy with
      # Pipelines.

      logging.info('Transformer start\n%s',
                   json.dumps(config, indent=4, separators=(',', ': ')))

      logging.info('CsvMatchReplace called for = %s', source_url)

      sink_url = config['sinks'][0]
      if len(config['sinks']) > 1:
        badrows_url = config['sinks'][1]
      else:
        badrows_url = None

      finished = ReadTransformWrite(config, source_url, sink_url, badrows_url)
      if not finished:
        logging.error('Unable to CsvMatchReplace')
        return

  def Lint(self, linter):
    """Stage-specific configuration linting."""
    linter.FieldCheck('fieldDelimiter', required=True)
    linter.FieldCheck('columns', field_type=list, required=True)


def FindStartAfterSkippingRows(skip_leading_rows, source_url):
  source_filename = gcs.Gcs.UrlToBucketAndNamePath(source_url)
  with cloudstorage.open(source_filename) as source_file:
    for _ in range(skip_leading_rows):
      source_file.readline()
    return source_file.tell()


def ReadTransformWrite(config, source_url, sink_url, badrows_url=None):
  """Transformation from one GCS file into another.

  Args:
    config: the transform config section from table.AsDataPipelineJsonDict.
    source_url: The blob_key of the csv file to transform.
    sink_url: The gs://bucket/name url to write the output transformations to.
    badrows_url: (optional) The gs://bucket/name url to write the badrows to.
  Returns:
    True only if the function successfully runs to completion.
  """

  delimiter = str(config['fieldDelimiter'])

  source_filename = gcs.Gcs.UrlToBucketAndNamePath(source_url)
  sink_filename = gcs.Gcs.UrlToBucketAndNamePath(sink_url)

  logging.info('CsvMatchReplace %r -> %r', source_filename, sink_filename)

  start = config.get('start', 0)
  length = config.get('length', -1)

  with cloudstorage.open(source_filename) as source_file:
    with cloudstorage.open(sink_filename, 'w') as sink_file:
      if start > 0:
        source_file.seek(start)
        source_file.readline()
      csv_writer = csv.writer(sink_file)
      csv_reader = csv.reader(source_file, delimiter=delimiter)
      if length > 0:
        finished_func = lambda: source_file.tell() > start + length
      else:
        finished_func = None

      if badrows_url:
        badrows_filename = gcs.Gcs.UrlToBucketAndNamePath(badrows_url)
        with cloudstorage.open(badrows_filename, 'w') as badrows_file:
          (row_count, bad_row_count) = ReadTransformWriteRows(
              config, csv_reader, csv_writer, finished_func, badrows_file)
      else:
        (row_count, bad_row_count) = ReadTransformWriteRows(
            config, csv_reader, csv_writer, finished_func)

    logging.info('CsvMatchReplace complete. %d rows, %d bad.',
                 row_count, bad_row_count)
    return True


def ReadTransformWriteRows(config, csv_reader, csv_writer,
                           finished_func=None,
                           badrows_file=None):
  """Transform each from from the csv_reader into the csv_reader."""
  row_count = 0
  bad_row_count = 0
  for row in csv_reader:
    (transformed_row, bad_cols) = transform.TransformRow(row, config)
    if not bad_cols:
      csv_writer.writerow(transformed_row)
    else:
      bad_row_count += 1
      logging.debug('Row %d\'s bad cols = %r', row_count, bad_cols)
      if badrows_file:
        transform.WriteErrors(badrows_file,
                              transform.CellsToCsvString(row), bad_cols)
    row_count += 1
    if finished_func and finished_func():
      break
  return (row_count, bad_row_count)
