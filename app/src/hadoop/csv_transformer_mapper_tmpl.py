#!/usr/bin/python
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


"""Hadoop MapReduce mapper to perform CSV transformation for Datapipeline."""

import csv
import json
import re
import sys


# Configuration is replaced to actual configuration by AppEngine.
# Unit tests can directly pass configuration strings to Transform().
TRANSFORM_CONFIG_JSON_STRING = '{{ transform_config }}'


def Transform(config_json, input_file, output_file):
  """Performs transformation on CSV lines.

  Args:
    config_json: Transform configuration in JSON string format.
    input_file: File-like object to receive CSV input.
    output_file: File-like object to output transform result.
  """
  transform_config = json.loads(config_json)
  columns = transform_config['columns']

  # Prepare regular expression patterns.
  for column in columns:
    if column['transformations']:
      for transform in column['transformations']:
        transform['re'] = re.compile(transform['match'])

  csv_writer = csv.writer(output_file, lineterminator='\n')
  for row in csv.reader(input_file,
                        delimiter=str(transform_config['fieldDelimiter'])):
    if len(row) != len(columns):
      # Mismatch in number of columns
      continue

    # TODO(user): Install transformation module onto GCE instance at start up
    #     and use the shared files.
    #     datapipeline/app/src/csvmatchreplace/transform.py.
    new_row = []
    for i in xrange(len(columns)):
      column = columns[i]
      if not column['wanted']:
        continue
      new_value = row[i]
      # Perform transformation.
      if column['transformations']:
        for transform in column['transformations']:
          new_value = transform['re'].sub(transform['replace'], new_value)
      new_row.append(new_value)

    csv_writer.writerow(new_row)


if __name__ == '__main__':
  Transform(TRANSFORM_CONFIG_JSON_STRING, sys.stdin, sys.stdout)
