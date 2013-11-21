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

"""Unit tests for csv_transformer_mapper_tmpl.py."""

import cStringIO
import json
import unittest

from src.hadoop import csv_transformer_mapper_tmpl


class TransformTest(unittest.TestCase):
  """Unit test class for mapper Transform() method."""

  def testNoTransform(self):
    """Tests no-op transformation."""
    config = {
        'sinks': [
            '/bucket/output_file',
            '/bucket/output_file_badrows'
        ],
        'fieldDelimiter': ',',
        'sources': [
            'input-file'
        ],
        'skipLeadingRows': 1,
        'type': 'Data Pipeline',
        'columns': [
            {
                'wanted': True,
                'type': 'STRING',
                'name': 'Name',
                'transformations': []
            },
            {
                'wanted': True,
                'type': 'INTEGER',
                'name': 'Number',
                'transformations': []
            }
        ]
    }

    input_file = cStringIO.StringIO('abc,1\ndef,2\n')
    output_file = cStringIO.StringIO()

    csv_transformer_mapper_tmpl.Transform(
        json.dumps(config), input_file, output_file)

    self.assertEqual('abc,1\ndef,2\n', output_file.getvalue())

  def testColumnFilter(self):
    """Tests column filtering.

    Note the second column is not wanted in the config.
    """
    config = {
        'sinks': [
            '/bucket/output_file',
            '/bucket/output_file_badrows'
        ],
        'fieldDelimiter': ',',
        'sources': [
            'input-file'
        ],
        'skipLeadingRows': 1,
        'type': 'Data Pipeline',
        'columns': [
            {
                'wanted': True,
                'type': 'STRING',
                'name': 'Name',
                'transformations': []
            },
            {
                'wanted': False,
                'type': 'STRING',
                'name': 'CapitalName',
                'transformations': []
            },
            {
                'wanted': True,
                'type': 'INTEGER',
                'name': 'Number',
                'transformations': []
            }
        ]
    }

    input_file = cStringIO.StringIO('abc,ABC,1\ndef,DEF,2\n')
    output_file = cStringIO.StringIO()

    csv_transformer_mapper_tmpl.Transform(
        json.dumps(config), input_file, output_file)

    self.assertEqual('abc,1\ndef,2\n', output_file.getvalue())

  def testTransform(self):
    """Tests transformation."""
    config = {
        'sinks': [
            '/bucket/output_file',
            '/bucket/output_file_badrows'
        ],
        'fieldDelimiter': ',',
        'sources': [
            'input-file'
        ],
        'skipLeadingRows': 1,
        'type': 'Data Pipeline',
        'columns': [
            {
                'wanted': True,
                'type': 'STRING',
                'name': 'Name',
                'transformations': [
                    {
                        'match': 'abc',
                        'replace': 'XYZ'
                    }
                ]
            },
            {
                'wanted': True,
                'type': 'INTEGER',
                'name': 'Number',
                'transformations': []
            }
        ]
    }

    input_file = cStringIO.StringIO('abc,1\ndef,2\n')
    output_file = cStringIO.StringIO()

    csv_transformer_mapper_tmpl.Transform(
        json.dumps(config), input_file, output_file)

    self.assertEqual('XYZ,1\ndef,2\n', output_file.getvalue())


if __name__ == '__main__':
  unittest.main()
