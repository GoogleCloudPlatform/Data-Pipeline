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

"""Pipeline config linter unit tests."""

import json
import os
import re



import logging

from src import basetest
from src.pipelines import linter


class MockPipeline(object):

  def __init__(self, name, config):
    self.name = name
    self.config = config


class LintPipelineTest(basetest.TestCase):
  """Tests the pipeline linter."""

  def getUnterminatedStringMessage(self, input_string):
    # Sadly json returns different indexes from different versions of python.
    # linux Python 2.7.3 json 2.0.9:
    # Unterminated string starting at: 'line 1 column 0 (char 0)'
    # Mac Python 2.7.5 json 2.0.9
    # Unterminated string starting at: 'line 1 column 1 (char 0)'
    # So we work out the right answer here.
    try:
      json.loads(input_string)
    except ValueError as e:
      if re.match('Unterminated string starting at: '
                  r'line \d+ column \d+ \(char \d+\)', e.message):
        return e.message
      else:
        raise ValueError('expected %r to raise a ValueError that looked like '
                         '"Unterminated string" but got %r' %
                         (input_string, e.message))
    raise ValueError('expected %r to raise a ValueError' % input_string)

  def testJunk(self):
    bad_strings = ((None, 'PreTemplate: expected string or buffer'),
                   ('', 'PreTemplate: No JSON object could be decoded'),
                   ('"', 'PreTemplate: end is out of bounds'),
                   ('"fish', 'PreTemplate: %s' %
                    self.getUnterminatedStringMessage('"fish')),
                   ('fish', 'PreTemplate: No JSON object could be decoded'))
    for (bad_string, reason) in bad_strings:
      logging.info('testing: %r', bad_string)
      pl = linter.PipelineLinter(bad_string)
      expect = {
          linter.PipelineLinter.CHECK_UNKNOWN_CONFIG_KEYS: {'pass': True},
          linter.PipelineLinter.CHECK_SYNTAX_VALID: {
              'pass': False,
              'reason': reason},
          linter.PipelineLinter.CHECK_REQ_IO_STAGES: {
              'pass': False,
              'reason': linter.PipelineLinter.MSG_MISSING_IO_STAGES}}
      self.assertFalse(pl.results.valid, 'Config:%r ' % bad_string)
      self.assertSameStructure(expect, pl.results.results,
                               'Config:%r' % bad_string)

  def testSimpleFailure(self):
    pl = linter.PipelineLinter('{"inputs": [{"type": "GcsInput"}]}')
    expect = {
        linter.PipelineLinter.CHECK_SYNTAX_VALID: {'pass': True},
        linter.PipelineLinter.CHECK_REQ_IO_STAGES: {'pass': True},
        linter.PipelineLinter.CHECK_UNKNOWN_CONFIG_KEYS: {'pass': True},
        'stages': {
            'inputs': [{
                linter.StageLinter.CHECK_TYPE_FMT % u'GcsInput': {'pass': True},
                linter.StageLinter.CHECK_FIELD_EXISTS_FMT %
                ['object', 'objects']:
                {'pass': False,
                 'reason':
                 linter.StageLinter.MSG_REQUIRE_AT_LEAST_ONE_FMT %
                 ['object', 'objects']
                },
            }]
        }
    }
    self.assertFalse(pl.results.valid)
    self.assertSameStructure(expect, pl.results.results)

  def testMissingStageType(self):
    pl = linter.PipelineLinter('{"inputs": [{}]}')
    expect = {
        linter.PipelineLinter.CHECK_SYNTAX_VALID: {'pass': True},
        linter.PipelineLinter.CHECK_REQ_IO_STAGES: {'pass': True},
        linter.PipelineLinter.CHECK_UNKNOWN_CONFIG_KEYS: {'pass': True},
        'stages': {
            'inputs': [{
                linter.StageLinter.CHECK_TYPE_FMT % None: {
                    'pass': False,
                    'reason': linter.StageLinter.MSG_TYPE_NOT_FOUND}}]}
    }
    self.assertFalse(pl.results.valid)
    self.assertSameStructure(expect, pl.results.results)

  def testMissingInputStageType(self):
    pl = linter.PipelineLinter('{}')
    expect = {
        linter.PipelineLinter.CHECK_SYNTAX_VALID: {'pass': True},
        linter.PipelineLinter.CHECK_UNKNOWN_CONFIG_KEYS: {'pass': True},
        linter.PipelineLinter.CHECK_REQ_IO_STAGES: {
            'pass': False,
            'reason': linter.PipelineLinter.MSG_MISSING_IO_STAGES},
    }
    self.assertFalse(pl.results.valid)
    self.assertSameStructure(expect, pl.results.results)

  def testBadStageType(self):
    pl = linter.PipelineLinter('{"inputs": [{"type": "UnknownInput"}]}')
    expect = {
        linter.PipelineLinter.CHECK_SYNTAX_VALID: {'pass': True},
        linter.PipelineLinter.CHECK_UNKNOWN_CONFIG_KEYS: {'pass': True},
        linter.PipelineLinter.CHECK_REQ_IO_STAGES: {'pass': True},
        'stages': {
            'inputs': [{
                linter.StageLinter.CHECK_TYPE_FMT % u'UnknownInput': {
                    'pass': False,
                    'reason': 'No module named unknowninput'}}],
        }
    }
    self.assertFalse(pl.results.valid)
    self.assertSameStructure(expect, pl.results.results)

  def testBadSection(self):
    pl = linter.PipelineLinter('{"input": [{"type": "UnknownInput"}]}')
    expect = {
        linter.PipelineLinter.CHECK_SYNTAX_VALID: {'pass': True},
        linter.PipelineLinter.CHECK_UNKNOWN_CONFIG_KEYS: {
            'pass': False,
            'reason':
            linter.PipelineLinter.MSG_UNKNOWN_CONFIG_KEYS_FMT % u'input'},
        linter.PipelineLinter.CHECK_REQ_IO_STAGES: {
            'pass': False,
            'reason': linter.PipelineLinter.MSG_MISSING_IO_STAGES}}
    self.assertFalse(pl.results.valid)
    self.assertSameStructure(expect, pl.results.results)

  def testUpdateNestedDict(self):
    o = {'h': {'b': 1}}
    linter.UpdateNestedDict(o, {'h': {'c': 2}})
    self.assertSameStructure({'h': {'b': 1, 'c': 2}}, o)

    o = {'h': {'b': 1}}
    linter.UpdateNestedDict(o, {'z': {'c': 2}})
    self.assertSameStructure({'h': {'b': 1}, 'z': {'c': 2}}, o)

    o = {'h': 'j'}  # It should not overwrite the h key.
    linter.UpdateNestedDict(o, {'h': 'k'})
    self.assertSameStructure({'h': 'j'}, o)

  def testGcsInputOk(self):
    pl = linter.PipelineLinter(
        '{"inputs": [{"type": "GcsInput", "object": "gs://b1/o1",'
        ' "sinks": ["gs:/b2/o2"]}]}')
    expect = {
        linter.PipelineLinter.CHECK_SYNTAX_VALID: {'pass': True},
        linter.PipelineLinter.CHECK_UNKNOWN_CONFIG_KEYS: {'pass': True},
        linter.PipelineLinter.CHECK_REQ_IO_STAGES: {'pass': True},
        'stages': {'inputs': [{
            linter.StageLinter.CHECK_TYPE_FMT % u'GcsInput': {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'sinks':
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'object':
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % ['object', 'objects']:
            {'pass': True},
        }]}}
    self.assertTrue(pl.results.valid)
    self.assertSameStructure(expect, pl.results.results)

  def testGcsInputNullSink(self):
    pl = linter.PipelineLinter(
        '{"inputs": [{"type": "GcsInput", "object": "gs://b1/o1",'
        ' "sinks": [null]}]}')
    expect = {
        linter.PipelineLinter.CHECK_SYNTAX_VALID: {'pass': True},
        linter.PipelineLinter.CHECK_UNKNOWN_CONFIG_KEYS: {'pass': True},
        linter.PipelineLinter.CHECK_REQ_IO_STAGES: {'pass': True},
        'stages': {'inputs': [{
            linter.StageLinter.CHECK_TYPE_FMT % u'GcsInput': {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'sinks':
            {'pass': False,
             'reason': linter.StageLinter.MSG_FIELD_INVALID_FMT % 'null'},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'object':
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % ['object', 'objects']:
            {'pass': True},
        }]}}
    self.assertFalse(pl.results.valid)
    self.assertSameStructure(expect, pl.results.results)

  def testGcsOutputOk(self):
    pl = linter.PipelineLinter(
        '{"outputs": [{"type": "GcsOutput", "object": "gs://b1/o1",'
        ' "sources": ["gs:/b2/o2"]}]}')
    expect = {
        linter.PipelineLinter.CHECK_SYNTAX_VALID: {'pass': True},
        linter.PipelineLinter.CHECK_UNKNOWN_CONFIG_KEYS: {'pass': True},
        linter.PipelineLinter.CHECK_REQ_IO_STAGES: {'pass': True},
        'stages': {'outputs': [{
            linter.StageLinter.CHECK_TYPE_FMT % u'GcsOutput': {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'sources':
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'object':
            {'pass': True},
        }]}}
    self.assertTrue(pl.results.valid)
    self.assertSameStructure(expect, pl.results.results)

  def testGcsCompositorOk(self):
    pl = linter.PipelineLinter(
        '{"inputs": [{"type": "GcsInput", "object": "gs://b1/o1",'
        ' "sinks": ["gs:/b2/o2"]}],'
        '"transforms": [{"type": "GcsCompositor", "sources": ["gs:/b2/o2"],'
        ' "sinks": ["gs:/b3/o3"], "contentType": "text/plain"}],'
        '"outputs": [{"type": "GcsOutput", "object": "gs://b3/o3",'
        ' "sources": ["gs:/b4/o4"]}]}')
    expect = {
        linter.PipelineLinter.CHECK_SYNTAX_VALID: {'pass': True},
        linter.PipelineLinter.CHECK_UNKNOWN_CONFIG_KEYS: {'pass': True},
        linter.PipelineLinter.CHECK_REQ_IO_STAGES: {'pass': True},
        'stages': {'inputs': [{
            linter.StageLinter.CHECK_TYPE_FMT % u'GcsInput': {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'sinks':
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'object':
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % ['object', 'objects']:
            {'pass': True}}], 'transforms': [{
                linter.StageLinter.CHECK_TYPE_FMT % u'GcsCompositor':
                {'pass': True},
                linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'sources':
                {'pass': True},
                linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'sinks':
                {'pass': True},
                linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'contentType':
                {'pass': True}}], 'outputs': [{
                    linter.StageLinter.CHECK_TYPE_FMT % u'GcsOutput':
                    {'pass': True},
                    linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'sources':
                    {'pass': True},
                    linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'object':
                    {'pass': True}}]
                  }}
    self.assertTrue(pl.results.valid)
    self.assertSameStructure(expect, pl.results.results)

  def testHttpInputOk(self):
    pl = linter.PipelineLinter(
        '{"inputs": [{"type": "HttpInput", "url": "http://foo/data.csv",'
        ' "sinks": ["gs:/b2/o2"]}]}')
    expect = {
        linter.PipelineLinter.CHECK_SYNTAX_VALID: {'pass': True},
        linter.PipelineLinter.CHECK_UNKNOWN_CONFIG_KEYS: {'pass': True},
        linter.PipelineLinter.CHECK_REQ_IO_STAGES: {'pass': True},
        'stages': {'inputs': [{
            linter.StageLinter.CHECK_TYPE_FMT % u'HttpInput': {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'url': {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'sinks':
            {'pass': True},
        }]}}
    self.assertTrue(pl.results.valid)
    self.assertSameStructure(expect, pl.results.results)

  def testHttpInputBadShardSize(self):
    pl = linter.PipelineLinter(
        '{"inputs": [{"type": "HttpInput", "url": "http://foo/data.csv",'
        ' "shardSize": 33554433, "sinks": ["gs:/b2/o2"]}]}')
    expect = {
        linter.PipelineLinter.CHECK_SYNTAX_VALID: {'pass': True},
        linter.PipelineLinter.CHECK_UNKNOWN_CONFIG_KEYS: {'pass': True},
        linter.PipelineLinter.CHECK_REQ_IO_STAGES: {'pass': True},
        'stages': {'inputs': [{
            linter.StageLinter.CHECK_TYPE_FMT % u'HttpInput': {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'url': {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'sinks':
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'shardSize':
            {'pass': False,
             'reason': 'Invalid value: \''
                       'Size exceeds App Engine response limit.\''},
        }]}}
    self.assertFalse(pl.results.valid)
    self.assertSameStructure(expect, pl.results.results)

  def testDatastoreInputOk(self):
    pl = linter.PipelineLinter(
        '{"inputs": [{"type": "DatastoreInput", "gql": "SELECT *",'
        ' "params": {"projection": ["a", "b"]},'
        ' "sinks": ["gs:/b2/o2"]}]}')
    expect = {
        linter.PipelineLinter.CHECK_SYNTAX_VALID: {'pass': True},
        linter.PipelineLinter.CHECK_UNKNOWN_CONFIG_KEYS: {'pass': True},
        linter.PipelineLinter.CHECK_REQ_IO_STAGES: {'pass': True},
        'stages': {'inputs': [{
            linter.StageLinter.CHECK_TYPE_FMT % u'DatastoreInput':
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'sinks':
            {'pass': True},
            linter.StageLinter.CHECK_TYPE_FMT % 'params': {'pass': True},
            linter.StageLinter.CHECK_TYPE_FMT % 'params.projection':
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % ['gql', 'object']:
            {'pass': True},
        }]}}
    self.assertTrue(pl.results.valid)
    self.assertSameStructure(expect, pl.results.results)

  def testS3InputOk(self):
    pl = linter.PipelineLinter(
        '{"inputs": [{"type": "S3Input", "object": "s3://b/o",'
        ' "s3Credentials": {"accessKey": "123", "accessSecret": "abc"},'
        ' "sinks": ["gs:/b2/o2"]}]}')
    expect = {
        linter.PipelineLinter.CHECK_SYNTAX_VALID: {'pass': True},
        linter.PipelineLinter.CHECK_UNKNOWN_CONFIG_KEYS: {'pass': True},
        linter.PipelineLinter.CHECK_REQ_IO_STAGES: {'pass': True},
        'stages': {'inputs': [{
            linter.StageLinter.CHECK_TYPE_FMT % u'S3Input': {'pass': True},
            linter.StageLinter.CHECK_TYPE_FMT % 's3Credentials': {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'object':
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'sinks':
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 's3Credentials':
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % ['object', 'objects']:
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT %
            's3Credentials.accessKey': {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT %
            's3Credentials.accessSecret': {'pass': True},
        }]}}
    self.assertTrue(pl.results.valid)
    self.assertSameStructure(expect, pl.results.results)

  def testBigQueryOutputOk(self):
    pl = linter.PipelineLinter(
        '{"outputs": [{"type": "BigQueryOutput",'
        ' "destinationTable": {"projectId": "123", "tableId": "abc",'
        ' "datasetId": "xyz"}, "schema": {"fields": [{"type": "STRING"}]},'
        ' "sources": ["gs:/b2/o2"]}]}')
    expect = {
        linter.PipelineLinter.CHECK_SYNTAX_VALID: {'pass': True},
        linter.PipelineLinter.CHECK_UNKNOWN_CONFIG_KEYS: {'pass': True},
        linter.PipelineLinter.CHECK_REQ_IO_STAGES: {'pass': True},
        'stages': {'outputs': [{
            linter.StageLinter.CHECK_TYPE_FMT % u'BigQueryOutput':
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'sources':
            {'pass': True},
            linter.StageLinter.CHECK_TYPE_FMT % 'destinationTable':
            {'pass': True},
            linter.StageLinter.CHECK_TYPE_FMT % 'schema': {'pass': True},
            linter.StageLinter.CHECK_TYPE_FMT % 'schema.fields': {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'destinationTable':
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'schema':
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT %
            'destinationTable.projectId': {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT %
            'destinationTable.tableId': {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT %
            'destinationTable.datasetId': {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT %
            'schema.fields': {'pass': True},
        }]}}
    self.assertTrue(pl.results.valid)
    self.assertSameStructure(expect, pl.results.results)

  def testCsvMatchReplaceOk(self):
    pl = linter.PipelineLinter(
        '{"inputs": [{"type": "GcsInput", "object": "gs://b1/o1",'
        ' "sinks": ["gs:/b2/o2"]}],'
        ' "transforms": [{"type": "CsvMatchReplace",'
        ' "fieldDelimiter": ",", "columns": [{"wanted": true, '
        ' "type": "STRING", "name": "col1"}],'
        ' "sources": ["gs://bucket/foo.csv"],'
        ' "sinks": ["gs://bucket/results", "gs://bucket/badrows"]}]}')
    expect = {
        linter.PipelineLinter.CHECK_SYNTAX_VALID: {'pass': True},
        linter.PipelineLinter.CHECK_UNKNOWN_CONFIG_KEYS: {'pass': True},
        linter.PipelineLinter.CHECK_REQ_IO_STAGES: {'pass': True},
        'stages': {'transforms': [{
            linter.StageLinter.CHECK_TYPE_FMT % u'CsvMatchReplace':
            {'pass': True},
            linter.StageLinter.CHECK_TYPE_FMT % 'columns': {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'sources':
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'sinks':
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'fieldDelimiter':
            {'pass': True},
            linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'columns':
            {'pass': True},}],
                   'inputs': [{
                       linter.StageLinter.CHECK_TYPE_FMT % u'GcsInput':
                       {'pass': True},
                       linter.StageLinter.CHECK_FIELD_EXISTS_FMT % 'sinks':
                       {'pass': True},
                       linter.StageLinter.CHECK_FIELD_EXISTS_FMT %
                       'object': {'pass': True},
                       linter.StageLinter.CHECK_FIELD_EXISTS_FMT %
                       ['object', 'objects']: {'pass': True}}]}}
    self.assertTrue(pl.results.valid)
    self.assertSameStructure(expect, pl.results.results)

  def testLintFullFiles(self):
    directories = ['src/pipelines/testdata', 'static/examples']

    for directory in directories:
      directory = os.path.join(os.path.dirname(__file__),
                               '../..', directory)
      filenames = [x for x in os.listdir(directory) if x.endswith('.json')]
      logging.info('directory %s files %r', directory, filenames)

      for filename in filenames:
        logging.info('Linting %r from %r',
                     filename, os.path.basename(directory))
        j = open(os.path.join(directory, filename)).read()
        pl = linter.PipelineLinter(j)
        self.assertTrue(pl.results.valid)


if __name__ == '__main__':
  basetest.main()
