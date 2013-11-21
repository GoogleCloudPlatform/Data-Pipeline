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

"""HTTP pipeline stages unit tests."""



import urllib2

from mapreduce.lib.pipeline import common
import mock

import logging
from src import basetest
from src.clients import gcs
from src.pipelines.stages import gcscompositor
from src.pipelines.stages import httpinput


class HttpInputTest(basetest.TestCase):
  _mocked_urls_created = 0

  def testInputSingleRequestWithoutCR(self):
    class MockMeta(object):

      def __init__(self, payload):
        self.payload = payload

      def getheaders(self, h):
        if h == 'Content-Type':
          return ['text/plain']
        elif h == 'Content-Length':
          return [len(self.payload)]
        elif h == 'Content-Range':
          return None
        else:
          raise NotImplementedError

    self._InputSingleRequest(MockMeta('foobar'))

  def testInputSingleRequestWithCR(self):
    class MockMeta(object):

      def __init__(self, payload):
        self.payload = payload

      def getheaders(self, h):
        l = len(self.payload)
        if h == 'Content-Type':
          return ['text/plain']
        elif h == 'Content-Length':
          return [str(l)]
        elif h == 'Content-Range':
          return ['bytes 0-%d/%d' % (l, l)]
        else:
          raise NotImplementedError

    self._InputSingleRequest(MockMeta('foobar'))

  def _InputSingleRequest(self, meta_mock):
    config = {
        'url': 'http://foo/bar.txt',
        'sinks': ['gs://bucket/obj']
    }
    mock_resp = mock.MagicMock()
    mock_resp.info.return_value = meta_mock
    mock_resp.read.return_value = meta_mock.payload
    with mock.patch.object(urllib2,
                           'urlopen',
                           return_value=mock_resp):
      stage = httpinput.HttpInput(config)
      with mock.patch.object(gcs.Gcs,
                             'InsertObject',
                             mock.MagicMock()) as mock_gcs_insert:
        stage.start_test()
        mock_gcs_insert.assert_called_once_with(mock.ANY,
                                                url='gs://bucket/obj')

  def testInputMultipleRequests(self):
    class MockMeta(object):

      def __init__(self, payload):
        self.payload = payload
        self.length = len(self.payload)
        self.half_length = self.length / 2
        self.calls = 0

      def getheaders(self, h):
        self.calls += 1
        if h == 'Content-Type':
          return ['text/plain']
        elif h == 'Content-Length':
          if self.calls == 1:
            return 0
          elif self.calls <= 3:
            return [str(self.half_length)]
          else:
            return None
        elif h == 'Content-Range':
          if self.calls == 1:
            return ['bytes 0-0/%d' % self.length]
          elif self.calls == 2:
            return ['bytes 0-%d/%d' % (self.half_length - 1, self.length)]
          elif self.calls == 3:
            return ['bytes %d-%d/%d' % (
                self.half_length, self.length - 1, self.length)]
          else:
            return None
        else:
          raise NotImplementedError

    config = {
        'url': 'http://foo/bar.txt',
        'shardPrefix': 'chunks/',
        'sinks': ['gs://bucket/obj']
    }

    mock_resp = mock.MagicMock()
    meta_mock = MockMeta('foobar')
    mock_resp.info.return_value = meta_mock
    read_return_val = [meta_mock.payload[:meta_mock.half_length],
                       meta_mock.payload[meta_mock.half_length:],
                       '']
    def _MockReadSideEffect():
      return read_return_val.pop()
    mock_resp.read.side_effect = _MockReadSideEffect
    with mock.patch.object(urllib2, 'urlopen',
                           return_value=mock_resp):
      stage = httpinput.HttpInput(config)
      stage.REQUEST_CHUNK_SIZE = meta_mock.half_length
      with mock.patch.object(gcs.Gcs,
                             'InsertObject',
                             return_value=mock.MagicMock()) as mock_gcs_insert:
        with mock.patch.object(gcs.Gcs,
                               'UrlCreator',
                               return_value=mock.MagicMock()) as mock_creator:

          def _MockUrlCreatorSideEffect(unused_bucket, prefix):
            def _MockUrlCreator():
              self._mocked_urls_created += 1
              return 'gs://created_bucket/%screated_obj' % prefix
            return _MockUrlCreator
          mock_creator.side_effect = _MockUrlCreatorSideEffect

          with mock.patch.object(gcscompositor,
                                 'GcsCompositor',
                                 common.Ignore):
            stage.start_test()
            first_create = mock.call('bucket', 'chunks/')
            second_create = mock.call('bucket', 'chunks/')
            mock_creator.has_calls([first_create, second_create])
            first_insert = mock.call(
                mock.ANY,
                url='gs://created_bucket/chunks/created_obj')
            second_insert = mock.call(
                mock.ANY,
                httpinput.HttpInput.DEFAULT_CONTENT_TYPE,
                url='gs://created_bucket/chunks/created_obj')
            mock_gcs_insert.has_calls([first_insert, second_insert])
            self.assertEquals(2, mock_gcs_insert.call_count)
            self.assertEquals(2, self._mocked_urls_created)

  def testSubRange(self):
    class MockMeta(object):

      def __init__(self, payload):
        self.payload = payload
        self.length = len(self.payload)
        self.half_length = self.length / 2
        self.calls = 0

      def getheaders(self, h):
        self.calls += 1
        if h == 'Content-Type':
          return ['text/plain']
        elif h == 'Content-Length':
          if self.calls == 1:
            return 0
          elif self.calls <= 3:
            return [str(self.half_length)]
          else:
            return None
        elif h == 'Content-Range':
          if self.calls == 1:
            return ['bytes 0-0/%d' % self.length]
          elif self.calls == 2:
            return ['bytes 0-%d/%d' % (self.half_length - 1, self.length)]
          elif self.calls == 3:
            return ['bytes %d-%d/%d' % (
                self.half_length, self.length - 1, self.length)]
          else:
            return None
        else:
          raise NotImplementedError

    mock_req = mock.MagicMock()
    mock_resp = mock.MagicMock()
    meta_mock = MockMeta('foobar')
    mock_resp.info.return_value = meta_mock
    mock_resp.read.return_value = meta_mock.payload

    config = {
        'url': 'http://foo/bar.txt',
        'start': 2,
        'length': 5,
        'sinks': ['gs://bucket/bar.txt'],
    }

    with mock.patch('urllib2.Request', autospec=True, return_value=mock_req):
      with mock.patch.object(urllib2, 'urlopen', return_value=mock_resp):
        with mock.patch.object(gcs.Gcs,
                               'InsertObject',
                               mock.MagicMock()):
          with mock.patch.object(gcscompositor,
                                 'GcsCompositor',
                                 common.Ignore):
            stage = httpinput.HttpInput(config)
            stage.start_test()
            first_chunk = mock.call('Range', 'bytes=2-6')
            mock_req.add_header.assert_has_calls([first_chunk])


if __name__ == '__main__':
  basetest.main()
