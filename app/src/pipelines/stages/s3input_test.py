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

"""S3 pipeline stages unit tests."""



import cStringIO as StringIO

import mock

import logging
from src import basetest
from src.clients import gcs
from src.clients import s3  # pylint: disable=unused-import
from src.pipelines.stages import s3input


class S3InputTest(basetest.TestCase):

  def testSimple(self):
    config = {
        'object': 's3://foo/bar.txt',
        's3Credentials': {
            'accessKey': 'key',
            'accessSecret': 'secret'
        },
        'sinks': ['gs://bucket/obj']
    }

    def _MockedS3StatObject(url, bucket=None, obj=None):
      self.assertIsNotNone(url)
      self.assertIsNone(bucket)
      self.assertIsNone(obj)
      return {'size': 10}

    mock_s3 = mock.MagicMock()
    mock_s3_ro = mock.MagicMock()
    mock_s3.ReadObject = mock_s3_ro
    mock_s3.StatObject = _MockedS3StatObject
    with mock.patch(
        'src.clients.s3.S3',
        autospec=True,
        return_value=mock_s3):
      stage = s3input.S3Input(config)
      stage.start_test()
      mock_s3_ro.assert_called_once_with(url=config['object'],
                                         handler=mock.ANY,
                                         start=0,
                                         length=10)

  def testOneObjectNoChunksNoRange(self):
    payload = 'foofoofoofoobar'
    content_type = 'text/plain'
    config = {
        'object': 's3://foo/bar.txt',
        's3Credentials': {
            'accessKey': 'key',
            'accessSecret': 'secret'
        },
        'sinks': ['gs://bucket/obj']
    }

    def _MockedS3ReadObject(url, handler, start=None, length=None):
      self.assertIsNotNone(url)
      self.assertIsNotNone(handler)
      self.assertIsNotNone(start)
      self.assertIsNotNone(length)

      buf = StringIO.StringIO(payload)
      handler(buf, len(payload), 0, content_type)

    def _MockedS3StatObject(url, bucket=None, obj=None):
      self.assertIsNotNone(url)
      self.assertIsNone(bucket)
      self.assertIsNone(obj)
      return {'size': len(payload)}

    mock_s3 = mock.MagicMock()
    mock_s3.ReadObject = _MockedS3ReadObject
    mock_s3.StatObject = _MockedS3StatObject
    with mock.patch(
        'src.clients.s3.S3',
        autospec=True,
        return_value=mock_s3):
      with mock.patch.object(gcs.Gcs,
                             'InsertObject',
                             return_value=mock.MagicMock()) as mock_gcs_insert:
        stage = s3input.S3Input(config)
        stage.start_test()
        mock_gcs_insert.assert_called_once_with(mock.ANY,
                                                config['sinks'][0])

  # TODO(user): flesh out more comprehensive tests


if __name__ == '__main__':
  basetest.main()
