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

"""S3 utility unit tests."""


import boto
import mock

import logging
from src import basetest
from src.clients import s3


class S3Test(basetest.TestCase):

  def testList(self):
    objs = ['foo', 'foozle', 'dir/a/b', 'dir/a/b/c', 'dir/a/x']

    mock_service = mock.MagicMock()
    mock_list_resp = [{'name': o} for o in objs]
    config = {'get_bucket.return_value.list.return_value': mock_list_resp}
    mock_service.configure_mock(**config)

    with mock.patch.object(boto, 'connect_s3') as mock_connect_s3:
      storage = s3.S3()
      storage.service = mock_service
      mock_connect_s3.assert_called_once_with(None, None)
      res = storage.ListBucket('bucket')
      for o in objs:
        self.assertIn(o, res)

  def testReadSimpleSmall(self):
    s3_url = 's3://bucket/foo.txt'
    s3_content_type = 'text/plain'
    payload = 'foofoofoofoobar'

    def _MockedGetContentsToFile(buf,
                                 headers=None,
                                 cb=None,
                                 num_cb=10):
      buf.write(payload)
      self.assertIsNone(headers)
      self.assertIsNone(cb)
      self.assertEquals(num_cb, 0)

    mock_boto_key = mock.MagicMock()
    mock_boto_key.size.return_value = len(payload)
    mock_boto_key.content_type = s3_content_type
    mock_boto_key.get_contents_to_file = _MockedGetContentsToFile
    mock_service = mock.MagicMock()
    config = {'get_bucket.return_value.get_key.return_value': mock_boto_key}
    mock_service.configure_mock(**config)

    with mock.patch.object(boto, 'connect_s3'):
      storage = s3.S3()
      storage.service = mock_service
      buf = storage.ReadObject(url=s3_url)
      self.assertEquals(payload, buf.read(1024))
      buf.close()

  def testReadCallbackSmall(self):
    s3_url = 's3://bucket/foo.txt'
    s3_content_type = 'text/plain'
    payload = 'foofoofoofoobar'

    def _MockedGetContentsToFile(buf,
                                 headers=None,
                                 cb=None,
                                 num_cb=10):
      buf.write(payload)
      self.assertIsNone(headers)
      self.assertIsNotNone(cb)
      self.assertEquals(num_cb, 1)

    def _Callback(buf, bytes_read, unused_bytes_left, content_type):
      self.assertEquals(payload, buf.read(1024))
      self.assertEquals(bytes_read, len(payload))
      self.assertEquals(content_type, s3_content_type)

    mock_boto_key = mock.MagicMock()
    mock_boto_key.size = len(payload)
    mock_boto_key.content_type = s3_content_type
    mock_boto_key.get_contents_to_file = _MockedGetContentsToFile
    mock_service = mock.MagicMock()
    config = {'get_bucket.return_value.get_key.return_value': mock_boto_key}
    mock_service.configure_mock(**config)

    with mock.patch.object(boto, 'connect_s3'):
      storage = s3.S3()
      storage.service = mock_service
      buf = storage.ReadObject(url=s3_url, handler=_Callback)
      self.assertIsNone(buf)


if __name__ == '__main__':
  basetest.main()
