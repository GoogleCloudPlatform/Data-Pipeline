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

"""GCS utility unit tests."""

import mock

import cloudstorage  # pylint: disable=unused-import

import logging
from src import basetest
from src.clients import gcs


class GCSTest(basetest.TestCase):

  def testURLFuncs(self):
    bad_proto_urls = ['/bad', 'bad', '//bad', '', '/', '//', ' ', 'bad/bad',
                      '//bad/bad', 'foo://bad', 'foo://bad/bad', '://', ':/',
                      '://foo', '://foo/foo',
                     ]
    bad_content_urls = ['gs://', 'gs://bad', 'gs://bad/']

    good_urls = [('gs://bucket/obj', 'bucket', 'obj', '/bucket/obj'),
                 ('gs://bucket/a/obj', 'bucket', 'a/obj', '/bucket/a/obj'),
                 ('gs://bucket/a/b/c/de',
                  'bucket', 'a/b/c/de', '/bucket/a/b/c/de')]

    for u in bad_proto_urls:
      self.assertRaises(ValueError, gcs.Gcs.UrlToBucketAndName, u)
    for u in bad_content_urls:
      (bucket, obj) = gcs.Gcs.UrlToBucketAndName(u)
      self.assertIs((bucket and obj), '')

    for (url, expected_bucket, expected_object, expected_path) in good_urls:
      (bucket, obj) = gcs.Gcs.UrlToBucketAndName(url)
      self.assertEquals(expected_bucket, bucket)
      self.assertEquals(expected_object, obj)
      self.assertEquals(expected_path, gcs.Gcs.UrlToBucketAndNamePath(url))

    (bucket, obj) = gcs.Gcs.UrlToBucketAndName(gcs.Gcs.UrlCreator('')())
    self.assertIs((bucket and obj), '')
    self.assertIsNot((bucket or obj), gcs.Gcs.UrlCreator('bucket'))

  def testInsert(self):
    mock_service = mock.MagicMock()
    mock_buckets = mock.MagicMock()
    mock_service.buckets.return_value = mock_buckets

    storage = gcs.Gcs()
    storage._service = mock_service
    storage.InsertBucket('project', 'bucket')

    call = mock.call.insert(
        project='project',
        body={'name': 'bucket', 'location': 'US'})
    mock_buckets.assert_has_calls(call)

  def testList(self):
    objs = ['/bucket/booo', '/bucket/foozle', '/bucket/dir/a/b']
    objs_in_gs = ['gs:/' + o for o in objs]
    storage = gcs.Gcs()
    mock_list_resp = mock.MagicMock()
    mock_list_resp.return_value = [mock.MagicMock(filename=o) for o in objs]
    with mock.patch.object(cloudstorage, 'listbucket',
                           mock_list_resp):
      res = storage.ListBucket('bucket')
      for o in objs_in_gs:
        self.assertIn(o, res)

  def testStat(self):
    class MockStat(object):
      def __init__(self):
        self.st_size = 100
        self.etag = '35cb8ce70d66aac33163db67180fb6d3'
        self.content_type = 'text/plain'
        self.metadata = {}

    expect = {
        'size': 100,
        'md5Hash': '35cb8ce70d66aac33163db67180fb6d3',
        'contentType': 'text/plain',
        'metadata': {}
    }

    # test using gae cloudstorage api
    storage = gcs.Gcs()
    mock_stat_resp = mock.MagicMock()
    mock_stat_resp.return_value = MockStat()
    with mock.patch.object(cloudstorage, 'stat',
                           mock_stat_resp):
      stat = storage.StatObject(bucket='bucket', obj='obj')
      self.assertSameStructure(stat, expect)

  def testComposeNoRec(self):
    src = ['0', '1', '2', '3', '4', '5', '6', '7']
    mock_service = mock.MagicMock()
    mock_objects = mock.MagicMock()
    mock_service.objects.return_value = mock_objects

    storage = gcs.Gcs()
    storage._service = mock_service
    storage.ComposeObjects('bucket', src, 'dest', 'text/plain')

    call = mock.call.compose(
        destinationBucket='bucket',
        destinationObject='dest',
        body={'sourceObjects': [{'name': s} for s in src],
              'destination': {'contentType': 'text/plain'}})
    mock_objects.assert_has_calls(call)

  def testComposeOneRec(self):
    src = ['0', '1', '2', '3', '4', '5', '6', '7']
    mock_service = mock.MagicMock()
    mock_objects = mock.MagicMock()
    mock_service.objects.return_value = mock_objects

    storage = gcs.Gcs()
    with mock.patch.object(storage,
                           'UrlCreator',
                           return_value=lambda: 'gs://bucket/X',
                           autospec=True):
      storage.MAX_COMPOSABLE_OBJECTS = 3
      storage._service = mock_service
      storage.ComposeObjects('bucket', src, 'dest', 'text/plain')

      call_a = mock.call.compose(
          destinationBucket='bucket',
          destinationObject='X',
          body={'sourceObjects': [{'name': '0'}, {'name': '1'}, {'name': '2'}],
                'destination': {'contentType': 'text/plain'}})
      call_b = mock.call.compose(
          destinationBucket='bucket',
          destinationObject='X',
          body={'sourceObjects': [{'name': '3'}, {'name': '4'}, {'name': '5'}],
                'destination': {'contentType': 'text/plain'}})
      call_c = mock.call.compose(
          destinationBucket='bucket',
          destinationObject='X',
          body={'sourceObjects': [{'name': '6'}, {'name': '7'}],
                'destination': {'contentType': 'text/plain'}})
      call_d = mock.call.compose(
          destinationBucket='bucket',
          destinationObject='dest',
          body={'sourceObjects': [{'name': 'X'}, {'name': 'X'}, {'name': 'X'}],
                'destination': {'contentType': 'text/plain'}})

      calls = [call_a, call_b, call_c, call_d]
      mock_objects.assert_has_calls(calls, any_order=True)

  def testSplitEvenly(self):
    self.assertEquals([6, 5],
                      [len(x) for x in gcs.SplitEvenly(tuple(range(11)), 9)])
    self.assertEquals([6, 5],
                      [len(x) for x in gcs.SplitEvenly(tuple(range(11)), 6)])
    self.assertEquals([4, 4, 3],
                      [len(x) for x in gcs.SplitEvenly(tuple(range(11)), 5)])
    self.assertEquals([1],
                      [len(x) for x in gcs.SplitEvenly(tuple(range(1)), 5)])
    self.assertEquals([1],
                      [len(x) for x in gcs.SplitEvenly(tuple(range(1)), 99)])
    self.assertEquals([1, 1],
                      [len(x) for x in gcs.SplitEvenly(tuple(range(2)), 1)])
    self.assertEquals([2],
                      [len(x) for x in gcs.SplitEvenly(tuple(range(2)), 2)])
    self.assertEquals([],
                      [len(x) for x in gcs.SplitEvenly([], 2)])


if __name__ == '__main__':
  basetest.main()
