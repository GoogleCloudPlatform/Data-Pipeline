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

"""Data Pipeline Server unit tests."""

import cStringIO

import logging
from src import basetest
from src import server


class ServerTest(basetest.TestCase):

  def testOkHandler(self):
    handler = server.OkHandler()

    class MockResponse(object):
      out = cStringIO.StringIO()
    handler.response = MockResponse()
    handler.get()
    ans = handler.response.out.getvalue()
    self.assertEqual(ans, 'ok')


if __name__ == '__main__':
  basetest.main()
