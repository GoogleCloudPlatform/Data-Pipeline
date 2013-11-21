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

"""Unit tests for the CrudHandler utilities."""



import cStringIO
import json


from google.appengine.ext import ndb

import logging
from lib.crud import crud_handler
from lib.crud import crud_model
from lib.crud import crud_test


class SampleNdb(crud_model.CrudNdbModel):
  name = ndb.StringProperty()
  count = ndb.IntegerProperty()


class CrudHandlerTest(crud_test.TestCase):
  """Tests the CrudHandler."""

  def assertJsonReplyEqual(self, ans, expected):
    """Make sure that two json strings are the same."""
    self.assertTrue(ans.startswith(crud_handler.JSON_PREFIX))
    self.assertTrue(expected.startswith(crud_handler.JSON_PREFIX))

    ans = json.loads(ans[len(crud_handler.JSON_PREFIX):])
    expected = json.loads(expected[len(crud_handler.JSON_PREFIX):])
    self.assertSameStructure(ans, expected)

  def testGetNew(self):
    handler = crud_handler.GetCrudHandler(SampleNdb)()
    handler.request = {'id': 'new'}

    class MockResponse(object):
      out = cStringIO.StringIO()
      headers = {}
    handler.response = MockResponse()
    handler.get()
    ans = handler.response.out.getvalue()
    expected = """)]}',\n{"id": "new", "name": null, "count": null}"""
    self.assertJsonReplyEqual(ans, expected)


if __name__ == '__main__':
  crud_test.main()
