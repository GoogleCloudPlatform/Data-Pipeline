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

"""Pipeline hanlder utilities unit tests."""

import mock

from src import basetest
from src.handlers import basehandler


class BaseHanlderTest(basetest.TestCase):
  """Tests the pipeline utils."""

  def testResponseHelper(self):
    mock_response = mock.MagicMock()
    bh = basehandler.RequestHandler()
    bh.response = mock_response

    msg = 'you call that a request'
    bh.BadRequest(msg)
    mock_response.write.assert_called_once_with(msg)
    self.assertEquals(400, mock_response.status)

    mock_response.reset_mock()
    msg = 'this is not the test you are looking for'
    bh.NotFound(msg)
    mock_response.write.assert_called_once_with(msg)
    self.assertEquals(404, mock_response.status)


if __name__ == '__main__':
  basetest.main()
