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

"""Base class for Appengine Unit Tests."""




from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import testbed

from google_apputils import basetest


class TestCase(basetest.TestCase):
  """Base class for unit tests used by the opensource code."""

  def setUp(self):
    super(TestCase, self).setUp()
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    # Don't use init_all_stubs here because we need to exclude the unused
    # Images API due to conflicts with the PIL library and virtualenv tests.
    # http://stackoverflow.com/questions/2485295
    self.testbed.init_app_identity_stub()
    self.testbed.init_blobstore_stub()
    self.testbed.init_capability_stub()
    self.testbed.init_channel_stub()
    # self.testbed.init_datastore_v3_stub()  # Done later.
    self.testbed.init_files_stub()
    # self.testbed.init_images_stub()  # Intentionally excluded.
    self.testbed.init_logservice_stub()
    self.testbed.init_mail_stub()
    self.testbed.init_memcache_stub()
    self.testbed.init_taskqueue_stub()
    self.testbed.init_urlfetch_stub()
    self.testbed.init_user_stub()
    self.testbed.init_xmpp_stub()
    self.policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(
        probability=1)
    self.testbed.init_datastore_v3_stub(consistency_policy=self.policy)

  def tearDown(self):
    super(TestCase, self).tearDown()
    self.testbed.deactivate()


def main(*args, **kwargs):
  """Call the unit test framework."""
  basetest.main(*args, **kwargs)
