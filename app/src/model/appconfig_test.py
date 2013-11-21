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

"""Unit tests for model.AppConfig."""


from google.appengine.api import users
from lib.crud import crud_model
from src import basetest
from src.model import appconfig


class ModelAppConfigTest(basetest.TestCase):
  """Run all the ModelAppConfigTests."""

  def testIsAllowed(self):
    ac = appconfig.AppConfig.GetAppConfig()
    self.assertTrue(ac.IsAllowed(crud_model.Actions.READ, None))
    self.assertFalse(users.is_current_user_admin())
    self.assertFalse(ac.IsAllowed(crud_model.Actions.UPDATE, None))


if __name__ == '__main__':
  basetest.main()
