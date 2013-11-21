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

"""Application Configuration."""


from google.appengine.api import users
from google.appengine.ext import ndb

from lib.crud import crud_model

OPTIONS_STORAGE_KEY = 'storage'
OPTIONS_STORAGE_BUCKET_KEY = 'bucket'
OPTIONS_STORAGE_PREFIX_KEY = 'prefix'


class AppConfig(crud_model.CrudNdbModel):
  """Configuration details for this app."""
  cloud_storage_bucket = ndb.StringProperty()  # e.g. gs://bucketname
  cloud_storage_prefix = ndb.StringProperty()  # e.g. test/

  def IsAllowed(self, action, unused_user):
    """Anyone can read and query, but only admins can write."""
    if action in (crud_model.Actions.READ, crud_model.Actions.QUERY):
      return True
    return users.is_current_user_admin()

  @staticmethod
  def Search(query, unused_request):
    """Only allow one instance of this class to be returned."""
    for x in query:
      return x
    return AppConfig()

  @staticmethod
  def GetAppConfig():
    """Utility method to get the AppConfig."""
    app_config = AppConfig.query().get()
    if not app_config:
      app_config = AppConfig()
      app_config.put()
    return app_config

  def AsOptionsDict(self):
    """Return the current object as an options dict object."""

    bucket_name = self.cloud_storage_bucket or ''
    if bucket_name.startswith('gs://'):
      bucket_name = bucket_name[5:]

    return {
        OPTIONS_STORAGE_KEY: {
            OPTIONS_STORAGE_BUCKET_KEY: bucket_name,
            OPTIONS_STORAGE_PREFIX_KEY: self.cloud_storage_prefix or '',
            }
        }
