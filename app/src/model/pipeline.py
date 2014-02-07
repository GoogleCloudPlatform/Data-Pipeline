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

"""Datastore object containing information about a pipeline."""

import json
import uuid


from google.appengine.api import users
from google.appengine.ext import ndb

from lib.crud import crud_model
from src.model import user


class Pipeline(crud_model.CrudNdbModel):
  """A Pipeline to load data."""
  parent_model_name = 'User'

  # If you add more fields here that the user might edit then make sure you
  # add them to pipelineValueKeys in static/components/pipelines/pipelines.js
  name = ndb.StringProperty()
  api_key = ndb.StringProperty()
  config = ndb.TextProperty()
  variables = ndb.TextProperty(default='{}')  # JSON string of key/value dict
  created = ndb.DateTimeProperty(auto_now_add=True)
  last_updated = ndb.DateTimeProperty(auto_now=True)
  running_pipeline_ids = ndb.StringProperty(repeated=True)

  def IsAllowed(self, unused_action, this_user):
    """Is this user allowed to perform this action on this Pipeline."""
    ndb_user = user.User.query().filter(
        user.User.email == this_user.email()).get()
    return ndb_user.key == self.key.parent()

  @staticmethod
  def Search(query, unused_request):
    """Search for pipelines owned by this user (admins see all pipelines)."""
    if users.is_current_user_admin():
      return query  # All Pipelines.
    return Pipeline.query(ancestor=user.User.GetUserKey())

  def ExtractFromJsonDict(self, json_dict):
    """Called before a json dict is used to update this entity."""
    # Never update the API key from the UI
    json_dict.pop('api_key', None)
    # If we don't have an api_key then create one.
    if not self.api_key:
      self.api_key = str(uuid.uuid4())
    json_dict['variables'] = json.dumps(json_dict.get('variables', {}))

  def AddToJsonDict(self, json_dict):
    """Called before a json dict is sent to the client."""
    json_dict['variables'] = json.loads(json_dict.get('variables', '{}'))
