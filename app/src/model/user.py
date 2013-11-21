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

"""User Datastore object."""


from google.appengine.api import users
from google.appengine.ext import ndb

from lib.crud import crud_model


# Created when someone first shows up on the site.
class User(crud_model.CrudNdbModel):
  """A User of the Application."""
  email = ndb.StringProperty()
  created = ndb.DateTimeProperty(auto_now_add=True)
  last_updated = ndb.DateTimeProperty(auto_now=True)

  def AddToJsonDict(self, json_dict):
    """Called before a json dict is sent back to the client."""
    # If email is not in the dict make sure to add the current user.
    if not json_dict.get('email'):
      user = users.get_current_user()
      json_dict['email'] = user.email()
    json_dict['is_admin'] = users.is_current_user_admin()

  def IsAllowed(self, unused_action, user):
    return user.email() == self.email

  @staticmethod
  def Search(query, unused_request):
    """When a user searches for all users we only want to return one user."""
    user = users.get_current_user()
    ans = query.filter(User.email == user.email()).get()
    if not ans:
      ans = User(email=user.email())
      ans.put()
    return ans

  @staticmethod
  def GetUser(user=None):
    """Utility method to get the currently logged in user's User entity."""
    if user is None:
      user = users.get_current_user()
    return User.query().filter(User.email == user.email()).get()

  @staticmethod
  def GetUserKey(user=None):
    """Utility method to get the key of the current user's entity."""
    # TODO(user) look into if we can use the email address to make keys
    # withouth a datastore lookup.
    u = User.GetUser(user=user)
    return u.key
