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

"""App Engine Handlers for handling AngularJS ngResource requests for Models.

The very easiest way to use this is to have your models inherit from
crud_model.CrudNdbModel you can then easily Create, Read, Update and
Delete your objects by adding this to you WSGIApplication:

    ('/data/user.*', crud_handler.GetCrudHandler(model.User)),

Then in your angular code:

angular.module('example.services.User', ['ngResource']).
  factory('User', function($resource) {
    return $resource('data/user/?id=:id', {id: '@id'}, {});
  });

Now you can use the User object in your controllers.

If you need access control implement an IsAllowed method in your
model object.

  def IsAllowed(self, action, user):
    return users.is_current_user_admin()

action is one of crud_handler.CREATE, READ, UPDATE, DELETE or QUERY
user is the user returned by users.get_current_user()

By default the handler will return all instances of the model you are
authorized to access to a query request. If you want to modify the
query before the access control is checked, you can define a static
Search method.

  @staticmethod
  def Search(query, request):
    "Only allow one instance of this class to be returned."
    for x in query:
      return [x]
    return [AppConfig()]

# Another Search example (all users for admins, only yourself otherwise).

  @staticmethod
  def Search(query, request):
    if users.is_current_user_admin():
      return query  # All Users.
    user = users.get_current_user()
    return query.filter(User.email == user.email())

# Another example (only allow Tables for which you are an owner).

  @staticmethod
  def Search(query, request):
    if users.is_current_user_admin():
      return query  # All Tables.
    user = users.get_current_user()
    user = User.query(User.email == user.email()).fetch(1)
    return query.filter(ancestor == user.key)

It is also possible to have handlers from existing ndb.Model
classes. If you wish to implement authorization and query filters then
you must pass them in to the GetCrudHandler method.

You can also just inherit from CrudHandler with your own Handler if you wish.
"""



import collections
import datetime
import json

import webapp2

from google.appengine.api import users
from google.appengine.ext import db
from google.appengine.ext import ndb

from lib.crud import crud_model


# Use this JSON_PREFIX to avoid this subtle JSON Vulnerability
# haacked.com/archive/2008/11/20/anatomy-of-a-subtle-json-vulnerability.aspx
JSON_PREFIX = ")]}',\n"

# TODO(user) see appengine binary codelab for xsrf


def JsonPrinter(json_dict):
  if (isinstance(json_dict, datetime.datetime) or
      isinstance(json_dict, datetime.date)):
    return json_dict.isoformat() + 'Z'


def IsAdmin(unused_action, unused_user, unused_entity):
  """An authorized_function that checks to see if user is an admin."""
  return users.is_current_user_admin()


def IsOwner(unused_action, user, entity):
  """An authorized_function that checks to see if user is an entity owner."""
  return hasattr(entity, 'owner') and entity.owner == user.user_id()


def SingletonFactory(default):
  def Singleton(query, unused_request):
    for x in query:
      return [x]
    return [default]
  return Singleton


def GetCrudHandler(model,
                   authorized_function=None,
                   search_function=None,
                   create_function=None):
  """Return a CrudHandler class for this model.

  Args:
    model: the Model class to serve results for.
    authorized_function: is a function that takes 3 arguments
        action, user, entity and returns True if the user is allowed
        to perform that action on that entity. The action argument is
        one of crud_model.Actions.CREATE, READ, UPDATE, DELETE or QUERY.
        For advanced use authorized_function can be an array of
        authorized_functions. Or it can be a dict of action:
        authorized_functions. With default being deny. the dict can
        also have a special key of ANY. the authorized_function
        associated with ANY will be called if no specific key is
        found.
        If you have your own classes inherited from CrudModel then
        it's much easier to implement a IsAllowed(self, action, user)
        method.
    search_function: is a function that takes a query and a request
        object and returns a new query object (possibly filtering out or
        ordering results based on values from the request object.
        If you have your own classes inherited from CrudModel then
        it's much easier to implement a static Search(query, request) method.
    create_function: is a function that creates a new instance of the model.
        currently only parent=parent_key is passed in.
        If you have your own classes inherited from CrudModel then
        it's much easier to implement this in your __init__ method.
        Don't forget to call super()!
  Returns:
    a Class suitable for passing to webapp2.WSGIApplication.
  """

  the_model = model
  the_authorized_function = authorized_function
  the_search_function = search_function
  the_create_function = create_function

  class SpecificHandler(CrudHandler):
    model = staticmethod(the_model)
    authorized_function = staticmethod(the_authorized_function)
    search_function = staticmethod(the_search_function)
    create_function = staticmethod(the_create_function)

  return SpecificHandler


def _HasProperty(model, prop):
  if hasattr(model, 'properties'):
    return prop in model.properties()
  else:
    # TODO(user) work out a way to do this without hitting a private attribute.
    return prop in model._properties  # pylint: disable=protected-access


class CrudHandler(webapp2.RequestHandler):
  """An abstract Handler for crud requests."""

  model = None
  authorized_function = None
  search_function = None
  create_function = None

  def IsAuthorizedFunction(self, f, action, user, entity):
    """Call function f to see if user is authorized to do action on entity."""
    if hasattr(f, 'get'):
      # f is a dictionary of 'action: function' to call
      f = f.get(action, f.get(crud_model.Actions.ANY))
      if f:
        # we recurse on IsAuthorizedFunction because f might be an array
        return self.IsAuthorizedFunction(f, action, user, entity)
      else:
        return False
    elif isinstance(f, collections.Iterable):
      # f is an array/tuple of functions that must pass
      functions = f
      for f in functions:
        if not f(action, user, entity):
          return False
      return True
    else:
      # f is a plain simple function to call and return the value from.
      return f(action, user, entity)

  def IsAuthorized(self, action, user, entity):
    """Check if user is authorized for action on entity."""
    if self.authorized_function:
      return self.IsAuthorizedFunction(self.authorized_function,
                                       action, user, entity)
    if hasattr(entity, 'IsAuthorized'):
      return entity.IsAuthorized(action, user)
    return True

  def _GetJsonDict(self, entity, *args, **kwargs):
    if hasattr('entity', 'AsJsonDict'):
      return entity.AsJsonDict(*args, **kwargs)
    else:
      return crud_model.JsonDictFromEntity(entity, *args, **kwargs)

  def _CreateNewEntity(self, *args, **kwargs):
    # pylint: disable=not-callable
    if self.create_function:
      return self.create_function(*args, **kwargs)
    else:
      return self.model(*args, **kwargs)

  def get(self):
    """Return json for a specific or new entity. or search results."""
    user = users.get_current_user()

    # This could be a query (search) or a req. for a specific (or new) entity.
    key_id = self.request.get('id')
    parent_id = self.request.get('parent_id')
    if parent_id and hasattr(self.model, 'parent_model_name'):
      if parent_id == crud_model.NEW_ENTITY_ID:
        return
      if issubclass(self.model, ndb.Model):
        parent_key = ndb.Key(self.model.parent_model_name, long(parent_id))
      else:
        parent_key = db.Key.from_path(self.model.parent_model_name,
                                      long(parent_id))
    else:
      parent_key = None

    if not key_id:
      if parent_key:
        if hasattr(self.model, 'ancestor'):
          all_models = self.model.query().ancestor(parent_key)
        else:
          all_models = self.model.query(ancestor=parent_key)
      else:
        all_models = self.model.query()
      if _HasProperty(self.model, 'active'):
        all_models = all_models.filter('active >', False)
      if self.search_function:
        # pylint: disable=not-callable
        all_models = self.search_function(all_models, self.request)
      if hasattr(self.model, 'Search'):
        all_models = self.model.Search(all_models, self.request)
      # Now it's possible that all_models is now just a single model
      if isinstance(all_models, db.Model) or isinstance(all_models, ndb.Model):
        if not self.IsAuthorized(crud_model.Actions.QUERY, user, all_models):
          self.error(403)
          return
        ans = self._GetJsonDict(all_models)
      else:
        all_models = [x for x in all_models
                      if self.IsAuthorized(crud_model.Actions.QUERY, user, x)]
        ans = [self._GetJsonDict(ex) for ex in all_models]
    else:
      if key_id == crud_model.NEW_ENTITY_ID:
        ex = self._CreateNewEntity(parent=parent_key)
        if not self.IsAuthorized(crud_model.Actions.CREATE, user, ex):
          self.error(403)
          return
      else:
        ex = self.model.get_by_id(long(key_id), parent=parent_key)
        if not self.IsAuthorized(crud_model.Actions.READ, user, ex):
          self.error(403)
          return
      ans = self._GetJsonDict(ex)
      if key_id == crud_model.NEW_ENTITY_ID:
        ans['id'] = crud_model.NEW_ENTITY_ID
    self.response.headers['Content-Type'] = 'text/json'
    self.response.out.write(JSON_PREFIX + json.dumps(ans, default=JsonPrinter))

  def post(self):
    """Handle post requests to update an entity.

    If you want your own method to be called with the datastore entity
    simply define your own method called postEntity that takes an
    entity and the json dictionary as a parameter and returns a json
    dictionary to send back to the client.
    """
    user = users.get_current_user()
    key_id = self.request.get('id')
    parent_id = self.request.get('parent_id')
    if parent_id and hasattr(self.model, 'parent_model_name'):
      if parent_id == crud_model.NEW_ENTITY_ID:
        return
      if issubclass(self.model, ndb.Model):
        parent_key = ndb.Key(self.model.parent_model_name, long(parent_id))
      else:
        parent_key = db.Key.from_path(self.model.parent_model_name,
                                      long(parent_id))
    else:
      parent_key = None

    if key_id == crud_model.NEW_ENTITY_ID:
      ex = self._CreateNewEntity(parent=parent_key)
    else:
      ex = self.model.get_by_id(long(key_id), parent=parent_key)

    if not self.IsAuthorized(crud_model.Actions.UPDATE, user, ex):
      self.error(403)
      return

    ex.UpdateFromJsonDict(json.loads(self.request.body))
    if hasattr(ex, 'last_updated_by'):
      ex.last_updated_by = user.email()

    if hasattr(self, 'postEntity'):
      ans = self.postEntity(ex, json.loads(self.request.body))
    else:
      key = ex.put()
      ans = self._GetJsonDict(ex)
      ans['id'] = key.id()
    self.response.headers['Content-Type'] = 'text/json'
    self.response.out.write(JSON_PREFIX + json.dumps(ans, default=JsonPrinter))

  def delete(self):
    """Delete an Entity."""
    user = users.get_current_user()
    parent_id = self.request.get('parent_id')
    if parent_id:
      if issubclass(self.model, ndb.Model):
        parent_key = ndb.Key(self.model.parent_model_name, long(parent_id))
      else:
        parent_key = db.Key.from_path(self.model.parent_model_name,
                                      long(parent_id))
    else:
      parent_key = None
    ex = self.model.get_by_id(long(self.request.get('id')),
                              parent=parent_key)
    if not self.IsAuthorized(crud_model.Actions.DELETE, user, ex):
      self.error(403)
      return
    if hasattr(ex, 'delete'):
      ex.delete()
    else:
      ex.key.delete()
    # TODO(user): send some kind of success?
