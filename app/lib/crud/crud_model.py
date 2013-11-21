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

"""Model Classes that support json serialization and deserialization.

See crud_handler for an easy to use way to stream these to AngularJS ngResource.
"""



import collections

import dateutil.parser

from google.appengine.ext import db
from google.appengine.ext import ndb

# id used when we have a new entity with no id.
NEW_ENTITY_ID = 'new'


class Actions(object):
  """Constants used for action_type parameters."""
  CREATE = 1
  READ = 2
  UPDATE = 3
  DELETE = 4
  QUERY = 5
  ANY = 6


def _UpdateDictFromDbEntity(entity, json_dict, include=None, exclude=None):
  """Add all the members of an entity to the json_dict.

  We only need this for db entities since ndb entities have a to_dict method.

  Args:
    entity: the model entity to get the values from (db.Model or ndb.Model).
    json_dict: the dict to add our values to.
    include: a list of keys to use. If None all keys will be used.
    exclude: a list of keys to not use (overrides "include" if key is in both).
  """
  keys = set(entity.properties().keys())
  if include:
    keys = keys.intersection(include)
  if exclude:
    keys -= set(exclude)
  for key in keys:
    json_dict[key] = getattr(entity, key)


def _GetKey(entity):
  """Get the key for a db or ndb model.

  db models return the key if you call a method called key, ndb models
  just have it as a member variable.

  Args:
    entity: the model entity to get the values from (db.Model or ndb.Model).
  Returns:
    a datastore key or None.
  """
  try:
    if callable(entity.key):
      return entity.key()
    elif hasattr(entity, 'key'):
      return entity.key
  except db.NotSavedError:
    pass
  return None


def JsonDictFromEntity(entity, entity_id=None, json_dict=None,
                       include=None, exclude=None):
  """Make a dict suitable for json encoding from a db or ndb model.

  Args:
    entity: the model entity to get the values from (db.Model or ndb.Model).
    entity_id: the id to use for this entity (string). If None the
        entity key's id or name will be used, or crud_model.NEW_ENTITY_ID
        if no key is available (the entity has not be saved in datastore).
    json_dict: a dict to fill in. If None is provided it will be created.
    include: a list of keys you want added to the dict.
    exclude: a list of keys to not use (overrides "include" if key is in both).
  Returns:
    A dict suitable for json encoding representing the entity parameter.
  """
  if json_dict is None:
    json_dict = {}
  if hasattr(entity, 'to_dict'):
    # It's an ndb entity which has a useful to_dict method
    json_dict.update(entity.to_dict(include=include, exclude=exclude))
  else:
    _UpdateDictFromDbEntity(entity, json_dict, include=include, exclude=exclude)
  # Some models provide a AddToJsonDict to call to add more to the dict.
  if hasattr(entity, 'AddToJsonDict'):
    entity.AddToJsonDict(json_dict)

  key = None
  if not exclude or 'id' not in exclude:
    if entity_id is None:
      key = _GetKey(entity)
      if key is None:
        entity_id = NEW_ENTITY_ID
      else:
        entity_id = key.id()
    # JavaScript can only accurately parse numbers less than 52 bits.
    # Key id's might be longer than this so we convert to string.
    json_dict['id'] = str(entity_id)
  if not exclude or 'parent_id' not in exclude:
    if key and key.parent():
      json_dict['parent_id'] = key.parent().id()
  return json_dict


def UpdateEntityFromJsonDict(entity, json_dict):
  """Update a db or ndb Model entity from a dict.

  Only attributes that are present in both the entity and the
  json_dict will be updated in the entity.

  Args:
    entity: the model entity to get the values from (db.Model or ndb.Model).
    json_dict: a dict containing key/values to be updated in the entity.
  """
  if hasattr(entity, 'ExtractFromJsonDict'):
    # allow the entity to massage the data before reading in.
    entity.ExtractFromJsonDict(json_dict)

  for key, value in json_dict.items():
    if key not in ('id', 'created', 'key', 'last_updated'):
      if hasattr(entity, key):
        prop_type = _GetEntityPropertyType(entity, key)
        # logging.info('%r type %r setting %r', key, prop_type, value)
        if prop_type == 'IntegerProperty':
          if value:
            if (isinstance(value, collections.Iterable) and
                not isinstance(value, basestring)):
              value = map(int, value)
            else:
              value = int(value)
        elif prop_type == 'DateTimeProperty':
          if value:
            if (isinstance(value, collections.Iterable) and
                not isinstance(value, basestring)):
              value = map(dateutil.parser.parse, value)
            else:
              value = dateutil.parser.parse(value)

        setattr(entity, key, value)


# following code adapted from: http://stackoverflow.com/questions/1440958/
def _GetEntityPropertyType(entity, pname):
  """Given the name of a property of a Model entity, return its type."""
  if hasattr(entity, 'properties'):
    return entity.properties().get(pname, None).__class__.__name__
  else:
    # TODO(user) work out a way to do this without hitting a private attribute.
    entity_properties = entity._properties  # pylint: disable=protected-access
    return entity_properties.get(pname, None).__class__.__name__


class CrudDbModel(db.Model):
  """A simple subclass of db.Model class with support for json serialization.

  Instead of having your classes inherit from db.Model inherit from
  this and you'll get AsJsonDict and UpdateFromJsonDict methods.
  """

  # If your model has a parent type, you need to set it here as a string.
  parent_model_name = None

  def AsJsonDict(self, entity_id=None, json_dict=None,
                 include=None, exclude=None):
    """Make a dict suitable for json encoding from this model entity."""
    return JsonDictFromEntity(self, entity_id=entity_id, json_dict=json_dict,
                              include=include, exclude=exclude)

  def UpdateFromJsonDict(self, json_dict):
    """Update this entity from the json_dict."""
    return UpdateEntityFromJsonDict(self, json_dict)


class CrudNdbModel(ndb.Model):
  """A simple subclass of ndb.Model class with support for json serialization.

  see: CrudDbModel.
  """

  def AsJsonDict(self, entity_id=None, json_dict=None,
                 include=None, exclude=None):
    """Make a dict suitable for json encoding from this model entity."""
    return JsonDictFromEntity(self, entity_id=entity_id, json_dict=json_dict,
                              include=include, exclude=exclude)

  def UpdateFromJsonDict(self, json_dict):
    """Update this entity from the json_dict."""
    return UpdateEntityFromJsonDict(self, json_dict)
