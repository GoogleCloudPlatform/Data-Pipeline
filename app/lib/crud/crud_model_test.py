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

"""Unit tests for the CrudModel utilities."""




from google.appengine.ext import db
from google.appengine.ext import ndb

import logging
from lib.crud import crud_model
from lib.crud import crud_test


class SampleDb(crud_model.CrudDbModel):
  name = db.StringProperty()
  count = db.IntegerProperty()


class SampleNdb(crud_model.CrudNdbModel):
  name = ndb.StringProperty()
  count = ndb.IntegerProperty()


class CrudModelTest(crud_test.TestCase):
  """Unit tests we want to run on both db and ndb model entities.

  We want to test both google.appengine.ext.db and
  google.appengine.ext.ndb models so we have one base class that both
  test classes use.
  """

  def RunTestAsJsonDict(self, entity, as_dict):
    # First test a whole new entity
    json_dict = entity.AsJsonDict()
    new_entity_dict = as_dict.copy()
    new_entity_dict['id'] = crud_model.NEW_ENTITY_ID
    self.assertSameStructure(json_dict, new_entity_dict)

    # now test it if we have saved the object (we have an id now)
    key = entity.put()
    json_dict = entity.AsJsonDict()
    new_entity_dict['id'] = str(key.id())
    self.assertSameStructure(json_dict, new_entity_dict)

    # now test includes and excludes
    partial_obj_dict = new_entity_dict.copy()
    del partial_obj_dict['name']
    json_dict = entity.AsJsonDict(include=['count'])
    self.assertSameStructure(json_dict, partial_obj_dict)

    json_dict = entity.AsJsonDict(exclude=['name'])
    self.assertSameStructure(json_dict, partial_obj_dict)

    # Now id is 'special' it's always inserted unless we exclude it
    del partial_obj_dict['id']
    json_dict = entity.AsJsonDict(exclude=['name', 'id'])
    self.assertSameStructure(json_dict, partial_obj_dict)

  def RunTestUpdateFromJsonDict(self, entity):
    as_dict = entity.AsJsonDict()
    entity.UpdateFromJsonDict({'name': u'Pippy'})
    as_dict['name'] = u'Pippy'
    self.assertSameStructure(entity.AsJsonDict(), as_dict)


class CrudDbModelTest(CrudModelTest):
  """Run all the CrudModelTests on a classic db model."""

  def testAsJsonDict(self):
    sample = SampleDb(name='ark', count=66)
    self.RunTestAsJsonDict(sample, {'name': 'ark', 'count': 66})

  def testUpdateFromJsonDict(self):
    sample = SampleDb(name='ark', count=66)
    self.RunTestUpdateFromJsonDict(sample)


class CrudNdbModelTest(CrudModelTest):
  """Run all the CrudModelTests on an new ndb model."""

  def testAsJsonDict(self):
    sample = SampleNdb(name=u'ark', count=66)  # ndb likes unicode
    self.RunTestAsJsonDict(sample, {'name': u'ark', 'count': 66})

  def testUpdateFromJsonDict(self):
    sample = SampleNdb(name='ark', count=66)
    self.RunTestUpdateFromJsonDict(sample)


if __name__ == '__main__':
  crud_test.main()
