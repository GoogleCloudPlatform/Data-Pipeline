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

"""Test functions."""

import collections


import logging
from lib.crud import crud_test
from lib.crud import crud_utils


class SimpleClass(object):
  FIRST = 0
  SECOND = 1
  IGNORE = 'String'
  ME_TOO = 2.3

  strings = collections.OrderedDict(((FIRST, '1st'),
                                     (SECOND, '2nd')))


class TestMethods(crud_test.TestCase):

  def testClassAsJsonDict(self):
    self.assertSameStructure({'FIRST': 0, 'SECOND': 1},
                             crud_utils.ClassAsJsonDict(SimpleClass))

  def testDictAsArrayForNgOptions(self):
    self.assertSameStructure([{'key': 0, 'name': '1st'},
                              {'key': 1, 'name': '2nd'}],
                             crud_utils.DictAsArrayForNgOptions(
                                 SimpleClass.strings))

  def testGenerateRandomString(self):
    self.assertEqual(30, len(crud_utils.GenerateRandomString(30)))
    self.assertEqual(10, len(crud_utils.GenerateRandomString(10)))


if __name__ == '__main__':
  crud_test.main()
