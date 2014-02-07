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

"""Tests for runhandler.py."""



import logging

from src import basetest
from src.handlers import runhandler


class OptionsTest(basetest.TestCase):

  def testExpandOptionsDictNoArgs(self):
    options = {'a': 2}
    orig_options = options.copy()
    runhandler.RunHandler.expandOptionsDict(options, {})
    self.assertSameStructure(orig_options, options)

  def testExpandOptionsDictNewArgs(self):
    options = {'a': 2}
    runhandler.RunHandler.expandOptionsDict(options, {'b': 3})
    self.assertSameStructure({'a': 2, 'b': 3}, options)

  def testExpandOptionsDictNewNestedArgs(self):
    options = {'a': 2}
    runhandler.RunHandler.expandOptionsDict(options, {'b.c': 3})
    self.assertSameStructure({'a': 2, 'b': {'c': 3}}, options)

  def testExpandOptionsDictOverwriteValues(self):
    options = {'a': 2}
    runhandler.RunHandler.expandOptionsDict(options, {'a.c': 3})
    self.assertSameStructure({'a': 2}, options)


if __name__ == '__main__':
  basetest.main()
