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

"""Utilities for CRUD apps."""

import random
import string


def GenerateRandomString(length=30):
  """Make a unique string that is difficult to guess."""
  valid_chars = ''.join(set(string.ascii_lowercase) - set('aeiou'))
  valid_chars += valid_chars.upper() + string.digits
  return ''.join(random.choice(valid_chars)
                 for _ in range(length))


def ClassAsJsonDict(o, ans=None):
  """Turn all the int values from a class into a json object.

  This is useful if you have a class of constants which you wish to
  send as a json object for the Angular UI to use the same constant
  values.

  Args:
    o: a class or object with integer members.
    ans: an optional dictionary to update
  Returns:
    a dictionary of key/value pairs from the o object.
  """
  if ans is None:
    ans = {}
  for key, val in o.__dict__.items():
    if isinstance(val, int):
      ans[key] = val
  return ans


def DictAsArrayForNgOptions(o):
  """Turn a dict into an array suitable to make a select with ng-options."""
  return [{'name': name, 'key': key} for (key, name) in o.items()]
