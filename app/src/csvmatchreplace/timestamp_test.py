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

"""Test the timestamp processing functions."""


import logging
from src import basetest
from src.csvmatchreplace import timestamp


class TestTimestamp(basetest.TestCase):

  def testConvertFmtToRe(self):
    lookup = {'a': 'apple', 'b': 'bat', 'c': 'cat'}
    tests = (
        ('', ''),
        ('%a', 'apple'),
        ('%a ', r'apple\ '),
        (' %a', r'\ apple'),
        (' %a ', r'\ apple\ '),
        ('%a-%b-%c', r'apple\-bat\-cat'),
        ('%b%b%b', r'batbatbat'),
        # check that optional things are optional
        ('%b%b%b%c', r'batbatbat(cat)?'),
        ('%b%b%b%c%c', r'batbatbat(cat(cat)?)?')
        )
    for fmt, expected in tests:
      self.assertEquals(expected, timestamp.ConvertFmtToRe(fmt,
                                                           lookup=lookup))

  def testLooksLikeTimestamp(self):
    ts = ('1989-10-02 05:23:48',
          '1958-06-24T12:18:35.5803',
          '1988-08-15T19:06:56.235',
          '2012-12-12',
          '2012-12-12 19:45')
    for t in ts:
      self.assertTrue(timestamp.LooksLikeTimestamp(t),
                      '%s should look like a timestamp' % t)

  def testNormalizeTimeStamp(self):
    ts = (
        ('1989-10-02 05:23:48', '1989-10-02 05:23:48.000000 '),
        ('2006-06-05', '2006-06-05 00:00:00.000000 '),
        ('10OCT2012:05:20:00.000000', '2012-10-10 05:20:00.000000 '),
        ('1983-01-28 15:12:31.488416', '1983-01-28 15:12:31.488416 '),
        (u'1971-09-01 04:00:30.942295 ', '1971-09-01 04:00:30.942295 ')
        )
    for t, expected in ts:
      self.assertEquals(expected, timestamp.NormalizeTimeStamp(t))

  def testOneOff(self):
    """Useful for --test_arg=TestTimestamp.testOneOff to test one thing."""
    s = u'1971-09-01 04:00:30.942295 '
    self.assertEquals(s, timestamp.NormalizeTimeStamp(s))


if __name__ == '__main__':
  basetest.main()
