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

"""test the bigquery calls."""


import logging
from src import basetest
from src.clients import bigquery


class TestBigQueryFunctions(basetest.TestCase):

  def testMakeValidHeader(self):
    self.assertEquals('', bigquery.MakeValidFieldName(''))
    self.assertEquals('', bigquery.MakeValidFieldName('&^*^'))
    self.assertEquals('ark', bigquery.MakeValidFieldName('ark'))
    self.assertEquals('ark', bigquery.MakeValidFieldName(' ark '))
    self.assertEquals('ark', bigquery.MakeValidFieldName('"ark"'))
    self.assertEquals('col_2003', bigquery.MakeValidFieldName('2003'))
    self.assertEquals('col_2003', bigquery.MakeValidFieldName(' 2003 '))
    self.assertEquals('col_2003', bigquery.MakeValidFieldName('_2003 '))
    self.assertEquals('ark_2003', bigquery.MakeValidFieldName(' ark 2003 '))
    self.assertEquals('ark_2003', bigquery.MakeValidFieldName(' ark (2003) '))
    self.assertEquals('ark_2003',
                      bigquery.MakeValidFieldName(' ark -(2003)&#@ '))
    self.assertEquals('ark_2',
                      bigquery.MakeValidFieldName('ark__2'))
    self.assertEquals('ark_2',
                      bigquery.MakeValidFieldName('ark___2'))
    self.assertEquals('ark_2',
                      bigquery.MakeValidFieldName('ark____2'))

  def testMakeValidTableName(self):
    self.assertEquals('_', bigquery.MakeValidTableName(''))
    self.assertEquals('ark', bigquery.MakeValidTableName('ark'))
    self.assertEquals('_ark', bigquery.MakeValidTableName('_ark'))
    self.assertEquals('_ark99', bigquery.MakeValidTableName('_ark99'))
    self.assertEquals('_9ark', bigquery.MakeValidTableName('9ark'))
    self.assertEquals('_9ark66',
                      bigquery.MakeValidTableName(' &^@*#9ark#&$^$^66'))


if __name__ == '__main__':
  basetest.main()
