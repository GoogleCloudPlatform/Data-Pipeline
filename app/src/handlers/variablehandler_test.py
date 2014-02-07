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

"""Tests for variablehandler.py."""

from src import basetest
from src.handlers import variablehandler

_TEMPLATE = '{{foo}} {{foo.bar}} {{baz}}'
_TEMPLATE_WITH_ASSIGNMENT = '{% set baz = foo.bar %}'
_TEMPLATE_WITH_FILTER = '{{ foo.bar|default("foo.bar is not defined") }}'
_TEMPLATE_WITH_FOR = """
    {% for item in foo.bar %}
      {{ item.baz }}
    {% endfor %}
    """
_TEMPLATE_WITH_IF = """
    {% if foo.bar %}
      {{ foo.bar }}
    {% else %}
      {{ foo.baz }}
    {% endif %}
    """
_TEMPLATE_WITH_MANY_ATTRIBUTES = '{{ foo.bar.baz.qux.quux }}'
_TEMPLATE_WITH_MANY_ATTRIBUTES2 = '''{{ foo.bar.baz.qux.quux }}
    {{ foo.bar.ball }} {{goo}}'''


class TemplateUtilTest(basetest.TestCase):

  def AssertVariableAttributesEqual(self, var_list, template_src):
    self.assertItemsEqual(
        var_list, variablehandler.GetVariableAttributes(template_src))

  def testFindUndeclaredDictVariables(self):
    self.AssertVariableAttributesEqual(['foo.bar', 'baz'], _TEMPLATE)
    self.AssertVariableAttributesEqual(['foo.bar'], _TEMPLATE_WITH_ASSIGNMENT)
    self.AssertVariableAttributesEqual(['foo.bar'], _TEMPLATE_WITH_FILTER)
    self.AssertVariableAttributesEqual(['foo.bar'], _TEMPLATE_WITH_FOR)
    self.AssertVariableAttributesEqual(
        ['foo.bar', 'foo.baz'], _TEMPLATE_WITH_IF)
    self.AssertVariableAttributesEqual(
        ['foo.bar.baz.qux.quux'], _TEMPLATE_WITH_MANY_ATTRIBUTES)
    self.AssertVariableAttributesEqual(
        ['foo.bar.baz.qux.quux', 'foo.bar.ball', 'goo'],
        _TEMPLATE_WITH_MANY_ATTRIBUTES2)
    with self.assertRaises(Exception):
      variablehandler.GetVariableAttributes(' and {% endif %}')

if __name__ == '__main__':
  basetest.main()
