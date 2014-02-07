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

"""Handler for pipeline config linter."""

import json
import logging

import jinja2
from jinja2 import meta

from src.handlers import basehandler


class VariableHandler(basehandler.RequestHandler):
  """Lints a pipeline configuration."""

  def post(self):
    """Find and lint a pipeline."""
    p = json.loads(self.request.body)

    if not p or 'config' not in p:
      self.NotFound('Unable to find pipeline config in json request.')
    else:
      logging.info('config is:\n%r', p['config'])
      variable_names = GetVariableAttributes(p['config'])
      logging.info('var names is %r', variable_names)
      variables = p.get('variables', [])
      variables = dict([(v.get('name', ''), v) for v in variables])

      for v in set(variables.keys()) - variable_names:
        del variables[v]  # remove vars not in variable_names
      for v in variable_names:
        variables.setdefault(v, {'name': v})  # add missing variables
      p['variables'] = variables.values()
      logging.info('returning variables %r from %r', variables, variable_names)
      self.SendJson(p)


class _GetattrNodeVisitor(jinja2.visitor.NodeVisitor):
  """NodeVisitor class that keeps a set of top-level Getattr nodes in the tree.

  NodeVisitors walk the abstract syntax tree that Jinja generates by parsing a
  template, calling visitor functions for every node found. This class is used
  to visit and capture all top-level nodes of the type 'Getattr'.

  For the template '{{ foo.bar.baz }}', the abstract syntax tree will have the
  following Getattr node representation:

  Getattr(
    node=Getattr(
      node=Name(name='foo', ctx='load'),
      attr='bar',
      ctx='load'),
    attr='baz',
    ctx='load')

  There are two Getattr nodes: one for 'bar' and one for 'baz'. This class will
  visit the outer-most Getattr node and add it to a set. When the tree walk
  completes, we will be able to extract 'foo.bar.baz' from the visited node.
  """

  def __init__(self):
    """Initializes the NodeVisitor, creating a set for saving Getattr nodes."""
    jinja2.visitor.NodeVisitor.__init__(self)
    self.getattr_nodes = set()
  def visit(self, node):
    """Begin walking the tree at the given node.

    The NodeVisitor's visit() method allows additional arguments that will get
    passed on the visit_*() methods. This node visitor class does not support
    additional arguments, so we override the visit() method as well.

    Args:
      node: The node to visit.
    """
    super(_GetattrNodeVisitor, self).visit(node)
  def visit_Getattr(self, node):
    """Adds visited Getattr nodes to the internal set.

    We are only interested in the top-level Getattr nodes, so this visit method
    does not recursively process the Getattr node's children.

    Args:
      node: The node being visited.
    """
    self.getattr_nodes.add(node)


def _GetAttributeList(node, attr_list=None):
  """Creates a list of variable attribute references.

  Attributes are represented in the Jinja2 abstract syntax tree in reverse
  order: the rightmost attribute is the outermost node. For example, the repr
  of a node representing 'foo.bar' in the tree is:

  Getattr(node=Name(name='foo', ctx='load'), attr='bar', ctx='load').

  This method will recursively parse a Getattr node and create a list of the
  variable name and its attributes.

  Args:
    node: A Jinja2 node. Should be either a Getattr or Name node.
    attr_list: The list of attributes previously referenced when recursively
        processing a Getattr node.

  Returns:
    A list consisting of a variable name and the attributes referenced on it.
  """
  attr_list = attr_list or []
  if isinstance(node, jinja2.nodes.Getattr):
    attr_list.insert(0, node.attr)
    _GetAttributeList(node.node, attr_list)
  elif isinstance(node, jinja2.nodes.Name):
    attr_list.insert(0, node.name)
  return attr_list


def GetVariableAttributes(template_src, env=None):
  """Returns a set of all variable attributes in a template.

  This returns variable attribute names, not the top-level variable names.
  Attributes are included if they belong to undeclared variables in the
  template. If we have a template with a for loop:

  {% for item in foo.bar %}
    {{ item.baz }}
  {% endfor %}

  this method will return a set with 'foo.bar' because 'foo' is an undeclared
  variable in the template. The attribute 'item.baz' will be excluded because
  'item' is a declared variable. For example:

  template_util.GetVariables('{{ foo.bar }}')  # ['foo.bar']

  This leverages the Jinja2 Meta API. See the tests and external Jinja docs
  for more info: http://jinja.pocoo.org/docs/api/#the-meta-api.

  Args:
    template_src: The template string.
    env: Optional Jinja2 Environment.

  Returns:
    A set of all variable attributes in the template that will be looked up
    from the context.
  """
  env = env or jinja2.Environment()
  abstract_syntax_tree = env.parse(template_src)
  node_visitor = _GetattrNodeVisitor()
  node_visitor.visit(abstract_syntax_tree)

  output = set()
  undeclared_variables = meta.find_undeclared_variables(abstract_syntax_tree)
  used_variables = set()
  for node in node_visitor.getattr_nodes:
    attr_list = _GetAttributeList(node)
    if attr_list[0] in undeclared_variables:
      used_variables.add(attr_list[0])
      output.add('.'.join(attr_list))
  return output | (undeclared_variables - used_variables)
