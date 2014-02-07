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

"""Pipeline configuration linter."""

import collections
import copy
import datetime
import json
import sys
import types

import jinja2
from jinja2 import exceptions as jinja2_exceptions
from jsonminify import minify_json

from google.appengine.api import app_identity

from src.pipelines import pipelines


class PipelineLinter(object):
  """Linter for a pipeline's configuration."""

  VALID_ROOT_KEYS = set(('inputs', 'outputs', 'transforms', 'options'))
  CHECK_SYNTAX_VALID = 'SyntaxValid'
  CHECK_TEMPLATE_VALID = 'TemplateValid'
  CHECK_REQ_IO_STAGES = 'HasOneInputOrOutputStage'
  CHECK_UNKNOWN_CONFIG_KEYS = 'NoUnknownKeys'
  MSG_MISSING_IO_STAGES = 'Must have at least one "inputs" or "outputs" stage.'
  MSG_UNKNOWN_CONFIG_KEYS_FMT = 'Unrecognized config keys: %s'

  def __init__(self,
               config_json,
               default_options=None):
    self.results = LintResults()
    self.config = None
    self.Lint(config_json, default_options)

  def Lint(self, config_json, default_options):
    """Lint the pipeline.

    Performs the following steps:
      1. Strips any comments.
      2. Validates JSON syntax.
      3. Adds default options.
      4. Substitutes for embedded variables and re-checks syntax.
      5. Runs per-stage linting/

    Args:
      config_json: The pipeline configuration JSON rep.
      default_options: a default options section to merge in with the config.
    """
    self.SyntaxCheck(config_json)
    if self.config and default_options:
      self.AddDefaultOptions(default_options)
      config_json = self.ExpandTemplateVariables()
      # json parse it a second time.
      self.SyntaxCheck(config_json, phase='PostTemplate: ')
    self.StageCheck()

  def AddDefaultOptions(self, default_options):
    # take self.config and merge in the default_options
    options = self.config.get('options', {})
    if default_options:
      UpdateNestedDict(options, default_options)
    self.config['options'] = options

  def ExpandTemplateVariables(self):
    template_variables = self.GetTemplateVariables()
    config_json = json.dumps(self.config, indent=2, separators=(',', ': '))
    try:
      config_json = jinja2.Template(config_json).render(template_variables)
    except jinja2_exceptions.TemplateSyntaxError as err:
      self.results.AddCheckResults(self.CHECK_TEMPLATE_VALID, False, str(err))
    self.results.AddCheckResults(self.CHECK_TEMPLATE_VALID, True)
    return config_json

  def GetTemplateVariables(self):
    """Return the values that can be used as jinja variables in templates."""
    today = datetime.date.today()
    options = copy.deepcopy(self.config.get('options', {}))
    storage = options.get('storage', {})
    UpdateNestedDict(storage, {'bucket': '', 'prefix': ''})
    storage['url'] = 'gs://%s/%s' % (storage['bucket'], storage['prefix'])

    UpdateNestedDict(options, {
        'app': {
            'id': app_identity.get_application_id(),
            'hostname': app_identity.get_default_version_hostname(),
            'serviceAccountName': app_identity.get_service_account_name(),
            },
        'storage': storage,
        'date': {
            'y-m-d': today.strftime('%Y-%m-%d'),
            'ymd': today.strftime('%Y%m%d'),
            },
        })
    return options

  def SyntaxCheck(self, config_json, phase=None):
    """Ensure the config string is valid JSON and is loadable in to a dict.

    Args:
      config_json: The pipeline configuration JSON rep.
      phase: what part of the syntax checking are we at
    """
    try:
      self.config = json.loads(minify_json.json_minify(config_json))
      self.results.AddCheckResults(self.CHECK_SYNTAX_VALID, True)
    except (ValueError, TypeError) as e:
      phase = phase or 'PreTemplate: '
      self.results.AddCheckResults(
          self.CHECK_SYNTAX_VALID, False, phase + str(e))

  def StageCheck(self):
    """Lints all stages in the config."""
    config = self.config or {}

    has_io_stage = False
    if (('inputs' in config and config['inputs']) or
        ('outputs' in config and config['outputs'])):
      has_io_stage = True
    self.results.AddCheckResults(self.CHECK_REQ_IO_STAGES,
                                 has_io_stage,
                                 self.MSG_MISSING_IO_STAGES)

    unknown_keys = set(config.keys()) - self.VALID_ROOT_KEYS
    self.results.AddCheckResults(
        self.CHECK_UNKNOWN_CONFIG_KEYS,
        not unknown_keys,
        self.MSG_UNKNOWN_CONFIG_KEYS_FMT % ', '.join(sorted(unknown_keys)))

    for s in config.get('inputs', []):
      self.LintStage('inputs', s)
    for s in config.get('transforms', []):
      self.LintStage('transforms', s)
    for s in config.get('outputs', []):
      self.LintStage('outputs', s)

  def LintStage(self, category, config):
    """Lints a single stage config.

    Args:
      category: a stage category (e.g. 'inputs')
      config: a stage config.
    """
    stage_config = copy.deepcopy(config)
    sl = StageLinter(category, stage_config)
    sl.TypeCheck()
    if sl.results.valid:
      stage = pipelines.GetStage(stage_config)
      if hasattr(stage, 'Lint'):
        stage.Lint(sl)
    sl.SourceSinkCheck()
    self.results.AddStageCheckResults(category, sl.results)


class StageLinter(object):
  """Provides helper functions for stages to lint their configurations."""
  CHECK_FIELD_EXISTS_FMT = 'FieldExists [%s]'
  CHECK_FIELD_MIN_LENGTH = 'FieldMinLength'
  CHECK_FIELD_MAX_LENGTH = 'FieldMaxLength'
  CHECK_TYPE_FMT = 'TypeValid [%s]'
  MSG_FIELD_BAD_LENGTH_FMT = '%r has wrong number of items. expected %r got %r'
  MSG_FIELD_INVALID_FMT = 'Invalid value: %r'
  MSG_REQUIRE_AT_LEAST_ONE_FMT = 'At least one of %r must be provided.'
  MSG_REQUIRED_FIELD_FMT = '%r must be provided.'
  MSG_TYPE_NOT_FOUND = 'Type must be provided.'
  MSG_WRONG_TYPE_FMT = 'Type must be a %r.'

  def __init__(self, category, config):
    """Initialize the linter.

    Args:
      category: The stage category (e.g. 'inputs')
      config: The stage configuration.
    """
    self.category = category
    self.config = config
    self.results = LintResults()

  def SourceSinkCheck(self):
    for which in ['sources', 'sinks']:
      if which in self.config:
        self.results.AddCheckResults(self.CHECK_FIELD_EXISTS_FMT % str(which),
                                     self.config[which] is None or
                                     None not in self.config[which],
                                     self.MSG_FIELD_INVALID_FMT % 'null')

  def TypeCheck(self):
    stage = self.config.get('type')
    check = StageLinter.CHECK_TYPE_FMT % str(stage)
    if stage:
      try:
        pipelines.GetStage(self.config)
        self.results.AddCheckResults(check, True)
      except ImportError as e:
        self.results.AddCheckResults(check, False, str(e))
    else:
      self.results.AddCheckResults(check, False, StageLinter.MSG_TYPE_NOT_FOUND)

  def FieldCheck(self, field_name, field_type=None, required=False,
                 validator=None, list_min=None, list_max=None):
    """Performs a linting check on a configuration field.

    Args:
      field_name: the field name
      field_type: the expected type, None to skip
      required: if the field must be provided
      validator: optional validation function taking the field value
      list_min: if typ is list, the minimum number of items required/allowed
      list_max: if typ is list, the maximum number of items required/allowed
    """
    def _GetValue(key, d):
      if not d:
        return None
      k = key.rsplit('.', 1)
      if len(k) == 1:
        return d.get(k[0])
      else:
        return _GetValue(k[1], d.get(k[0]))

    val = _GetValue(field_name, self.config)
    if required:
      self.results.AddCheckResults(self.CHECK_FIELD_EXISTS_FMT % field_name,
                                   val is not None,
                                   self.MSG_REQUIRED_FIELD_FMT % field_name)
    if val:
      if field_type:
        self.results.AddCheckResults(
            self.CHECK_TYPE_FMT % field_name,
            isinstance(val, field_type),
            self.MSG_WRONG_TYPE_FMT % repr(field_type))

      is_list = isinstance(
          field_type, collections.Iterable) and not isinstance(
              field_type, types.StringTypes)
      if is_list and (list_min or list_max):
        l = len(val)
        self.results.AddCheckResults(
            self.CHECK_FIELD_MIN_LENGTH,
            list_min is None or l >= (list_min or 0),
            self.MSG_FIELD_BAD_LENGTH_FMT %
            (field_name, list_min, l))
        self.results.AddCheckResults(
            self.CHECK_FIELD_MAX_LENGTH,
            list_max is None or l <= (list_max or sys.maxint),
            self.MSG_FIELD_BAD_LENGTH_FMT %
            (field_name, list_max, l))

      if validator:
        msg = ''
        valid = False
        try:
          if is_list:
            for v in val:
              validator(v)
          else:
            validator(val)
          valid = True
        except Exception as e:  # pylint: disable=broad-except
          msg = str(e)
        self.results.AddCheckResults(self.CHECK_FIELD_EXISTS_FMT % field_name,
                                     valid,
                                     self.MSG_FIELD_INVALID_FMT % msg)

  def AtLeastOneFieldRequiredCheck(self, fields):
    missing = True
    for f in fields:
      if f in self.config:
        missing = False
        break
    self.results.AddCheckResults(self.CHECK_FIELD_EXISTS_FMT % fields,
                                 not missing,
                                 self.MSG_REQUIRE_AT_LEAST_ONE_FMT % fields)


class LintResults(object):
  """Encapsulates the results on the pipeline configuration linting."""

  def __init__(self):
    self.valid = True
    self.results = {}

  def AddCheckResults(self, name, valid, reason=None):
    c = {'pass': valid}
    if not valid:
      c['reason'] = reason
    self.valid = self.valid and valid
    self.results = UpdateNestedDict(self.results, {name: c})

  def AddStageCheckResults(self, category, check):
    self.valid = self.valid and check.valid
    if 'stages' not in self.results:
      self.results['stages'] = {category: [check.results]}
    elif category not in self.results['stages']:
      self.results['stages'][category] = [check.results]
    else:
      self.results['stages'][category].append(check.results)


def UpdateNestedDict(master, to_add):
  """Merges the values from to_add into the master dict."""
  for key, value in to_add.items():
    if key not in master:
      master[key] = value
    elif isinstance(value, dict):
      UpdateNestedDict(master[key], value)
  return master
