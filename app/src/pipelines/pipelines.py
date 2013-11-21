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

"""Data pipelines."""

import importlib
import pkgutil
import sys


from mapreduce.lib.pipeline import common
from mapreduce.lib.pipeline import pipeline


def GetStage(stage_defn):
  """Get the pipe for a given config dict with optional input."""
  return GetStageClass(stage_defn.get('type'))(stage_defn)


def GetStageClass(type_name):
  """Get the class for a pipe for a given name."""
  type_mod = importlib.import_module(
      ''
      'src.pipelines.stages.' + type_name.lower())
  return getattr(type_mod, type_name)


def ListStages():
  """Get a list of all the pipeline stage names available."""
  stages_mod = importlib.import_module(
      ''
      'src.pipelines.stages')
  prefix = stages_mod.__name__ + '.'
  ans = []
  for unused_importer, module_name, is_package in pkgutil.iter_modules(
      stages_mod.__path__, prefix):
    if not is_package:
      if not module_name.endswith('_test'):
        lowercase_stage_name = module_name.rsplit('.', 1)[1]
        stage_module = importlib.import_module(module_name)
        for name in dir(stage_module):
          if name.lower() == lowercase_stage_name:
            ans.append(name)
            break
  return ans


def GetStageHelp(type_name):
  """Get the help string for a particular stage.

  First try and call static GetHelp method on class.
  Otherwise return the class' docstring
  Otherwise return the module's docstring.

  Args:
    type_name: The name of a stage (e.g. GcsInput).
  Returns:
    A doc string in markdown format.
  """
  stage_class = GetStageClass(type_name)
  if hasattr(stage_class, 'GetHelp'):
    return stage_class.GetHelp()
  if stage_class.__doc__:
    return stage_class.__doc__
  if sys.modules[stage_class.__module__].__doc__:
    return sys.modules[stage_class.__module__].__doc__
  return ''


class PipelineError(Exception):

  def __init__(self, msg, stage=None):
    if stage:
      msg = ''.join(['Stage', stage, ': ', msg])
    Exception.__init__(self, msg)


class PushPipeline(pipeline.Pipeline):
  """A pipeline that evaluates stages sequentially first-to-last."""

  def run(self, defn):
    """Executes the pipeline.

    Args:
      defn: The definition of the pipeline. Input stages may be run in parallel.
        Transforms will always be run serially. Outputs may be run in parallel.

    Yields:
      A PipelineFuture from which the results of the output stages can be read.
    """

    # this pipeline runs all input stages in parallel, fans them back in then
    # runs the transforms tages in-order, finally fanning out the output stages
    # in parallel.
    inputs = yield FanOut(defn.get('inputs', []))
    with pipeline.After(inputs):
      last_tx = None
      with pipeline.InOrder():
        for d in defn.get('transforms', []):
          last_tx = yield GetStage(d)
      if not last_tx:
        last_tx = yield common.Ignore()
      with pipeline.After(last_tx):
        yield FanOut(defn.get('outputs', []))


class FanOut(pipeline.Pipeline):
  """Helper stage to parallelize other stages."""

  def run(self, defn):
    all_stages = []
    for d in defn:
      s = yield GetStage(d)
      all_stages.append(s)
    yield common.Append(*all_stages)
