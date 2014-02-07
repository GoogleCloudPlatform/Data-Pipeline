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

from src.handlers import basehandler
from src.model import appconfig
from src.pipelines import linter


class LintHandler(basehandler.RequestHandler):
  """Lints a pipeline configuration."""

  def post(self):
    """Find and lint a pipeline."""
    p = json.loads(self.request.body)

    if not p or 'config' not in p:
      self.NotFound('Unable to find pipeline config in json request.')
    else:
      lint = linter.PipelineLinter(
          p['config'],
          appconfig.AppConfig.GetAppConfig().AsOptionsDict())
      p['lint'] = lint.results.results
      self.SendJson(p)
