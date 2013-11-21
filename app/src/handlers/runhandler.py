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

"""Pipeline handler."""

import logging
import urlparse

from google.appengine.api import modules
from google.appengine.api import taskqueue
from google.appengine.api import users

from src.clients import gcs
from src.handlers import basehandler
from src.model import appconfig
from src.model import pipeline
from src.pipelines import linter
from src.pipelines import runner

BACKEND_URL = '/backend/run'


class RunHandler(basehandler.RequestHandler):
  """A super simple handler that prints out ok to show it's working."""

  def get(self, path):
    return self.post(path)

  def post(self, path):
    """Find and Start the pipeline running."""
    (name, api_key) = path.split('/', 1)
    p = pipeline.Pipeline.query().filter(
        pipeline.Pipeline.name == name,
        pipeline.Pipeline.api_key == api_key).get()

    if not p:
      self.NotFound('Unable to find pipeline [%s] with api_key [%r]',
                    name, api_key)
      return

    if modules.get_current_module_name() != 'backend':
      if users.is_current_user_admin():
        self.redirect(self.GetModuleUrl('backend'))
      else:
        # Get the url without the hostname (Taskqueue likes it like that).
        url = urlparse.urljoin('http://example.com/', self.request.url)
        url = '/' + url.split('/', 3)[-1]
        taskqueue.add(queue_name='backend', url=url, params={})
      return

    self.RunPipeline(p)

  def RunPipeline(self, p):
    """Run the pipeline."""
    logging.info('Linting pipeline: %s', p.name)
    pl = linter.PipelineLinter(
        p.config,
        appconfig.AppConfig.GetAppConfig().AsOptionsDict())
    results = pl.results
    if not results.valid:
      self.BadRequest('Linting for pipeline [%s] FAILED.\n%s',
                      p.name, pl.results)
      return

    logging.info('Running pipeline: %s', p.name)
    config = pl.config

    storage = config.get('options', {}).get(appconfig.OPTIONS_STORAGE_KEY, {})
    bucket = storage[appconfig.OPTIONS_STORAGE_BUCKET_KEY]
    prefix = storage.get(appconfig.OPTIONS_STORAGE_PREFIX_KEY, '')

    pr = runner.PipelineRunner()
    pipe = pr.Build(config, gcs.Gcs.UrlCreator(bucket, prefix))
    pipe.max_attempts = 1
    pipe.start()
    p.running_pipeline_ids.append(pipe.pipeline_id)
    p.put()
    self.redirect('/_ah/pipeline/status?root=%s' % pipe.pipeline_id)
