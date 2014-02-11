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


class RunHandler(basehandler.RequestHandler):
  """Runs a pipeline."""

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
        parsed = urlparse.urlparse(self.request.url)
        taskqueue.add(queue_name='backend', url=parsed.path,
                      params=urlparse.parse_qs(parsed.query))
      return

    self.RunPipeline(p)

  def RunPipeline(self, p):
    """Run the pipeline."""
    logging.info('Linting pipeline: %s', p.name)
    options_dict = appconfig.AppConfig.GetAppConfig().AsOptionsDict()
    self.expandOptionsDict(options_dict, self.GetAllArguments())
    logging.info('options_dict is:\n%r', options_dict)
    logging.info('input config is:\n%s', p.config)
    pl = linter.PipelineLinter(p.config, options_dict)
    if not pl.results.valid:
      self.BadRequest('Linting for pipeline [%s] FAILED.\n%r',
                      p.name, pl.results.results)
      return

    logging.info('Running pipeline: %s with config\n%s', p.name, pl.config)
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
    # show the status page using the default frontend module
    url = urlparse.urljoin(self.GetModuleUrl('default'),
                           '/_ah/pipeline/status?root=%s' % pipe.pipeline_id)
    self.redirect(str(url))

  def GetAllArguments(self):
    """Get all the arguments from the request and put them into a dict."""
    arguments = self.request.arguments()
    return dict(zip(arguments, [self.request.get(x) for x in arguments]))

  @staticmethod
  def expandOptionsDict(options, arguments):
    """Add values to an options dict from the url paramters in arguments.

    This method will create dictionaries if arguments have periods in them

    so storage.api and storage.user will make an object called storage
    with two keys of api and user.

    Args:
      options: a current set of options to update (json dict structure)

      arguments: a flat dict of key/value pairs. key's might have
          periods to indicate structure to be created.
    """
    for key, value in arguments.items():
      key_parts = key.split('.')
      obj = options
      try:
        for idx in range(len(key_parts) - 1):
          obj = options.setdefault(key_parts[idx], {})
        obj[key_parts[-1]] = value
      except TypeError as err:
        logging.error('unable to overwrite key %r with value %r because %r '
                      'is not a dict (%r)', key, value, obj, err)
