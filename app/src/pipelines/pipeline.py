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

"""Base class for all datapipeline pipelines."""

import datetime
import logging

from mapreduce.lib.pipeline import pipeline
from google.appengine.ext import db

_STATUS_WRITE_FREQUENCY = datetime.timedelta(minutes=1)


class Pipeline(pipeline.Pipeline):
  """Base class for all datapipeline pipelines."""

  def __init__(self, *args, **kwargs):
    super(Pipeline, self).__init__(*args, **kwargs)
    self._status_record = None
  def set_status(self, message=None, console_url=None, status_links=None,
                 important=True):
    """Sets the current status of this pipeline if not done recently.

    This method is purposefully non-transactional. Updates are written to the
    datastore immediately and overwrite all existing statuses.

    This is a modified version of set_status in
    third_party/py/appengine_pipeline/src/pipeline/pipeline.py
    It only writes the status to datastore if no update has happened
    within 1 minute or the status update is marked as important with
    the impartant keyword arg.

    Args:
      message: (optional) Overall status message.
      console_url: (optional) Relative URL to use for the "console" of this
        pipeline that displays current progress. When None, no console will
        be displayed.
      status_links: (optional) Dictionary of readable link names to relative
        URLs that should be associated with this pipeline as it runs.
        These links provide convenient access to other dashboards, consoles,
        etc associated with the pipeline.
      important: Is this update important enough to force a datastore put().

    Raises:
      PipelineRuntimeError if the status could not be set for any reason.
    """
    # pylint: disable=protected-access
    if pipeline._TEST_MODE:
      logging.info(
          'New status for %s#%s: message=%r, console_url=%r, status_links=%r',
          self, self.pipeline_id, message, console_url, status_links)
      return

    if not self._status_record:
      important = True  # force a put()
      status_key = db.Key.from_path(pipeline._StatusRecord.kind(),
                                    self.pipeline_id)
      root_pipeline_key = db.Key.from_path(
          pipeline._PipelineRecord.kind(), self.root_pipeline_id)
      self._status_record = pipeline._StatusRecord(
          key=status_key, root_pipeline=root_pipeline_key)

    try:
      self._status_record.message = message or ''
      self._status_record.console_url = console_url or ''
      if status_links:
        # Alphabeticalize the list.
        self._status_record.link_names = sorted(
            db.Text(s) for s in status_links.iterkeys())
        self._status_record.link_urls = [
            db.Text(status_links[name])
            for name in self._status_record.link_names]
      else:
        self._status_record.link_names = []
        self._status_record.link_urls = []

      if (important or
          ((datetime.datetime.now() - self._status_record.status_time) >
           _STATUS_WRITE_FREQUENCY)):
        self._status_record.status_time = datetime.datetime.utcnow()
        self._status_record.put()
    except Exception, e:
      raise pipeline.PipelineRuntimeError(
          'Could not set status for %s#%s: %s' %
          (self, self.pipeline_id, str(e)))


After = pipeline.After
InOrder = pipeline.InOrder
