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

"""Base Handler class."""

import json
import logging
import os
import urlparse

import webapp2

from google.appengine.api import modules

from lib.crud import crud_handler


class RequestHandler(webapp2.RequestHandler):
  """Base Handler class with useful utility methods."""

  def Respond(self, msg, status=200, *args):
    logging.info(msg, *args)
    self.response.write(msg % args)
    self.response.status = status

  def BadRequest(self, msg, *args):
    self.Respond(msg, 400, *args)

  def NotFound(self, msg, *args):
    self.Respond(msg, 404, *args)

  def Error(self, msg, *args):
    self.Responsd(msg, 500, *args)

  def SendJson(self, json_dict, pretty_print=False, include_prefix=True):
    """Send a json dict as a response."""
    if include_prefix:
      self.response.write(crud_handler.JSON_PREFIX)
    if pretty_print:
      self.response.write(
          json.dumps(json_dict, indent=2, separators=(',', ': '),
                     default=crud_handler.JsonPrinter))
    else:
      self.response.write(
          json.dumps(json_dict, default=crud_handler.JsonPrinter))

  def GetModuleUrl(self, module=None, url=None):
    """Get the url for the current page but on the given module."""
    url_parts = list(urlparse.urlsplit(url or self.request.url))
    url_parts[1] = modules.get_hostname(module=module)
    # If we're on https we need to replace version.backend.datapipeline
    # with version-dot-backend-dot-datapipeline
    if url_parts[0] == 'https':
      hostname_parts = url_parts[1].rsplit('.', 2)
      hostname_parts[0] = hostname_parts[0].replace('.', '-dot-')
      url_parts[1] = '.'.join(hostname_parts)
    return str(urlparse.urlunsplit(url_parts))

  def OpenResource(self, path):
    """Open up a file that is included with this app engine deployment.

    NOTE: you cannot open a file that has been pushed as a static file.

    Args:
      path: the path relative to the app/ directory to open.
    Returns:
      a file object to the opened file (don't forget to use 'with').
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    file_name = os.path.join(base_dir, path)
    return open(file_name)
