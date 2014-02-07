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

"""Server for app."""



from mapreduce.lib.pipeline import pipeline as appengine_pipeline
import webapp2

from lib.crud import crud_handler
from src.handlers import helphandler
from src.handlers import linthandler
from src.handlers import runhandler
from src.handlers import variablehandler
from src.model import appconfig
from src.model import pipeline
from src.model import runstat
from src.model import user


class OkHandler(webapp2.RequestHandler):
  """A super simple handler that prints out ok to show it's working."""

  def get(self):
    self.response.out.write('ok')


main = webapp2.WSGIApplication([
    ('/data/ok', OkHandler),
    ('/data/pipeline.*', crud_handler.GetCrudHandler(pipeline.Pipeline)),
    ('/data/runstat.*', crud_handler.GetCrudHandler(runstat.RunStat)),
    ('/data/user.*', crud_handler.GetCrudHandler(user.User)),
    ('/data/appconfig.*', crud_handler.GetCrudHandler(appconfig.AppConfig)),
    ('/data/help.*', helphandler.HelpHandler),
    ('/data/variables.*', variablehandler.VariableHandler),
    ])

runner = webapp2.WSGIApplication([
    ('/_ah/start', OkHandler),
    ('/run/(.*)', runhandler.RunHandler),
    ('/action/lint.*', linthandler.LintHandler),
    ] + appengine_pipeline.create_handlers_map(), debug=True)
