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

"""Handler for pipeline stage help."""


import markdown

from src.handlers import basehandler
from src.pipelines import pipelines


class HelpHandler(basehandler.RequestHandler):
  """Get the documentation for stages."""

  def get(self):
    """output stage documentation."""

    # strip off the /data/help/ part of the path
    section = self.request.path.split('/', 3)[-1]

    if section in ['install', 'overview', 'usage', 'examples', 'cloudhistory',
                   'changelog']:
      self.SendSection(section)
    elif section.startswith('stage'):
      self.SendJson([{
          'name': stage_name,
          'help': MarkdownToHtml(pipelines.GetStageHelp(stage_name)),
          } for stage_name in pipelines.ListStages()])
    else:
      self.NotFound('could not find help section: %r', section)

  def SendSection(self, section):
    with self.OpenResource('help/%s.md' % section) as resource:
      self.Respond(MarkdownToHtml(resource.read()))


def MarkdownToHtml(markdown_string):
  html = markdown.markdown(
      markdown_string,
      output_format='html',
      extensions=['codehilite', 'fenced_code', 'footnotes', 'tables'])
  # Since angular intercepts all links we fix it like this.
  html = html.replace('<a ', '<a target="_blank" ')
  return html
