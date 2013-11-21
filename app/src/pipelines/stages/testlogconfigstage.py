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

"""Test stages."""


from mapreduce.lib.pipeline import common
from mapreduce.lib.pipeline import pipeline


class TestLogConfigStage(pipeline.Pipeline):
  """Just logs the incoming config data."""

  def run(self, config):
    yield common.Log.info('Running TestLogConfigStage with config: %r', config)
