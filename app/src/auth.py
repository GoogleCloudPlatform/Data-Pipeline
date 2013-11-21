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

"""API authentication utility."""

from apiclient import discovery
import httplib2

from oauth2client import appengine as oauth_appengine

from google.appengine.api import memcache


class Service(object):

  @staticmethod
  def HttpFromServiceAccount(scope):
    """Prepare HTTP authenticated by the application's service account.

    Args:
      scope: Desired service API scope.
    Returns:
      Authenticated HTTP object.
    """
    credentials = oauth_appengine.AppAssertionCredentials(scope)
    return credentials.authorize(http=httplib2.Http(memcache))

  @staticmethod
  def FromServiceAccount(name, version, scope, developer_key=None):
    """Authenticates using the application's service account.

    Args:
      name: The service name.
      version: The service API version.
      scope: Desired service API scope.
      developer_key: The simple API key (optional).

    Returns:
      The authorized service.
    """
    service = discovery.build(name, version,
                              http=Service.HttpFromServiceAccount(scope),
                              developerKey=developer_key)
    return service
