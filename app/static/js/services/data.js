// Copyright 2013 Google Inc. All Rights Reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/**
 * @fileoverview Add the Data object served by ngResource.
 */

angular.module('datapipeline.services.AppConfig', ['ngResource']).
  factory('AppConfig', function($resource) {
    return $resource('/data/appconfig/?id=:id', {id: '@id'},
                     {query: {method: 'GET', isArray: false}});
  });

angular.module('datapipeline.services.Pipeline', ['ngResource']).
  factory('Pipeline', function($resource) {
    return $resource('/data/pipeline/?id=:id&parent_id=:parent_id',
                     {id: '@id', parent_id: '@parent_id'}, {});
  });

angular.module('datapipeline.services.User', ['ngResource']).
  factory('User', function($resource) {
    return $resource('/data/user/?id=:id', {id: '@id'},
                     {query: {method: 'GET', isArray: false}});
  });

angular.module('datapipeline.services.Help', ['ngResource']).
  factory('Help', function($resource) {
    return $resource('/data/help/:section', {section: 'stages'},
                     {query: {method: 'GET', isArray: true}});
  });
