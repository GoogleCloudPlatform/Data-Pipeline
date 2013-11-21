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
 * @fileoverview Primary module for the Data Pipeline app.
 */

/**
 * Whitelist urls starting with blob for CSV generation.
 *
 * @param {angular.$compileProvider} $compileProvider Angular compiler provider.
 * @ngInject
 */
datapipeline.application.whitelistProtocols = function($compileProvider) {
  $compileProvider.urlSanitizationWhitelist(/^\s*(https?|ftp|mailto|blob):/);
};

/**
 * The main module for the datapipeline app.
 */
datapipeline.application.module = angular.module('datapipeline',
    [
      'datapipeline.directives.download',
      'datapipeline.directives.dropUpload',
      'datapipeline.directives.layout',
      'datapipeline.directives.upload',
      'datapipeline.services.AppConfig',
      'datapipeline.services.Pipeline',
      'datapipeline.services.User'
    ]);

datapipeline.application.module.config(
    datapipeline.application.whitelistProtocols);

// register all controllers
var controllers = {
  AppConfigCtrl: datapipeline.controllers.AppConfigCtrl,
  AppCtrl: datapipeline.controllers.AppCtrl,
  PipelineCtrl: datapipeline.controllers.PipelineCtrl,
  PipelineListCtrl: datapipeline.controllers.PipelineListCtrl
};

for (var key in controllers) {
  datapipeline.application.module.controller(key, controllers[key]);
}

// now set up the little help applicaton

/**
 * The module/application for the help pages.
 */
datapipeline.help.module = angular.module('help',
    [
      'datapipeline.services.Help'
    ]);

/** The route provider switches in templates to ng-view depending on path.
 * @param {angular.$routeProvider} $routeProvider The Angular route provider
 *     service.
 * @param {angular.$locationProvider} $locationProvider The Angular location
 *     provider service.
 */
datapipeline.help.routeProvider = function($routeProvider, $locationProvider) {
  $locationProvider.html5Mode(true);
  $routeProvider.
    when('/help/overview', {templateUrl: '/data/help/overview'}).
    when('/help/installation', {templateUrl: '/data/help/install'}).
    when('/help/usage', {templateUrl: '/data/help/usage'}).
    when('/help/stages',
         {templateUrl: '/static/html/help/stages.ng',
          reloadOnSearch: false}).
    when('/help/examples', {templateUrl: '/data/help/examples'}).
    otherwise({redirectTo: '/help/overview'});
};

datapipeline.help.module.config(datapipeline.help.routeProvider);

datapipeline.help.module.controller(
    'HelpCtrl', datapipeline.controllers.HelpCtrl);
