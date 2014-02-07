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
 * @fileoverview Controller for the Help page.
 */
goog.provide('datapipeline.components.help');
goog.provide('datapipeline.components.help.HelpCtrl');
goog.provide('datapipeline.components.help.HelpData');

goog.scope(function() {

angular.module('datapipeline.components.help.HelpData', ['ngResource']).
  factory('HelpData', function($resource) {
    return $resource('/data/help/:section', {section: 'stages'},
                     {query: {method: 'GET', isArray: true}});
  });


/**
 * Help controller.
 *
 * @param {angular.Location} $location
 * @param {angular.Scope} $scope
 * @param {datapipeline.components.help.HelpData} HelpData
 * @constructor
 * @ngInject
 */
datapipeline.components.help.HelpCtrl = function($location, $scope, HelpData) {
  this.location = $location;
  this.scope = $scope;

  this.scope.helpSearch = $location.search().stage;

  this.scope.stageHelps = HelpData.query();
  this.scope.$watch('helpSearch', angular.bind(this, function(newValue) {
    if (newValue === '') {
      this.location.search('stage', null);
    } else {
      this.location.search('stage', newValue);
    }
  }));
};


// now set up the little help applicaton

/**
 * The module/application for the help pages.
 */
datapipeline.components.help.module = angular.module('help',
    [
      'ngRoute',
      'ngSanitize',
      'datapipeline.components.help.HelpData'
    ]);

/** The route provider switches in templates to ng-view depending on path.
 * @param {angular.$routeProvider} $routeProvider The Angular route provider
 *     service.
 * @param {angular.$locationProvider} $locationProvider The Angular location
 *     provider service.
 */
datapipeline.components.help.routeProvider = function($routeProvider,
                                                      $locationProvider) {
  $locationProvider.html5Mode(true);
  $routeProvider.
    when('/help/overview', {templateUrl: '/data/help/overview'}).
    when('/help/installation', {templateUrl: '/data/help/install'}).
    when('/help/usage', {templateUrl: '/data/help/usage'}).
    when('/help/stages',
         {templateUrl: '/static/components/help/stages.ng',
          reloadOnSearch: false}).
    when('/help/examples', {templateUrl: '/data/help/examples'}).
    when('/help/cloudhistory', {templateUrl: '/data/help/cloudhistory'}).
    when('/help/changelog', {templateUrl: '/data/help/changelog'}).
    otherwise({redirectTo: '/help/overview'});
};

datapipeline.components.help.module.config(
  datapipeline.components.help.routeProvider);

datapipeline.components.help.module.controller(
    'HelpCtrl', datapipeline.components.help.HelpCtrl);

}); // goog.scope
