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
 *
 */

/**
 * Help controller.
 *
 * @param {angular.Location} $location
 * @param {angular.Scope} $scope
 * @param {datapipeline.services.Help} Help
 * @constructor
 * @ngInject
 */
datapipeline.controllers.HelpCtrl = function($location, $scope, Help) {
  this.location = $location;
  this.scope = $scope;
  this.Help = Help;

  this.scope.helpSearch = $location.search().stage;

  this.scope.stageHelps = Help.query();
  this.scope.$watch('helpSearch', angular.bind(this, function(newValue) {
    if (newValue === '') {
      this.location.search('stage', null);
    } else {
      this.location.search('stage', newValue);
    }
  }));
};
