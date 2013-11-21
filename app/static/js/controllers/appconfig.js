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
 * @fileoverview Controller for the AppConfig dialog.
 *
 */

/**
 * AppConfig controller.
 *
 * @param {angular.Scope} $scope
 * @param {datapipeline.services.AppConfig} AppConfig
 * @constructor
 * @ngInject
 */
datapipeline.controllers.AppConfigCtrl = function($scope, AppConfig) {
  this.scope = $scope;
  this.AppConfig = AppConfig;

  this.scope.appConfigStatus = '';

  this.scope.saveAppConfig = angular.bind(this, this.saveAppConfig);
  this.scope.changed = angular.bind(this, this.changed);
  this.scope.needsSave = false;
};

/** Called when we change a value.
 */
datapipeline.controllers.AppConfigCtrl.prototype.changed = function() {
  this.scope.needsSave = true;
};

/** Called to close the App Config Dialog.
 * @param {boolean} s shall we save the config or restore the values?
 */
datapipeline.controllers.AppConfigCtrl.prototype.saveAppConfig = function(s) {
  if (this.scope.needsSave) {
    this.scope.needsSave = false;
    this.scope.$parent.showAppConfig = false;
    if (s) {
      var workId = this.scope.startWork('Saving Application Configuration');
      this.scope.$parent.appConfig.$save(angular.bind(this, function() {
        this.scope.finishWork(workId);
      }));
    } else {
      var workId = this.scope.startWork('Restoring Application Configuration');
      this.scope.$parent.appConfig = this.AppConfig.query(angular.bind(
        this, function() {
          this.scope.finishWork(workId);
        }));
    }
  } else {
    // nothing to save.
    this.scope.$parent.showAppConfig = false;
  }
};
