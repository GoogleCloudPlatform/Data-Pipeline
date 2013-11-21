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
 * @fileoverview Root controller of the Data Pipeline app.
 */

'use strict';

/**
 * Data Pipeline controller.
 *
 * @param {angular.Http} $http
 * @param {angular.Location} $location
 * @param {angular.Route} $route
 * @param {angular.Scope} $scope
 * @param {angular.Scope} $timeout
 * @param {datapipeline.services.AppConfig} AppConfig
 * @param {datapipeline.services.Pipeline} Pipeline
 * @param {datapipeline.services.User} User
 * @constructor
 * @ngInject
 */
datapipeline.controllers.AppCtrl = function($http, $location, $route, $scope,
                                            $timeout,
                                            AppConfig, Pipeline, User) {
  this.http = $http;
  this.location = $location;
  this.route = $route;
  this.scope = $scope;
  this.timeout = $timeout;

  this.AppConfig = AppConfig;
  this.Pipeline = Pipeline;
  this.User = User;

  // An array of work being done. Each item is an object with an
  // workId and a description.
  this.workId = 0;  // the last workId used.

  this.getPipelineIdFromPath = angular.bind(this, this.getPipelineIdFromPath);
  this.loadPipelines = angular.bind(this, this.loadPipelines);
  this.routeChange = angular.bind(this, this.routeChange);
  this.userChanged = angular.bind(this, this.userChanged);

  this.scope.createPipeline = angular.bind(this, this.createPipeline);
  this.scope.finishWork = angular.bind(this, this.finishWork);
  this.scope.goTo = angular.bind(this, this.goTo);
  this.scope.message = angular.bind(this, this.message);
  this.scope.startWork = angular.bind(this, this.startWork);
  this.scope.ternary = angular.bind(this, this.ternary);

  this.scope.$watch('user.id', this.userChanged);
  this.scope.$on('$routeChangeStart', this.routeChange);

  this.scope.workBeingDone = [];
  this.scope.showAppConfig = false;

  var userWorkId = this.startWork('Loading User Data');
  this.scope.user = User.query(angular.bind(this, function() {
    this.finishWork(userWorkId);
  }));
  var pipelinesWorkId = this.startWork('Loading Pipelines');
  this.scope.pipelines = Pipeline.query(angular.bind(this, function() {
    this.finishWork(pipelinesWorkId);
  }));
  var appConfigWorkId = this.startWork('Loading Application Configuration');
  this.scope.appConfig = AppConfig.query(angular.bind(this, function() {
    this.finishWork(appConfigWorkId);
  }));
};

/** Create a new pipeline.
 */
datapipeline.controllers.AppCtrl.prototype.createPipeline = function() {
  console.log('Creating a pipeline');
  // TODO(user) check if current pipeline is modified or not, pop up warning

  var workId = this.scope.startWork('Creating new pipeline');
  this.scope.pipeline = this.Pipeline.get({
    id: datapipeline.constants.NEW_ENTITY_ID,
    parent_id: this.scope.user.id}, angular.bind(this, function() {
      this.scope.finishWork(workId);
    }));
};

/** Finish the annoucement that work is being done.
 * @param {int} workId the if we wish to cancel.
 */
datapipeline.controllers.AppCtrl.prototype.finishWork = function(workId) {
  // go through the work objects and remove this id
  // if this id is the last one, update the UI.
  for (var i = 0; i < this.scope.workBeingDone.length; i++) {
    if (this.scope.workBeingDone[i].workId === workId) {
      console.log('finishWork (' + workId + ') ' +
                  this.scope.workBeingDone[i].message + ' (@' + i + ')');
      this.scope.workBeingDone.splice(i, 1);
      return;
    }
  }
  console.log('Could not find work to remove with workId: ' + workId);
};

/** Look at the path of the current page and work out the pipeline_id
 * @return {string} the pipeline_id (or datapipeline.constants.NEW_ENTITY_ID).
 */
datapipeline.controllers.AppCtrl.prototype.getPipelineIdFromPath = function() {
  var path = this.location.path().split('/');
  if (path.length > 1) {
    return path[1];
  }
  return datapipeline.constants.NEW_ENTITY_ID;
};

/** Go to a particular path.
 * @param {string} path
 */
datapipeline.controllers.AppCtrl.prototype.goTo = function(path) {
  this.location.path(path);
};

/** Initialize the Controller by loading all the Pipelines a user can see. */
datapipeline.controllers.AppCtrl.prototype.loadPipelines = function() {
  console.log('AppCtrl:loadPipelines');
  if (!this.scope.user) {
    return;
  }
  console.log('user loaded');
  this.scope.pipelines = this.Pipeline.query(
    {parent_id: this.scope.user.id},
    angular.bind(this, function() {
      console.log('pipelines loaded: ' + this.scope.pipelines.length);
      // TODO(user) if there is a pipelineId in the path select that
    }));
};

/** Display a message for a short time at the top of the UI.
 * @param {string} message to display.
 * @param {!int} timeout in microseconds
 */
datapipeline.controllers.AppCtrl.prototype.message = function(message,
                                                              timeout) {
  // We'll reuse the start/finish work system since that works well.
  var workId = this.startWork(message);
  this.timeout(angular.bind(this, function() {
    this.finishWork(workId);
  }), timeout || 3000);
};

/** Called when the route changes.
 * @param {angular.Route} next
 * @param {angular.Route} old
 */
datapipeline.controllers.AppCtrl.prototype.routeChange = function(next, old) {
  // TODO(user) notice when we have a pipeline id in the route and load it.
};

/** let the user know we are doing some work.
 * @param {string} message to display.
 * @return {int} the workId of this message (for cancelling later).
 */
datapipeline.controllers.AppCtrl.prototype.startWork = function(message) {
  this.workId += 1;
  console.log('startWork  (' + this.workId + ') ' + message);
  this.scope.workBeingDone.push({workId: this.workId, message: message});
  return this.workId;
};

/** Choose between two values based on a boolean value.
 * @param {boolean} condition the boolean value to check.
 * @param {string} ifTrue value to return if condition is true.
 * @param {string} ifFalse value to return if condition is false.
 * @return {string} either the ifTrueor or ifFalse value.
 */
datapipeline.controllers.AppCtrl.prototype.ternary = function(
  condition, ifTrue, ifFalse) {
  return condition ? ifTrue : ifFalse;
};

/** Called when the user object updates in the scope.
 * @param {datapipeline.services.User} newval
 * @param {datapipeline.services.User} oldval
 */
datapipeline.controllers.AppCtrl.prototype.userChanged = function(newval,
                                                                  oldval) {
  console.log('userChanged ' + newval + ' / ' + oldval);
  if (newval) {
    this.loadPipelines();
  }
};
