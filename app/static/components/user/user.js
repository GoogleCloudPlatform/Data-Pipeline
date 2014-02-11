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
goog.provide('datapipeline.components.user.User');
goog.provide('datapipeline.components.user.UserData');

goog.require('datapipeline.components.butterbar.ButterBar');

goog.scope(function() {

/**
 * A service to CRUD User data from the server.
 */
datapipeline.components.user.UserData.module = angular.module(
  'datapipeline.components.user.UserData', ['ngResource']).
  factory('UserData', function($resource) {
    return $resource('/data/user/?id=:id', {id: '@id'},
                     {query: {method: 'GET', isArray: false}});
  });


/**
 * A service with information about all the pipelines.
 *
 * @param {datapipeline.components.butterbar.ButterBar} ButterBar service.
 * @param {datapipeline.components.user.UserData} UserData service.
 * @constructor
 * @ngInject
 */
datapipeline.components.user.User = function(ButterBar, UserData) {

  this.userLoaded = false;
  this.callbacks = [];
  this.data = undefined;

  var workId = ButterBar.startWork('Loading User Data');
  UserData.query(angular.bind(this, function(user) {
    console.log('Loaded USER Data! -----------------------' + user.email);
    ButterBar.finishWork(workId);
    this.data = user;
    this.userLoaded = true;
    for (var i = 0; i < this.callbacks.length; i++) {
      this.callbacks[i]();
    }
    this.callbacks = [];  // Clear out the callbacks
  }));
};
var User = datapipeline.components.user.User;

/** Add a callback to be called when the user is loaded.
 * @param {function} f the callback function to call.
 * */
User.prototype.addCallback = function(f) {
  if (this.userLoaded) {
    f(this.data);
  } else {
    this.callbacks.push(f);
  }
};

/**
 * Angular module.
 *
 * @type {!angular.Module}
 */
datapipeline.components.user.User.module = angular.module(
    'datapipeline.components.user.User', [
      datapipeline.components.butterbar.ButterBar.module.name,
      datapipeline.components.user.UserData.module.name
    ]).
    service('User', User);
}); // goog.scope
