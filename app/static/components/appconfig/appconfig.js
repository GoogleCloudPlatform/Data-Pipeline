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
 * @fileoverview Do we want to display the AppConfig panel
 */
goog.provide('datapipeline.components.appconfig.AppConfig');
goog.provide('datapipeline.components.appconfig.AppConfigCtrl');
goog.provide('datapipeline.components.appconfig.AppConfigData');

goog.require('datapipeline.components.butterbar.ButterBar');

goog.scope(function() {

/**
 * An ngResource service for get/set the AppConfigData on the server.
 */
datapipeline.components.appconfig.AppConfigData.module = angular.module(
  'datapipeline.components.appconfig.AppConfigData',
               ['ngResource']).
  factory('AppConfigData', function($resource) {
    return $resource('/data/appconfig/?id=:id', {id: '@id'},
                     {query: {method: 'GET', isArray: false}});
  });

/**
 * A service for showing the App Config setting dialog.
 *
 * @param {datapipeline.components.appconfig.AppConfigData} AppConfigData
 * @param {datapipeline.components.butterbar.ButterBar} ButterBar
 * @constructor
 * @ngInject
 */
datapipeline.components.appconfig.AppConfig = function(AppConfigData,
                                                       ButterBar) {
  this.AppConfigData = AppConfigData;
  this.ButterBar = ButterBar;
  this.showAppConfig = false;  // is the dialog shown.
  this.load();
};
var AppConfig = datapipeline.components.appconfig.AppConfig;

/** Load the application config.
 * @param {String} opt_message
 */
AppConfig.prototype.load = function(opt_message) {
  if (!opt_message) {
    opt_message = 'Loading Application Configuration';
  }
  var workId = this.ButterBar.startWork(opt_message);
  this.AppConfigData.query(angular.bind(this, function(reply) {
    this.data = reply;
    this.ButterBar.finishWork(workId);
  }));
};

/** Save the application config. */
AppConfig.prototype.save = function() {
  var workId = this.ButterBar.startWork('Saving Application Configuration');
  this.data.$save(angular.bind(this, function() {
    this.ButterBar.finishWork(workId);
  }));
};

/** revert the application config. */
AppConfig.prototype.revert = function() {
  this.load('Restoring Application Configuration');
};

/**
 * Angular module.
 *
 * @type {angular.Module}
 */
datapipeline.components.appconfig.AppConfig.module = angular.module(
    'datapipeline.components.appconfig.AppConfig', [
      datapipeline.components.appconfig.AppConfigData.module.name,
      datapipeline.components.butterbar.ButterBar.module.name
    ]).
    service('AppConfig', AppConfig);

/**
 * AppConfig controller.
 *
 * @param {datapipeline.components.appconfig.AppConfig} AppConfig
 * @param {datapipeline.components.butterbar.ButterBar} ButterBar
 * @constructor
 * @ngInject
 */
datapipeline.components.appconfig.AppConfigCtrl = function(AppConfig,
                                                           ButterBar) {
  this.AppConfig = AppConfig;
  this.ButterBar = ButterBar;

  this.needsSave = false;
};
var AppConfigCtrl = datapipeline.components.appconfig.AppConfigCtrl;

/** Called when we change a value in the dialog.
 */
AppConfigCtrl.prototype.changed = function() {
  this.needsSave = true;
};

/** Called to close the App Config Dialog.
 * @param {boolean} s shall we save the config or restore the values?
 */
AppConfigCtrl.prototype.save = function(s) {
  console.log('saveAppConfig');
  this.AppConfig.showAppConfig = false;
  if (this.needsSave) {
    this.needsSave = false;
    if (s) {
      this.AppConfig.save();
    } else {
      this.AppConfig.revert();
    }
  }
};
}); // goog.scope
