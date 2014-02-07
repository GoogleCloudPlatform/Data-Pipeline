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
goog.provide('datapipeline.components.app.AppCtrl');

goog.require('datapipeline.components.appconfig.AppConfig');
goog.require('datapipeline.components.pipelines.Pipelines');

goog.scope(function() {

/**
 * Data Pipeline controller.
 *
 * @param {datapipeline.components.appconfig.AppConfig} AppConfig
 * @param {datapipeline.components.pipelines.Pipelines} Pipelines
 * @constructor
 * @ngInject
 */
datapipeline.components.app.AppCtrl = function(AppConfig,
                                               Pipelines) {
  this.AppConfig = AppConfig;
  this.Pipelines = Pipelines;
};
var AppCtrl = datapipeline.components.app.AppCtrl;

/** Choose between two values based on a boolean value.
 * @param {boolean} condition the boolean value to check.
 * @param {string} ifTrue value to return if condition is true.
 * @param {string} ifFalse value to return if condition is false.
 * @return {string} either the ifTrueor or ifFalse value.
 */
AppCtrl.prototype.ternary = function(
  condition, ifTrue, ifFalse) {
  return condition ? ifTrue : ifFalse;
};
}); // goog.scope
