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
 * @fileoverview Controller for the Variables configuration.
 */
goog.provide('datapipeline.components.variables.VariablesCtrl');

goog.require('datapipeline.components.butterbar.ButterBar');
goog.require('datapipeline.components.pipelines.Pipelines');


goog.scope(function() {

/**
 * Variables controller.
 *
 * @param {datapipeline.components.butterbar.ButterBar} ButterBar service.
 * @param {datapipeline.components.pipelines.Pipelines} Pipelines service.
 * @constructor
 * @ngInject
 */
datapipeline.components.variables.VariablesCtrl = function(
    ButterBar,
    Pipelines) {
  this.ButterBar = ButterBar;
  this.Pipelines = Pipelines;

  this.variablesListSearch = '';
};

var VariablesCtrl = datapipeline.components.variables.VariablesCtrl;

/** Are there so many pipelines we want to show a seach box.
 * TODO(user): move this into a directive (see cr/50262334)
 * @return {boolean} should we show the search box.
 */
VariablesCtrl.prototype.showSearch = function() {
  if (this.variablesListSearch) {
    return true;  // we're already searching for something so keep it shown.
  }
  var el = $('.variables-list-items')[0];
  return Math.abs(el.clientHeight - el.scrollHeight) > 5;
};

}); // goog.scope
