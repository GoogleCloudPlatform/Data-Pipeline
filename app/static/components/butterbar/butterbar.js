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
 * @fileoverview Show a yellow bar showing that work is being done.
 */
goog.provide('datapipeline.components.butterbar.ButterBar');
goog.provide('datapipeline.components.butterbar.ButterBarCtrl');

goog.scope(function() {

/**
 * A service to show a butter bar with information at the top of the app.
 *
 * @param {angular.$timeout} $timeout
 * @constructor
 * @ngInject
 */
datapipeline.components.butterbar.ButterBar = function($timeout) {
  this.timeout = $timeout;
  // An array of work being done. Each item is an object with an
  // workId and a description.
  this.workId = 0;  // the last workId used.
  this.workBeingDone = [];
};
var ButterBar = datapipeline.components.butterbar.ButterBar;

/** Display a message for a short time at the top of the UI.
 * @param {string} message to display.
 * @param {!int} timeout in microseconds
 */
ButterBar.prototype.message = function(message, timeout) {
  // We'll reuse the start/finish work system since that works well.
  var workId = this.startWork(message);
  this.timeout(angular.bind(this, function() {
    this.finishWork(workId);
  }), timeout || 3000);
};

/** let the user know we are doing some work.
 * @param {string} message to display.
 * @return {int} the workId of this message (for cancelling later).
 */
ButterBar.prototype.startWork = function(message) {
  this.workId += 1;
  this.workBeingDone.push({workId: this.workId, message: message});
  console.log('startWork  (' + this.workId + ' of ' +
              this.workBeingDone.length + ') ' + message);
  return this.workId;
};

/** Finish the annoucement that work is being done.
 * @param {int} workId the if we wish to cancel.
 */
ButterBar.prototype.finishWork = function(workId) {
  // go through the work objects and remove this id
  // if this id is the last one, update the UI.
  for (var i = 0; i < this.workBeingDone.length; i++) {
    if (this.workBeingDone[i].workId === workId) {
      console.log('finishWork (' + workId + ' of ' +
              (this.workBeingDone.length - 1) + ') ' +
          this.workBeingDone[i].message + ' (@' + i + ')');
      this.workBeingDone.splice(i, 1);
      return;
    }
  }
  console.log('Could not find work to remove with workId: ' + workId);
};


/**
 * Angular module.
 *
 * @type {angular.Module}
 */
datapipeline.components.butterbar.ButterBar.module = angular.module(
    'datapipeline.components.butterbar.ButterBar', []).
    service('ButterBar', ButterBar);

/**
 * ButterBarCtrl controller for the div that shows the butterbar.
 *
 * @param {datapipeline.components.butterbar.ButterBar} ButterBar
 * @constructor
 * @ngInject
 */
datapipeline.components.butterbar.ButterBarCtrl = function(ButterBar) {
  this.ButterBar = ButterBar;
};
var ButterBarCtrl = datapipeline.components.butterbar.ButterBarCtrl;


}); // goog.scope
