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
 * @fileoverview Controller for the Pipeline list.
 *
 */

/**
 * PipelineList controller.
 *
 * @param {angular.Scope} $scope
 * @param {angular.Window} $window
 * @constructor
 * @ngInject
 */
datapipeline.controllers.PipelineListCtrl = function($scope, $window) {
  this.scope = $scope;

  this.scope.selectPipeline = angular.bind(this, this.selectPipeline);
  this.scope.showSearch = angular.bind(this, this.showSearch);

  this.scope.pipelineListSearch = '';

  // listen to resizes and call $apply (so we might show/hide search box).
  angular.element($window).bind('resize', angular.bind(this, function() {
    this.scope.$apply();
  }));
};

/** Called when user clicks on a pipeline in the list, edits pipeline.
 * @param {Pipeline} p the pipeline they clicked on.
 */
datapipeline.controllers.PipelineListCtrl.prototype.selectPipeline = function(
  p) {
  console.log('Click Pipeline: ' + JSON.stringify(p));
  this.scope.$parent.pipeline = p;
};

/** Are there so many pipelines we want to show a seach box.
 * TODO(user): move this into a directive (see cr/50262334)
 * @return {boolean} should we show the search box.
 */
datapipeline.controllers.PipelineListCtrl.prototype.showSearch = function() {
  if (this.scope.pipelineListSearch) {
    return true;  // we're already searching for something so keep it shown.
  }
  var el = $('.pipeline-list-items')[0];
  return Math.abs(el.clientHeight - el.scrollHeight) > 5;
};
