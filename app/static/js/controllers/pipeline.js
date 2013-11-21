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
 * @fileoverview Controller for the Pipeline configuration.
 *
 */

/**
 * Pipeline controller.
 *
 * @param {angular.Http} $http
 * @param {angular.Scope} $scope
 * @param {angular.Timeout} $timeout
 * @param {angular.Window} $window
 * @constructor
 * @ngInject
 */
datapipeline.controllers.PipelineCtrl = function($http, $scope,
                                                 $timeout, $window) {
  this.http = $http;
  this.scope = $scope;
  this.timeout = $timeout;
  this.window = $window;

  this.stableTime = 1000;  // only validate the config every second tops.

  this.scope.cancelPipelineEdit = angular.bind(this, this.cancelPipelineEdit);
  this.scope.configChanged = angular.bind(this, this.configChanged);
  this.scope.deletePipeline = angular.bind(this, this.deletePipeline);
  this.scope.describeDisabled = angular.bind(this, this.describeDisabled);
  this.scope.isNew = angular.bind(this, this.isNew);
  this.scope.runPipeline = angular.bind(this, this.runPipeline);
  this.scope.savePipeline = angular.bind(this, this.savePipeline);
  this.scope.lintConfig = angular.bind(this, this.lintConfig);
  this.scope.lintErrors = [];
  this.scope.linted = false;
  this.scope.lintDescription = '';
  this.scope.$watch('pipeline.config', this.scope.configChanged);

  this.configLintTimeout = null;
};

/** Cancel editing a pipe. */
datapipeline.controllers.PipelineCtrl.prototype.cancelPipelineEdit = function(
) {
  // TODO(user) revert back to the original?
  this.scope.$parent.pipeline = null;
};

/** Called when the config changes. */
datapipeline.controllers.PipelineCtrl.prototype.configChanged = function() {
  if (!this.scope.pipeline ||
      !this.scope.pipeline.config) {
    return;
  }
  this.scope.linted = false;
  if (this.configLintTimeout) {
    console.log('cancel timeout');
    this.timeout.cancel(this.configLintTimeout);
  }
  this.configLintTimeout = this.timeout(
    angular.bind(this, this.lintConfig), this.stableTime);
  this.scope.lintState = 'changed';
};

/** Delete the pipe we are currently editing. */
datapipeline.controllers.PipelineCtrl.prototype.deletePipeline = function() {
  if (confirm('Are you sure you want to delete the pipeline: ' +
              this.scope.pipeline.name +
              '\nThis cannot be undone.')) {
    // TODO(user) revert back to the original?
    var workId = this.scope.startWork('Deleting pipeline');
    this.scope.pipeline.$delete(angular.bind(this, function() {
      var pipelineId = this.scope.pipeline.id;
      this.scope.$parent.pipeline = null;
      // now remove the pipeline from the pipelines array
      var idx = -1;
      for (var i = 0; i < this.scope.pipelines.length; i++) {
        if (this.scope.pipelines[i].id === pipelineId) {
          idx = i;
          break;
        }
      }
      if (idx != -1) {
        this.scope.pipelines.splice(idx, 1);
      } else {
        console.log('Could not find pipeline with ID: ' + pipelineId);
      }
      this.scope.finishWork(workId);
    }));
  }
};

/** Describe why buttons are disabled.
 * @return {string} description of why buttons are disabled.
 */
datapipeline.controllers.PipelineCtrl.prototype.describeDisabled = function() {
  if (!this.scope.linted) {
    return 'Please wait for the pipeline to be linted.';
  } else if (this.scope.lintErrors.length > 0) {
    return 'Please fix lint error(s).';
  } else {
    return '';
  }
};

/** Is the pipeline currently displayed new?
 * @return {boolean} is the pipeline new.
 */
datapipeline.controllers.PipelineCtrl.prototype.isNew = function() {
  return this.scope.pipeline && this.scope.pipeline.id == 'new';
};

/** Run the pipeline. */
datapipeline.controllers.PipelineCtrl.prototype.runPipeline = function() {
  var url = ('/run/' + this.scope.pipeline.name + '/' +
             this.scope.pipeline.api_key);
  this.savePipeline(angular.bind(this, function() {
    this.window.open(url);
  }));
};

/** Save the pipeline.
 * @param {?Function=} opt_callback callback to call after saving (optional).
 */
datapipeline.controllers.PipelineCtrl.prototype.savePipeline = function(
    opt_callback) {
  // TODO(user) If pipeline is modified revert it back to original value.
  console.log('saving pipeline');
  var isNew = this.isNew();
  var message = 'Saving new pipeline';
  if (!isNew) {
    message = 'Saving pipeline: ' + this.scope.pipeline.name;
  }

  var workId = this.scope.startWork(message);
  this.scope.pipeline.$save(angular.bind(this, function(reply) {
    // console.log('we got a reply of: ' + JSON.stringify(reply));
    if (isNew) {
      console.log('pipeline was new, adding it');
      this.scope.pipeline.id = reply.id;
      this.scope.pipeline.api_key = reply.api_key;
      this.scope.pipelines.push(this.scope.pipeline);
    }
    this.scope.$parent.pipeline = null;
    this.scope.finishWork(workId);
    if (opt_callback) {
      opt_callback();
    }
  }));
};

/** Called when the config hasn't changed in a while. */
datapipeline.controllers.PipelineCtrl.prototype.lintConfig = function() {
  var workId = this.scope.startWork('Linting pipeline');
  this.scope.lintDescription = 'Linting';
  this.scope.lintErrors = [];
  this.scope.lintClass = 'pending';
  console.log('lintConfig!');
  this.configLintTimeout = null;
  this.http.post('/action/lint', angular.toJson(this.scope.pipeline)).
    success(angular.bind(this, function(reply) {
      this.scope.finishWork(workId);
      console.log('linted!!\n' + reply);
      // we only want to show the lint things that failed to pass.

      function getStageTypeFromLintResults(stage) {
        var ans = Object.keys(stage).filter(function(key) {
          console.log(key.substring(0, 11));
          return key.substring(0, 11) == 'TypeValid [';
        }).map(function(key) {
          console.log(key + ' : ' + key.substring(11, key.length - 1));
          return key.substring(11, key.length - 1);
        });
        if (ans.length > 0) {
          return ans[0];
        } else {
          return null;
        }
      }

      function getStageSectionErrors(stageSection) {
        var errors = [];
        if (reply['stages']) {
          var stages = reply['stages'][stageSection];
          if (stages) {
            stages.map(function(stage) {
              var stageType = getStageTypeFromLintResults(stage);
              errors = errors.concat(
                Object.keys(stage).filter(function(key) {
                  return stage[key] && stage[key]['pass'] === false;
                }).map(function(key) {
                  console.log(stageSection + ':' + stageType + ':' +
                              stage['type'] + ':' +
                              stage[key]['reason'] + JSON.stringify(stage));
                  return {'section': stageSection,
                          'type': stageType,
                          'reason': stage[key]['reason']};
                }));
            });
          }
        }
        return errors;
      }

      this.scope.lintErrors = [].concat(
        // First the general errors
        Object.keys(reply).filter(function(key) {
          return reply[key]['pass'] === false;
        }).map(function(key) {
          return reply[key]['reason'];
        }),
        getStageSectionErrors('inputs'),
        getStageSectionErrors('transforms'),
        getStageSectionErrors('outputs'));
      this.scope.linted = true;
      if (this.scope.lintErrors.length > 0) {
        this.scope.lintClass = 'error';
        this.scope.lintDescription = '';
        this.scope.message('Linting found ' + this.scope.lintErrors.length +
                           ' error(s). See "Lint" section below.');
      } else {
        this.scope.lintClass = 'success';
        this.scope.lintDescription = 'Success';
      }
    })).
    error(angular.bind(this, function(reply) {
      this.scope.finishWork(workId);
      this.scope.message(reply);
    }));
};
