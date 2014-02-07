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
 */
goog.provide('datapipeline.components.pipelineeditor.PipelineEditorCtrl');

goog.require('datapipeline.components.butterbar.ButterBar');
goog.require('datapipeline.components.pipelines.Pipelines');


goog.scope(function() {

/**
 * Pipeline controller.
 *
 * @param {angular.Scope} $scope
 * @param {angular.Timeout} $timeout
 * @param {angular.Window} $window
 * @param {datapipeline.components.butterbar.ButterBar} ButterBar service.
 * @param {datapipeline.components.pipelines.Pipelines} Pipelines service.
 * @constructor
 * @ngInject
 */
datapipeline.components.pipelineeditor.PipelineEditorCtrl = function(
    $scope,
    $timeout,
    $window,
    ButterBar,
    Pipelines) {
  this.scope = $scope;
  this.timeout = $timeout;
  this.window = $window;

  this.ButterBar = ButterBar;
  this.Pipelines = Pipelines;

  this.linted = false;
  this.lintErrors = [];
  this.lintDescription = '';
  this.lintClass = '';
  this.runUrlShown = false;

  this.configLintTimeout = null;

  this.stableTime = 1000;  // only validate the config every second tops.

  // TODO(user) watch this with a ng-change
  this.scope.$watch('pipelineEditorCtrl.Pipelines.selected.config',
                    angular.bind(this, this.configChanged));
};

var PipelineEditorCtrl = datapipeline.components.pipelineeditor.
    PipelineEditorCtrl;


/** Cancel editing a pipe. */
PipelineEditorCtrl.prototype.cancel = function() {
  if (this.Pipelines.isNew()) {
    this.Pipelines.removeById(this.Pipelines.selected.id);
  } else if (this.Pipelines.isChanged()) {
    this.Pipelines.revert();
  }
  this.Pipelines.deselect();
};

/** Called when the config changes. */
PipelineEditorCtrl.prototype.configChanged = function() {
  if (!this.Pipelines.selected ||
      !this.Pipelines.selected.config ||
      !this.Pipelines.isChanged()) {
    this.linted = true;
    this.lintErrors = [];
    return;
  }
  this.linted = false;
  if (this.configLintTimeout) {
    console.log('cancel timeout');
    this.timeout.cancel(this.configLintTimeout);
  }
  this.configLintTimeout = this.timeout(
    angular.bind(this, this.lintConfig), this.stableTime);
};

/** Delete the pipe we are currently editing. */
PipelineEditorCtrl.prototype.deletePipeline = function() {
  if (confirm('Are you sure you want to delete the pipeline: ' +
              this.Pipelines.selected.name +
              '\nThis cannot be undone.')) {
    this.Pipelines.deleteSelected();
  }
};

/** Describe any lint errors, or if lint is pending */
PipelineEditorCtrl.prototype.describeLintErrors = function() {
  if (!this.linted) {
    return 'Please wait for the pipeline to be linted.';
  } else if (this.lintErrors.length > 0) {
    return 'Please fix lint error(s).';
 } else {
   return '';
 }
};

/** Describe why you can't run the pipeline
 * @return {string} description of why buttons are disabled.
 */
PipelineEditorCtrl.prototype.describeRunErrors = function() {
  var lintErrors = this.describeLintErrors();
  if (lintErrors) {
    return lintErrors;
  }
  if (this.Pipelines.isChanged()) {
    return 'Save the changed pipeline';
  } else {
    return '';
  }
};

/** Describe why you can't save save a pipeline. */
PipelineEditorCtrl.prototype.describeSaveErrors = function() {
  if (this.Pipelines.selected) {
    var lintErrors = this.describeLintErrors();
    if (lintErrors) {
      return lintErrors;
    }
    if (!(this.Pipelines.selected.name)) {
      return 'Please provide a pipeline name.';
    }
  }
  if (this.Pipelines.isChanged()) {
    return '';
  } else {
    return 'Nothing to save.';
  }
};

/** Run the pipeline. */
PipelineEditorCtrl.prototype.runPipeline = function() {
  this.window.open(this.getRunUrl());
};

/** Get the runUrl for this pipeline and variables */
PipelineEditorCtrl.prototype.getRunUrl = function() {
  return this.Pipelines.getRunUrl();
};

/** Show the run URL and select and focus it. */
PipelineEditorCtrl.prototype.showRunUrl = function() {
  this.runUrlShown = true;
  $('#runUrl').each(function() {
    this.focus();
    this.select();
  });
};

/** Save the pipeline.
 * @param {?Function=} opt_callback callback to call after saving (optional).
 */
PipelineEditorCtrl.prototype.savePipeline = function(
    opt_callback) {
  // TODO(user) If pipeline is modified revert it back to original value.
  console.log('saving pipeline');
  var isNew = this.Pipelines.isNew();
  var message = 'Saving new pipeline';
  if (!isNew) {
    message = 'Saving pipeline: ' + this.Pipelines.selected.name;
  }

  var workId = this.ButterBar.startWork(message);
  this.Pipelines.selected.$save(angular.bind(this, function(reply) {
    if (isNew) {
      console.log('pipeline was new, adding it');
      this.Pipelines.selected.id = reply.id;
      this.Pipelines.selected.api_key = reply.api_key;
      // this.Pipelines.pipelines.push(this.Pipelines.selected);
    }
    this.Pipelines.select(this.Pipelines.selected);
    this.ButterBar.finishWork(workId);
    if (opt_callback) {
      opt_callback();
    }
  }));
};

/** Called when the config hasn't changed in a while. */
PipelineEditorCtrl.prototype.lintConfig = function() {
  var workId = this.ButterBar.startWork('Linting pipeline');
  this.lintDescription = 'Linting';
  this.lintErrors = [];
  this.lintClass = 'pending';
  console.log('lintConfig!');
  this.configLintTimeout = null;
  this.Pipelines.selected.$lint(angular.bind(this, function(reply) {
    this.ButterBar.finishWork(workId);
    reply = reply.lint;

    console.log('linted!!\n' + reply);
    // we only want to show the lint things that failed to pass.

    function getStageTypeFromLintResults(stage) {
      var ans = Object.keys(stage).filter(function(key) {
        return key.substring(0, 11) == 'TypeValid [';
      }).map(function(key) {
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
                      return {'section': stageSection,
                              'type': stageType,
                              'reason': stage[key]['reason']};
                                          }));
          });
        }
      }
      return errors;
    }

    this.lintErrors = [].concat(
        // First the general errors
        Object.keys(reply).filter(function(key) {
                                    return reply[key]['pass'] === false;
                                  }).map(function(key) {
                                    return {reason: reply[key]['reason']};
                                  }),
        getStageSectionErrors('inputs'),
        getStageSectionErrors('transforms'),
        getStageSectionErrors('outputs'));
    this.linted = true;
    if (this.lintErrors.length > 0) {
      this.lintClass = 'error';
      this.lintDescription = '';
      this.ButterBar.message('Linting found ' + this.lintErrors.length +
          ' error(s). See "Lint" section below.');
    } else {
      this.lintClass = 'success';
      this.lintDescription = 'Success';
    }
    this.Pipelines.getVariables();
  }));
};

}); // goog.scope
