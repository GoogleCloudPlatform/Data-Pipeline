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
 * @fileoverview The List of all pipelines.
 */
goog.provide('datapipeline.components.pipelines.PipelineData');
goog.provide('datapipeline.components.pipelines.Pipelines');

goog.require('datapipeline.components.butterbar.ButterBar');
goog.require('datapipeline.components.user.User');

goog.scope(function() {

/**
 * A service to CRUD Pipeline data from the server.
 */
datapipeline.components.pipelines.PipelineData.module = angular.module(
    'datapipeline.components.pipelines.PipelineData',
               ['ngResource']).
  factory('PipelineData', function($resource) {
    return $resource('/data/pipeline/?id=:id&parent_id=:parent_id',
                     {id: '@id', parent_id: '@parent_id'},
                     {
                       variables: {method: 'POST', url: '/data/variables'},
                       lint: {method: 'POST', url: '/action/lint'}
                     });
          });

/**
 * A service with information about all the pipelines.
 *
 * @param {angular.Location} $location
 * @param {datapipeline.components.appconfig.AppConfig} AppConfig service.
 * @param {datapipeline.components.butterbar.ButterBar} ButterBar service.
 * @param {datapipeline.components.pipelines.PipelineData} PipelineData service.
 * @param {datapipeline.components.user.User} User service.
 * @constructor
 * @ngInject
 */
datapipeline.components.pipelines.Pipelines = function($location,
                                                       AppConfig,
                                                       ButterBar,
                                                       PipelineData,
                                                       User) {
  this.location = $location;
  this.AppConfig = AppConfig;
  this.ButterBar = ButterBar;
  this.PipelineData = PipelineData;
  this.User = User;

  this.pipelines = undefined;  // array of all the pipelines.
  this.selected = undefined;  // the currently selected pipeline (PipelineData).
  this.selectedOriginal = undefined; // copy of the original selected pipeline.

  this.pipelineValueKeys = ['name', 'api_key', 'config'];

  var workId = ButterBar.startWork('Loading Pipelines');
  PipelineData.query(angular.bind(this, function(p) {
    ButterBar.finishWork(workId);
    this.pipelines = p;
    var id = this.location.search().id;
    if (id) {
      for (var i = 0; i < p.length; i++) {
        if (p[i].id == id) {
          this.select(p[i]);
          this.getVariables();
          break;
        }
      }
    }
  }));
};

var Pipelines = datapipeline.components.pipelines.Pipelines;

/** Constant for new pipeline's id. */
Pipelines.NEW_ENTITY_ID = 'new';

/** Create a new pipeline. */
Pipelines.prototype.create = function() {
  console.log('Creating a pipeline');
  // TODO(user) check if current pipeline is modified or not, pop up warning

  var workId = this.ButterBar.startWork('Creating new pipeline');
  this.PipelineData.get({
    id: Pipelines.NEW_ENTITY_ID,
    parent_id: this.User.data.id}, angular.bind(this, function(p) {
        this.ButterBar.finishWork(workId);
        this.select(p);
        this.pipelines.push(p);
    }));
};

/** Delete the currently selected pipeline from the list of pipelines */
Pipelines.prototype.deleteSelected = function() {
  // TODO(user) revert back to the original?
  var workId = this.ButterBar.startWork('Deleting pipeline');
  this.selected.$delete(angular.bind(this, function() {
    this.removeById(this.selected.id);
    this.deselect();
    this.ButterBar.finishWork(workId);
  }));
};

/** deselect any selected pipeline */
Pipelines.prototype.deselect = function() {
  this.select(null);
};

/** Get the run URL for the current selected pipeline (and variables). */
Pipelines.prototype.getRunUrl = function() {
  var ports = {'http': 80, 'https': 443};
  var runUrl = '';
  if (this.selected && this.selected.name) {
    runUrl = this.location.protocol() + '://' + this.location.host();
    if (ports[this.location.protocol()] != this.location.port()) {
      runUrl += ':' + this.location.port();
    }
    runUrl += ('/run/' +
        encodeURIComponent(this.selected.name) + '/' +
        encodeURIComponent(this.selected.api_key));
    if (this.selected.variables) {
      var vars_arr = [];
      for (var i = 0; i < this.selected.variables.length; i++) {
        var v = this.selected.variables[i];
        if (v.value !== '' && v.value !== undefined) {
          vars_arr.push(encodeURIComponent(v.name) + '=' +
              encodeURIComponent(v.value));
        }
      }
      runUrl += '?' + vars_arr.join('&');
    }
  }
  return runUrl;
};

/** get the variables used by this pipeline */
Pipelines.prototype.getVariables = function() {
  var workId = this.ButterBar.startWork('Getting variables');
  this.selected.$variables(angular.bind(this, function(reply) {
    this.ButterBar.finishWork(workId);
    this.variables = {};
    // this.variables || {};
    //    for (var i = 0; i < reply.length; i++) {
    //    if (!this.variables.hasOwnProperty(reply[i])) {
    //    this.variables[reply[i]] = '';
    //}
    //}
    console.log('Got the following variables back:\n' + JSON.stringify(reply));
  }));
};

/** remove a pipeline from the pipelines list by id (could be 'new').
 * @param {string} pipelineId the id of the pipeline to remove;
 */
Pipelines.prototype.removeById = function(pipelineId) {
  // now remove the pipeline from the pipelines array
  var idx = -1;
  for (var i = 0; i < this.pipelines.length; i++) {
    if (this.pipelines[i].id === pipelineId) {
      idx = i;
      break;
    }
  }
  if (idx != -1) {
    this.pipelines.splice(idx, 1);
  } else {
    console.log('Could not find pipeline with ID: ' + pipelineId);
  }
};

/** Has the currently selected pipeline been changed */
Pipelines.prototype.isChanged = function() {
  if (this.selected != null) {
    for (var i = 0; i < this.pipelineValueKeys.length; i++) {
      var key = this.pipelineValueKeys[i];
      if (JSON.stringify(this.selectedOriginal[key]) !=
          JSON.stringify(this.selected[key])) {
        return true;
      }
    }
  }
  return false;
};

/** Is the currently selected pipeline new */
Pipelines.prototype.isNew = function() {
  return this.selected && this.selected.id == 'new';
};

/** Revert the selected Pipeline to the original value
 */
Pipelines.prototype.revert = function() {
  for (var i = 0; i < this.pipelineValueKeys.length; i++) {
    var key = this.pipelineValueKeys[i];
    this.selected[key] = JSON.parse(JSON.stringify(this.selectedOriginal[key]));
  }
};

/** Select a pipeline
 * @param {PipelineData} p the pipeline to select.
 */
Pipelines.prototype.select = function(p) {
  this.selected = p;
  this.selectedOriginal = p ? JSON.parse(JSON.stringify(p)) : null;
  this.location.search('id', p ? p.id : null);
};

/**
 * Angular module.
 *
 * @type {angular.Module}
 */
datapipeline.components.pipelines.Pipelines.module = angular.module(
    'datapipeline.components.pipelines.Pipelines', [
      datapipeline.components.appconfig.AppConfig.module.name,
      datapipeline.components.butterbar.ButterBar.module.name,
      datapipeline.components.pipelines.PipelineData.module.name,
      datapipeline.components.user.User.module.name
    ]).
    service('Pipelines', Pipelines);

}); // goog.scope
