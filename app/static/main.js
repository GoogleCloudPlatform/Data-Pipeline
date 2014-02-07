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
 * @fileoverview Primary module for the Data Pipeline app.
 */
goog.provide('datapipeline.application.module');
goog.provide('datapipeline.application.routeProvider');

goog.require('datapipeline.components.app.AppCtrl');
goog.require('datapipeline.components.appconfig.AppConfig');
goog.require('datapipeline.components.appconfig.AppConfigCtrl');
goog.require('datapipeline.components.appconfig.AppConfigData');
goog.require('datapipeline.components.butterbar.ButterBar');
goog.require('datapipeline.components.butterbar.ButterBarCtrl');
goog.require('datapipeline.components.download');
goog.require('datapipeline.components.dropupload');
goog.require('datapipeline.components.layout');
goog.require('datapipeline.components.pipelineeditor.PipelineEditorCtrl');
goog.require('datapipeline.components.pipelinelist.PipelineListCtrl');
goog.require('datapipeline.components.pipelines.Pipelines');
goog.require('datapipeline.components.upload');
goog.require('datapipeline.components.user.User');
goog.require('datapipeline.components.variables.VariablesCtrl');

/**
 * The main module for the datapipeline app.
 */
datapipeline.application.module = angular.module('datapipeline',
    [
      datapipeline.components.appconfig.AppConfig.module.name,
      datapipeline.components.appconfig.AppConfigData.module.name,
      datapipeline.components.butterbar.ButterBar.module.name,
      datapipeline.components.download.module.name,
      datapipeline.components.dropupload.module.name,
      datapipeline.components.layout.module.name,
      datapipeline.components.pipelines.Pipelines.module.name,
      datapipeline.components.upload.module.name,
      datapipeline.components.user.User.module.name,
      'ngRoute'
    ]);

// register all controllers
var controllers = {
  AppConfigCtrl: datapipeline.components.appconfig.AppConfigCtrl,
  AppCtrl: datapipeline.components.app.AppCtrl,
  ButterBarCtrl: datapipeline.components.butterbar.ButterBarCtrl,
  PipelineEditorCtrl:
      datapipeline.components.pipelineeditor.PipelineEditorCtrl,
  PipelineListCtrl: datapipeline.components.pipelinelist.PipelineListCtrl,
  VariablesCtrl: datapipeline.components.variables.VariablesCtrl
};

for (var key in controllers) {
  datapipeline.application.module.controller(key, controllers[key]);
}
