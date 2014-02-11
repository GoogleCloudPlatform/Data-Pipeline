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
/** @fileoverview Angular directive for jQuery layout. */
goog.provide('datapipeline.components.layout');

goog.scope(function() {
/**
 * Directive to enable jQuery layout on tagged element.
 *
 * Use it like this:
 * <div layout layoutoptions="closable: true, resizable: true">
 *   <div class="ui-layout-north"></div>
 *   <div class="ui-layout-center"></div>
 * </div>
 *
 * @param {angular.$timeout} $timeout Provides timeout function for deferring.
 * @return {!Object} Angular directive definition object.
 */
datapipeline.components.layout = function($timeout) {
  return {
    link: function(scope, element, attrs) {
      var options;
      eval('options = {' + attrs.layoutoptions + '}');
      if (attrs.hasOwnProperty('embed') && attrs.embed == 'false') {
        element.layout(options);
      } else {
        // Defer element initialization to make sure its loaded after template
        // scope is ready.
        $timeout(function() { element.layout(options); });
      }
    }
  };
};

/**
 * Angular module.
 *
 * @type {!angular.Module}
 */
datapipeline.components.layout.module = angular.module(
    'datapipeline.components.layout', []).
  directive('layout', datapipeline.components.layout);
}); // goog.scope
