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
/** @fileoverview A custom file uploader that reads files to scope.
 *
 * A directive that converts any element to a clickable file selector and
 * reads the content of the selected file to a model.
 *
 * Uses HTML5 file reader API to read files on the client side.
 **/
goog.provide('datapipeline.components.upload');

goog.scope(function() {
/**
 * The sibiling can be any element that can act as a button and the directive
 * creates a hidden file input that is clicked by proxy through the custom
 * element. A custom button can be created that replaces the browser's native
 * file input widget.
 *
 * Example:
 *   <div upload style="display: inline;" ng-model="pipeline.config">
 *      <button class="maia-button maia-button-secondary">Import</button>
 *   </div>
 *
 * @return {!Object} Angular directive definition object.
 **/
datapipeline.components.upload = function() {
  return {
    restrict: 'A',
    scope: {
      ngModel: '='
    },
    link: function(scope, element, attr) {
      var fileInput = angular.element(
          '<input type="file" style="display: none;" />');

      fileInput.bind('change', function(event) {
        var file = event.target.files[0];
        var reader = new FileReader();
        reader.onload = function(e) {
          if (e.target.readyState == FileReader.DONE) {
            scope.$apply(function() {
              scope.ngModel = e.target.result;
            });
          }
        };
        reader.readAsText(file);
        return false;
      });

      element.bind('click', function() {
        fileInput.click();
      });
    }
  };
};

/**
 * Angular module.
 *
 * @type {angular.Module}
 */
datapipeline.components.upload.module = angular.module(
    'datapipeline.components.upload', []).
        directive('upload', datapipeline.components.upload);
}); // goog.scope
