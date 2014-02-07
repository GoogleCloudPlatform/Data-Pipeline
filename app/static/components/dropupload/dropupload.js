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
/** @fileoverview Directive to read and display files when dragged and dropped.
 *
 * Files that are dropped on an element will be read as a string and the
 * contents will be attached to scope. If the directive is applied to a textarea
 * the contents of the textarea are replaced with the contents of the file
 * through the ngModel.
 *
 * Uses HTML5 file reader API to read files on the client side.
 **/
goog.provide('datapipeline.components.dropupload');

goog.scope(function() {
/**
 * The directive considers the following options:
 *   mime-types: An array of acceptable mime types that is accepted and read.
 *   file-content: If applied on an element other than textarea, this scope
 *       variable will contain the content of the file.
 *   max-file-size: The max file size that can be uploaded in MB defaults to 2mb
 *       if not specified.
 *
 * Example:
 * <textarea
 *    dropupload
 *    ng-model="my.model"
 *    max-file-size="5"
 *    mime-types="[application/json, text/plain]"
 *    placeholder="Drag and drop a config file here...">
 * </textarea>
 *
 * @return {!Object} Angular directive definition object.
 **/
datapipeline.components.dropupload = function() {
  return {
    require: 'ngModel',
    restrict: 'A',
    scope: {
      fileContent: '=',
      model: '=ngModel'
    },
    link: function(scope, element, attr) {
      // jQuery creates it's own event object and requires explicit request for
      // datatransfer event object.
      jQuery.event.props.push('dataTransfer');

      var handleDrop = function(evt) {
        evt.preventDefault();
        var reader = new FileReader();
        reader.onload = function(event) {
          if (event.target.readyState == FileReader.DONE) {
            scope.$apply(function() {
              // Change content if applied to a textarea otherwise
              // attach to scope.
              if (element[0].type == 'textarea') {
                scope.model = event.target.result;
              } else {
                scope.fileContent = event.target.result;
              }
            });
          }
        };

        var file = evt.dataTransfer.files[0];
        if (isValidMimeType(file.type) && isUnderMaxFileSize(file.size)) {
          reader.readAsText(file);
        }
        return false;
      };

      var isValidMimeType = function(mimeType) {
        if (attr.mimeTypes.indexOf(mimeType) > -1) {
          return true;
        }
        return false;
      };

      var isUnderMaxFileSize = function(fileSize) {
        var fileSizeMb = fileSize / (1024 * 1024);
        // Restrict to 2MB by default.
        var maxFileSize = attr.maxFileSize || 2;
        if (fileSizeMb < maxFileSize) {
          return true;
        }
        return false;
      };

      var handleDragOver = function(evt) {
        evt.stopPropagation();
        evt.preventDefault();
        evt.dataTransfer.effectAllowed = 'copy';
        return false;
      };

      // Copy ngModel to directive scope and update as ngModel has a different
      // scope than the directive.
      scope.$watch('model', function() {
        scope.$eval(attr.ngModel + ' = model');
      });

      scope.$watch(attr.ngModel, function(val) {
        scope.model = val;
      });

      element.bind('dragover', handleDragOver);
      element.bind('drop', handleDrop);
    }
  };
};

/**
 * Angular module.
 *
 * @type {angular.Module}
 */
datapipeline.components.dropupload.module = angular.module(
    'datapipeline.components.dropupload', []).
        directive('dropupload', datapipeline.components.dropupload);
}); // goog.scope
