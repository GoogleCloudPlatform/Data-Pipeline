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
 * @fileoverview Angular directive that downloads the content of a scope.
 *
 * The directive downloads the contents of a scope variable into the format
 * described when clicked on a provided button. A HTML5 blob is created to
 * contain the data hence the file is created on the client side.
 *
 * Example usage:
 *    <button download ng-model="pipeline.config" mime-type="application/json"
 *              file-name="pipeline_config.json">Download</button>
 */
goog.provide('datapipeline.components.download');

goog.scope(function() {

/**
 * Directive for download.
 * The directive considers the following options:
 *   file-name: Name of the file when downloaded. The extension is
 *       automatically added according to the mime-type.
 *   mime-type: Type of the file to download.
 *   ng-model: The model whose contents are to be downloaded.
 * @return {Object} Directive definition object.
 */
datapipeline.components.download = function() {
  return {
    restrict: 'A',
    scope: {
      ngModel: '=',
      fileName: '@',
      mimeType: '@'
    },
    link: function(scope, element, attrs) {
      var download = angular.element('<a>')[0];
      element.bind('click', function(event) {
         var url = scope.prepareBlobUrl();
         download.href = url;
         download.download = scope.fileName;
         download.click();
      });

      scope.prepareBlobUrl = function() {
        var content = angular.isUndefined(scope.ngModel) ? '' : scope.ngModel;
        var blob = new Blob([content], {'type': scope.mimeType});
        var url = URL.createObjectURL(blob);
        return url;
      };
    }
  };
};

/**
 * Angular module.
 *
 * @type {angular.Module}
 */
datapipeline.components.download.module = angular.module(
    'datapipeline.components.download', []).
  directive('download', datapipeline.components.download);
}); // goog.scope
