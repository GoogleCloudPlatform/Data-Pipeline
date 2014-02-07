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
 * @fileoverview Provide the minimum code to be compatible with closure library.
 */

var goog = goog || {};

/** the global scope */
goog.global = this;

/** require something to be defined (no-op) */
goog.require = function() {};

/** Ensure that a package/namespace is defined.
 * @param {string} path the namespace being provided.
 */
goog.provide = function(path) {
  var path = path.split('.');
  var dat = goog.global;

  for (var idx = 0; idx < path.length; idx++) {
    if (!(path[idx] in dat)) {
      dat[path[idx]] = {};
    }
    dat = dat[path[idx]];
  }
};

/** Used to isolate variables within a file to allow aliases to namespaces.
 * @param {function} fn the function to call to define objects to use.
 */
goog.scope = function(fn) {
  fn.call(goog.global);
};
