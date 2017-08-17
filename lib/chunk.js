// Copyright IBM Corp. 2015,2016. All Rights Reserved.
// Node module: loopback
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT
var async = require('async');
var CHUNK_SIZE = 100;

exports.processInChunks = processInChunks;
exports.concatResults = concatResults;
exports.downloadInChunks = downloadInChunks;



/**
 * Divide an async call with large array into multiple calls using smaller chunks
 * @param {Array} largeArray - the large array to be chunked
 * @param {Function} processFunction - the function to be called multiple times
 * @param {Function} cb - the callback
 */
function processInChunks(largeArray, processFunction, cb) {
  var self = this;
  var chunkArrays = [];
  var chunkSize = CHUNK_SIZE;

  if (this.settings && this.settings.chunkSize) {
    chunkSize = this.settings.chunkSize;
  }

  if (largeArray.length > chunkSize) {

    // copying so that the largeArray object doesnt get affected during splice
    var copyOFLargeArray = [].concat(largeArray);

    // chunking to smaller arrays
    while (copyOFLargeArray.length > 0) {
      chunkArrays.push(copyOFLargeArray.splice(0, chunkSize));
    }

    var tasks = chunkArrays.map(function(chunkArray) {
      return function(previousResults, chunkCallback) {

        var lastArg = arguments[arguments.length - 1];

        if (typeof lastArg === 'function') {
          chunkCallback = lastArg;
        }

        processFunction.call(self, chunkArray, function(err, results) {
          if (err) {
            return chunkCallback(err);
          }

          // if this is the first async waterfall call or if previous results was not defined
          if (typeof previousResults === 'function' || typeof previousResults === 'undefined' ||
            previousResults === null) {
            previousResults = results;
          } else if (results) {
            previousResults = concatResults(previousResults, results);
          }

          chunkCallback(err, previousResults);
        });
      };
    });

    async.waterfall(tasks, cb);
  } else {
    processFunction.call(self, largeArray, cb);
  }
}

/**
 * Concat current results into previous results
 * Assumption made here that the previous results and current results are homogeneous
 * @param {Object|Array} previousResults
 * @param {Object|Array} currentResults
 */
function concatResults(previousResults, currentResults) {
  if (Array.isArray(currentResults)) {
    previousResults = previousResults.concat(currentResults);
  } else if (typeof currentResults === 'object') {
    Object.keys(currentResults).forEach(function(key) {
      previousResults[key] = concatResults(previousResults[key], currentResults[key]);
    });
  } else {
    previousResults = currentResults;
  }

  return previousResults;
}

/**
 * Page async download calls
 * @param {Object} filter - filter object used for the async call
 * @param {Function} processFunction - the function to be called multiple times
 * @param {Function} cb - the callback
 */
function downloadInChunks(filter, processFunction, cb) {
  var chunkSize = CHUNK_SIZE;

  if (this.settings && this.settings.chunkSize) {
    chunkSize = this.settings.chunkSize;
  }

  var results = [];
  filter = filter ? JSON.parse(JSON.stringify(filter)) : {};
  filter.skip = 0;
  filter.limit = chunkSize;

  processFunction(filter, pageAndConcatResults);

  function pageAndConcatResults(err, pagedResults) {
    if (err) {
      cb(err);
    } else {
      results = concatResults(results, (pagedResults));
      if (pagedResults.length === chunkSize) {
        filter.skip += chunkSize;
        processFunction(filter, pageAndConcatResults);
      } else {
        cb(null, results);
      }
    }
  }
}
