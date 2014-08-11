// FAERS API Request Test

var querystring = require('querystring');

var api_request = require('./api_request.js');

apiRequestError = function(test, params) {
  test.throws(function() { api_request.CheckParams(params) },
              api_request.API_REQUEST_ERROR,
              'Should be an API error: ' + JSON.stringify(params));
};

exports.testInvalidParam = function(test) {
  var request = 'search=foo&notvalid=true&skip=10';
  var params = querystring.parse(request);
  apiRequestError(test, params);

  test.done();
};

exports.testTooBigSearchLimit = function(test) {
  var request = 'search=foo&limit=101';
  var params = querystring.parse(request);
  apiRequestError(test, params);

  test.done();
};

exports.testTooBigCountLimit = function(test) {
  var request = 'search=foo&count=foo&limit=1001';
  var params = querystring.parse(request);
  apiRequestError(test, params);

  test.done();
};

exports.testCountRequestWithSkip = function(test) {
  // with skip
  var request = 'search=foo&count=bar&skip=10';
  var params = querystring.parse(request);
  apiRequestError(test, params);

  test.done();
};

apiRequestValid = function(test, params) {
  test.doesNotThrow(function() { api_request.CheckParams(params) },
                    api_request.API_REQUEST_ERROR,
                    'Should be valid: ' + JSON.stringify(params));
};

exports.testMaxLimit = function(test) {
  var request = 'search=foo&limit=100';
  var params = querystring.parse(request);
  apiRequestValid(test, params);

  test.done();
};

exports.testCountWithNoSearchParam = function(test) {
  var request = 'count=bar';
  var params = querystring.parse(request);
  apiRequestValid(test, params);

  test.done();
};

exports.testCountWithDot = function(test) {
  var request = 'count=primarysource.qualification';
  var params = querystring.parse(request);
  apiRequestValid(test, params);

  test.done();
};

exports.testCountMaxLimit = function(test) {
  var request = 'search=foo&count=bar&limit=1000';
  var params = querystring.parse(request);
  apiRequestValid(test, params);

  test.done();
};


// NDC /drug/info.json Tests
var assert = require('assert');

exports.testValidSearchNameParam = function(test) {
  var request = 'name=Cymbalta';
  var params = querystring.parse(request);

  var clean_params = api_request.CheckParams(params)

  assert.equal(params.name, 'Cymbalta', "Expected {\"name\":\"Cymbalta\"}")

  test.done();
};
