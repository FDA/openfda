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

exports.testZeroLimit = function (test) {
    var request = 'search=foo&limit=0';
    var params = querystring.parse(request);
    var cleanedParams = api_request.CheckParams(params);
    test.equal(0, cleanedParams.limit);
    test.done();
};

exports.testNonNumericSkip = function(test) {
    var request = 'search=foo&skip=501k';
    var params = querystring.parse(request);
    apiRequestError(test, params);

    test.done();
};

exports.testNonNumericLimit = function(test) {
    var request = 'search=foo&limit=501k';
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

exports.testSearchAfterWithSkip = function(test) {
  let request = 'search=foo&search_after=1&skip=1';
  let params = querystring.parse(request);
  apiRequestError(test, params);

  request = 'search=foo&search_after=1&skip=0';
  params = querystring.parse(request);
  apiRequestValid(test, params);

  request = 'search=foo&search_after=1';
  params = querystring.parse(request);
  apiRequestValid(test, params);

  test.done();
};


apiRequestValid = function(test, params) {
  test.doesNotThrow(function() { api_request.CheckParams(params) },
                    api_request.API_REQUEST_ERROR,
                    'Should be valid: ' + JSON.stringify(params));
};

exports.testSort = function(test) {
    var request = 'search=foo&sort=receiptdate';
    var params = querystring.parse(request);
    apiRequestValid(test, params);

    test.done();
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

exports.testCountMaxLimitExceeded = function(test) {
    var request = 'search=foo&count=bar&limit=1001';
    var params = querystring.parse(request);
    apiRequestError(test, params);

    test.done();
};


exports.testLimitlessCount = function(test) {
    var request = 'search=foo&count=openfda.generic_name&limit=2147482647';
    var params = querystring.parse(request);
    apiRequestValid(test, params);

    var request = 'search=foo&count=openfda.generic_name.exact&limit=2147482647';
    var params = querystring.parse(request);
    apiRequestValid(test, params);

    var request = 'search=foo&count=openfda.generic_name&limit=2147482648';
    var params = querystring.parse(request);
    apiRequestError(test, params);

    var request = 'search=foo&count=openfda.brand_name&limit=2147482647';
    var params = querystring.parse(request);
    apiRequestValid(test, params);

    var request = 'search=foo&count=openfda.brand_name.exact&limit=2147482647';
    var params = querystring.parse(request);
    apiRequestValid(test, params);

    var request = 'search=foo&count=openfda.brand_name&limit=2147482648';
    var params = querystring.parse(request);
    apiRequestError(test, params);

    var request = 'search=foo&count=openfda.substance_name&limit=2147482647';
    var params = querystring.parse(request);
    apiRequestValid(test, params);

    var request = 'search=foo&count=openfda.substance_name.exact&limit=2147482647';
    var params = querystring.parse(request);
    apiRequestValid(test, params);

    var request = 'search=foo&count=openfda.substance_name&limit=2147482648';
    var params = querystring.parse(request);
    apiRequestError(test, params);

    test.done();
};
