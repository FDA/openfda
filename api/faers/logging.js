// FAERS API Logging

// Based on: http://www.elasticsearch.org/guide/
// en/elasticsearch/client/javascript-api/current/logging.html

var bunyan = require('bunyan');

exports.GetLogger = function() {
  return bunyan.createLogger({
    name: 'openfda-api-logger',
    stream: process.stderr,
    level: 'info'
  });
};

exports.ElasticsearchLogger = function(config) {
  // config is the object passed to the client constructor.
  var logger = exports.GetLogger();

  this.error = logger.error.bind(logger);
  this.warning = logger.warn.bind(logger);
  this.info = logger.info.bind(logger);
  this.debug = logger.debug.bind(logger);
  this.trace = function(method, requestUrl, body,
                        responseBody, responseStatus) {
    logger.trace({
      method: method,
      requestUrl: requestUrl,
      body: body,
      responseBody: responseBody,
      responseStatus: responseStatus
    });
  };
  this.close = function() { /* bunyan's loggers do not need to be closed */ };
};
