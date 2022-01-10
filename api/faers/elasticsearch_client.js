const elasticsearch = require('elasticsearch');
const logging = require('./logging.js');
const client = new elasticsearch.Client({
  host: process.env.ES_HOST || 'localhost:9200',
  log: logging.ElasticsearchLogger,
  apiVersion: '5.6',
  // Note that this doesn't abort the query.
  requestTimeout: 30000  // milliseconds
});
exports.client = client;
