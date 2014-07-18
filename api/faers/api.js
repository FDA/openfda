// FAERS and RES API
//
// Exposes /drug/event.json, /drug/safety/enforcement.json
//  and /healthcheck GET endpoints

var ejs = require('elastic.js');
var elasticsearch = require('elasticsearch');
var express = require('express');
var moment = require('moment');
var underscore = require('underscore');

var api_request = require('./api_request.js');
var elasticsearch_query = require('./elasticsearch_query.js');
var logging = require('./logging.js');
var META = {
  'disclaimer': 'openFDA is a beta research project and not for clinical ' +
                'use. While we make every effort to ensure that data is ' +
                'accurate, you should assume all results are unvalidated.',
  'license': 'http://open.fda.gov/license',
  'last_updated': '2014-05-29'
};

var HTTP_CODE = {
  OK: 200,
  BAD_REQUEST: 400,
  NOT_FOUND: 404,
  SERVER_ERROR: 500
};

// Internal fields to remove from ES drugevent objects before serving
// via the API.
var FIELDS_TO_REMOVE = [
  '@timestamp',
  '@case_number'
];

var DRUG_EVENT_INDEX = 'drugevent';
var ALL_ENFORCEMENT_INDEX = 'recall';

var app = express();

app.disable('x-powered-by');

// Set caching headers for Amazon Cloudfront
CacheMiddleware = function(seconds) {
  return function(request, response, next) {
    response.setHeader('Cache-Control', 'public, max-age=' + seconds);
    return next();
  };
};
app.use(CacheMiddleware(60));

// Use gzip compression
app.use(express.compress());

// Setup defaults for API JSON error responses
app.set('json spaces', 2);
app.set('json replacer', undefined);

var log = logging.GetLogger();

var client = new elasticsearch.Client({
  host: process.env.ES_HOST || 'localhost:9200',
  log: logging.ElasticsearchLogger,

  // Note that this doesn't abort the query.
  requestTimeout: 10000  // milliseconds
});

app.get('/healthcheck', function(request, response) {
  client.cluster.health({
    index: DRUG_EVENT_INDEX,
    timeout: 1000 * 60,
    waitForStatus: 'yellow'
  }, function(error, health_response, status) {
    health_json = JSON.stringify(health_response, undefined, 2);
    if (error != undefined) {
      response.send(500, 'NAK.\n' + error + '\n');
    } else if (health_response['status'] == 'red') {
      response.send(500, 'NAK.\nStatus: ' + health_json + '\n');
    } else {
      response.send('OK\n\n' + health_json + '\n');
    }
  });
});

ApiError = function(response, code, message) {
  error_response = {};
  error_response.error = {};
  error_response.error.code = code;
  error_response.error.message = message;
  response.json(HTTP_CODE[code], error_response);
};

LogRequest = function(request) {
  log.info(request.headers, 'Request Headers');
  log.info(request.query, 'Request Query');
};

SetHeaders = function(response) {
  response.header('Server', 'open.fda.gov');
  // http://john.sh/blog/2011/6/30/cross-domain-ajax-expressjs-
  // and-access-control-allow-origin.html
  response.header('Access-Control-Allow-Origin', '*');
  response.header('Access-Control-Allow-Headers', 'X-Requested-With');
  response.header('Content-Security-Policy', "default-src 'none'");
  // https://www.owasp.org/index.php/REST_Security_Cheat_Sheet
  // #Send_security_headers
  response.header('X-Content-Type-Options', 'nosniff');
  response.header('X-Frame-Options', 'deny');
  response.header('X-XSS-Protection', '1; mode=block');
};

TryToCheckApiParams = function(request, response) {
  try {
    return api_request.CheckParams(request.query);
  } catch (e) {
    log.error(e);
    if (e.name == api_request.API_REQUEST_ERROR) {
      ApiError(response, 'BAD_REQUEST', e.message);
    } else {
      ApiError(response, 'BAD_REQUEST', '');
    }
    return null;
  }
};

TryToBuildElasticsearchParams = function(params, elasticsearch_index, response) {
  try {
    var es_query = elasticsearch_query.BuildQuery(params);
    log.info(es_query.toString(), 'Elasticsearch Query');
  } catch (e) {
    log.error(e);
    if (e.name == elasticsearch_query.ELASTICSEARCH_QUERY_ERROR) {
      ApiError(response, 'BAD_REQUEST', e.message);
    } else {
      ApiError(response, 'BAD_REQUEST', '');
    }
    return null;
  }

  var es_search_params = {
    index: elasticsearch_index,
    body: es_query.toString()
  };

  if (!params.count) {
    es_search_params.from = params.skip;
    es_search_params.size = params.limit;
  }

  return es_search_params;
};

TrySearch = function(index, params, es_search_params, response) {
  client.search(es_search_params).then(function(body) {
    if (body.hits.hits.length == 0) {
      ApiError(response, 'NOT_FOUND', 'No matches found!');
    }

    var response_json = {};
    response_json.meta = underscore.clone(META);
    if (index == ALL_ENFORCEMENT_INDEX) {
      response_json.meta.last_updated = "2014-07-11";
    }

    if (!params.count) {
      response_json.meta.results = {
        'skip': params.skip,
        'limit': params.limit,
        'total': body.hits.total
      };

      response_json.results = [];
      for (i = 0; i < body.hits.hits.length; i++) {
        var drugevent = body.hits.hits[i]._source;
        for (j = 0; j < FIELDS_TO_REMOVE.length; j++) {
          delete drugevent[FIELDS_TO_REMOVE[j]];
        }
        response_json.results.push(drugevent);
      }
      response.json(HTTP_CODE.OK, response_json);

    } else if (params.count) {
      if (body.facets.count.terms) {
        // Term facet count
        if (body.facets.count.terms.length != 0) {
          response_json.results = body.facets.count.terms;
          response.json(HTTP_CODE.OK, response_json);
        } else {
          ApiError(response, 'NOT_FOUND', 'Nothing to count');
        }
      } else if (body.facets.count.entries) {
        // Date facet count
        if (body.facets.count.entries.length != 0) {
          for (i = 0; i < body.facets.count.entries.length; i++) {
            var day = moment(body.facets.count.entries[i].time);
            body.facets.count.entries[i].time = day.format('YYYYMMDD');
          }
          response_json.results = body.facets.count.entries;
          response.json(HTTP_CODE.OK, response_json);
        } else {
          ApiError(response, 'NOT_FOUND', 'Nothing to count');
        }
      } else {
        ApiError(response, 'NOT_FOUND', 'Nothing to count');
      }
    } else {
      ApiError(response, 'NOT_FOUND', 'No matches found!');
    }
  }, function(error) {
    log.error(error);
    ApiError(response, 'SERVER_ERROR', 'Check your request and try again');
  });
};

EnforcementEndpoint = function(noun) {
  app.get('/' + noun + '/enforcement.json', function(request, response) {
    LogRequest(request);
    SetHeaders(response);

    var params = TryToCheckApiParams(request, response);
    if (params == null) {
      return;
    }

    var product_type_filter = 'product_type:'
    if (noun == 'drug' || noun == 'device') {
      product_type_filter += noun + 's';
    } else if (noun == 'food') {
      product_type_filter += noun;
    }

    if (params.search == undefined) {
      params.search = product_type_filter;
    } else {
      params.search += ' AND ' + product_type_filter;
    }

    var index = ALL_ENFORCEMENT_INDEX;
    var es_search_params =
      TryToBuildElasticsearchParams(params, index, response);
    if (es_search_params == null) {
      return;
    }

    TrySearch(index, params, es_search_params, response);
  });
};
EnforcementEndpoint('drug');
EnforcementEndpoint('food');
EnforcementEndpoint('device');

app.get('/drug/event.json', function(request, response) {
  LogRequest(request);
  SetHeaders(response);

  var params = TryToCheckApiParams(request, response);
  if (params == null) {
    return;
  }

  var index = DRUG_EVENT_INDEX;
  var es_search_params =
    TryToBuildElasticsearchParams(params, index, response);
  if (es_search_params == null) {
    return;
  }

  TrySearch(index, params, es_search_params, response);
});

// From http://strongloop.com/strongblog/
// robust-node-applications-error-handling/
if (process.env.NODE_ENV === 'production') {
  process.on('uncaughtException', function(e) {
    log.error(e);
    process.exit(1);
  });
}

var port = process.env.PORT || 8000;
app.listen(port, function() {
  console.log('Listening on ' + port);
});
