// OpenFDA APIs
//

var ejs = require('elastic.js');
var elasticsearch = require('elasticsearch');
var express = require('express');
var moment = require('moment');
var underscore = require('underscore');
var Stats = require('fast-stats').Stats;

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
  '@case_number',
  '@version',

  // MAUDE fields to remove
  'baseline_510_k_exempt_flag',
  'baseline_510_k_flag',
  'baseline_510_k_number',
  'baseline_brand_name',
  'baseline_catalog_number',
  'baseline_date_ceased_marketing',
  'baseline_date_first_marketed',
  'baseline_device_family',
  'baseline_generic_name',
  'baseline_model_number',
  'baseline_other_id_number',
  'baseline_pma_flag',
  'baseline_pma_number',
  'baseline_preamendment_flag',
  'baseline_shelf_life_contained',
  'baseline_shelf_life_in_months',
  'baseline_transitional_flag'
];


var ALL_ENFORCEMENT_INDEX = 'recall';
var DEVICE_EVENT_INDEX = 'deviceevent';
var DRUG_EVENT_INDEX = 'drugevent';
var DRUG_LABEL_INDEX = 'druglabel';
var PROCESS_METADATA_INDEX = 'openfdametadata';

var INDICES = {
  'enforcement' : ALL_ENFORCEMENT_INDEX,
  'device' : DEVICE_EVENT_INDEX,
  'drug' : DRUG_EVENT_INDEX,
  'label' : DRUG_LABEL_INDEX
};

// For each endpoint, we track the success/failure status for the
// last REQUEST_HISTORY_LENGTH requests; this is used to determine
// the green/yellow/red status for an endpoint.
var REQUEST_HISTORY_LENGTH = 100;

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


var index_info = {};
for (var name in INDICES) {
  index_info[INDICES[name]] = {
    last_updated: '2015-01-01',
    document_count: 1,
    latency: new Stats({ bucket_precision: 10, store_data: false }),
    status: []
  };
};

// Fetch index update times
client.search({
  index: PROCESS_METADATA_INDEX,
  type: 'last_run',
  fields: 'last_update_date'
}).then(function(body) {
  var util = require('util');
  for (var i = 0; i < body.hits.hits.length; ++i) {
    var hit = body.hits.hits[i];
    index_info[hit._id].last_updated = hit.fields.last_update_date[0];
  }
}, function (error) {
  console.log('Failed to fetch index update times:: ', error.message);
});

// Fetch document counts
for (var name in INDICES) {
  var index = INDICES[name];
  client
    .count({ index: index })
    .then(function(index, body) {
    index_info[index].document_count = body.count;
  }.bind(null, index));
}


// Returns a JSON response indicating the status of each endpoint. The status
// includes the last time the index was updated, a green/yellow/red "status"
// field indicating the recent health of the endpoint, the number of requests to
// the endpoint, and the average latency of the endpoint in milliseconds.
app.get('/status', function(request, response) {
  // Allow the status page to get status info....
  response.setHeader('Access-Control-Allow-Origin', '*');

  var res = [];
  for (var name in INDICES) {
    var index = INDICES[name];
    var info = index_info[index];
    var errorCount = 0;
    for (var i = 0; i < info.status.length; ++i) {
      errorCount += info.status[i] == false ? 1 : 0;
    }

    var status = 'GREEN';
    if (errorCount > REQUEST_HISTORY_LENGTH / 50) {
      status = 'YELLOW';
    }
    if (errorCount > REQUEST_HISTORY_LENGTH / 10) {
      status = 'RED';
    }

    var requestCount = 0;
    var buckets = info.latency.distribution();
    for (var i = 0; i < buckets.length; ++i) {
      if (!buckets[i]) { continue; }
      requestCount += buckets[i].count;
    }

    res.push({
      endpoint: index,
      status: status,
      last_updated: info.last_updated,
      documents: info.document_count,
      requests: requestCount,
      latency: info.latency.amean(),
    });
  }
  response.json(res);
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

  var index = elasticsearch_index;
  if (params.staging) {
    index = elasticsearch_index + '.staging';
  }
  // Added sort by _id to ensure consistent results
  // across servers
  var es_search_params = {
    index: elasticsearch_index,
    body: es_query.toString(),
    sort: '_uid'
  };

  if (!params.count) {
    es_search_params.from = params.skip;
    es_search_params.size = params.limit;
  }

  return es_search_params;
};

AddRequestStatus = function(index, success) {
  var info = index_info[index];
  info.status.push(success);
  if (info.status.length > REQUEST_HISTORY_LENGTH) {
    info.status.shift();
  }
};

TrySearch = function(index, params, es_search_params, response) {
  client.search(es_search_params).then(function(body) {
    AddRequestStatus(index, true);
    if (body.hits.hits.length == 0) {
      return ApiError(response, 'NOT_FOUND', 'No matches found!');
    }

    var requestTime = body.took;
    index_info[index].latency.push(requestTime);

    var response_json = {};
    response_json.meta = underscore.clone(META);
    // TODO(hansnelsen): Need to check this periodically (TTL) when we switch to
    //                   an always-on approach to Elasticsearch and the API
    response_json.meta.last_updated = index_info[index].last_updated;

    if (!params.count) {
      response_json.meta.results = {
        'skip': params.skip,
        'limit': params.limit,
        'total': body.hits.total
      };

      response_json.results = [];
      for (i = 0; i < body.hits.hits.length; i++) {
        var result = body.hits.hits[i]._source;
        for (j = 0; j < FIELDS_TO_REMOVE.length; j++) {
          delete result[FIELDS_TO_REMOVE[j]];

          // For MAUDE. TODO(mattmo): Refactor
          var device = result.device;
          if (device) {
            for (k = 0; k < device.length; k++) {
              delete device[k][FIELDS_TO_REMOVE[j]];
            }
          }
        }
        response_json.results.push(result);
      }
      response.json(HTTP_CODE.OK, response_json);

    } else if (params.count) {
      if (body.facets.count.terms) {
        // Term facet count
        if (body.facets.count.terms.length != 0) {
          response_json.results = body.facets.count.terms;
          response.json(HTTP_CODE.OK, response_json);
        } else {
          return ApiError(response, 'NOT_FOUND', 'Nothing to count');
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
          return ApiError(response, 'NOT_FOUND', 'Nothing to count');
        }
      } else {
        return ApiError(response, 'NOT_FOUND', 'Nothing to count');
      }
    } else {
      return ApiError(response, 'NOT_FOUND', 'No matches found!');
    }
  }, function(error) {
    log.error(error);
    AddRequestStatus(index, false);
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

app.get('/drug/label.json', function(request, response) {
  LogRequest(request);
  SetHeaders(response);

  var params = TryToCheckApiParams(request, response);
  if (params == null) {
    return;
  }

  var index = DRUG_LABEL_INDEX;
  var es_search_params =
    TryToBuildElasticsearchParams(params, index, response);
  if (es_search_params == null) {
    return;
  }

  TrySearch(index, params, es_search_params, response);
});

app.get('/device/event.json', function(request, response) {
  LogRequest(request);
  SetHeaders(response);

  var params = TryToCheckApiParams(request, response);
  if (params == null) {
    return;
  }

  var index = DEVICE_EVENT_INDEX;
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
