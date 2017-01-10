// OpenFDA APIs
//

var elasticsearch = require('elasticsearch');
var express = require('express');
var moment = require('moment');
var _ = require('underscore');
var request = require('request');
var querystring = require('querystring');
var url = require('url');
var cache = require('apicache').middleware;

var Stats = require('fast-stats').Stats;

var api_request = require('./api_request.js');
var elasticsearch_query = require('./elasticsearch_query.js');
var logging = require('./logging.js');

// This META object is duplicated in the python export code. If this changes,
// please update the openfda/index_util.py as well.
var META = {
  'disclaimer': 'Do not rely on openFDA to make decisions regarding medical care. ' +
                'While we make every effort to ensure that data is accurate, you ' +
                'should assume all results are unvalidated. We may limit or otherwise ' +
                'restrict your access to the API in line with our Terms of Service.',
  'terms': 'https://open.fda.gov/terms/',
  'license': 'https://open.fda.gov/license/',
  'last_updated': '2014-05-29'
};

// TODO(hansnelsen): If there are more bespoke disclaimers, externalize this
//                   text in a yaml that maps the text to an index.
var CAERS_DISCLAIMER = 'Do not rely on openFDA to make decisions regarding ' +
  'medical care. While we make every effort to ensure that data is accurate, ' +
  'you should assume all results are unvalidated. We may limit or otherwise ' +
  'restrict your access to the API in line with our Terms of Service. ' +
  'Submission of an adverse event report does not constitute an admission ' +
  'that a product caused or contributed to an event. The information in ' +
  'these reports has not been scientifically or otherwise verified as to a ' +
  'cause and effect relationship and cannot be used to estimate incidence ' +
  '(occurrence rate) or to estimate risk.'

var HTTP_CODE = {
  OK: 200,
  BAD_REQUEST: 400,
  NOT_FOUND: 404,
  SERVER_ERROR: 500
};

// Internal fields to remove from ES drugevent objects before serving
// via the API.
var FIELDS_TO_REMOVE = [
  '@checksum',
  '@id',
  '@timestamp',
  '@case_number',
  '@version',
  '@epoch',

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


// TODO(hansnelsen): all these index data structures are becoming too much,
//                   consolidate down to one and refactor the code accordingly.
var ALL_ENFORCEMENT_INDEX = 'recall';
var DEVICE_EVENT_INDEX = 'deviceevent';
var DEVICE_CLASSIFICATION_INDEX = 'deviceclass';
var DEVICE_REGISTRATION_INDEX = 'devicereglist';
var DEVICE_CLEARANCE_INDEX = 'deviceclearance';
var DEVICE_PMA_INDEX = 'devicepma';
var DEVICE_RECALL_INDEX = 'devicerecall';
var DEVICE_UDI_INDEX = 'deviceudi';
var DRUG_EVENT_INDEX = 'drugevent';
var DRUG_LABEL_INDEX = 'druglabel';
var FOOD_EVENT_INDEX = 'foodevent';
var PROCESS_METADATA_INDEX = 'openfdametadata';
var EXPORT_DATA_INDEX = 'openfdadata';

// This data structure is the standard way to add an endpoint to the api, which
// is to say, if there is a one-to-one mapping between an index and an endpoint,
// use this data structure.
// If there is a one-to-many relationship, then a custom function should be
// built, such as EnforcementEndpoint, which allows the splitting of one index
// (recall) across three endpoints by using a filtered search.
//
// {
//   'index': DEVICE_EVENT_INDEX,
//   'endpoint': 'device',
//   'name': 'deviceevent'
// }
// Results in an endpoint: /device/event.json hitting /deviceevent/maude index.

var ENDPOINTS = [
  {
    'index': DEVICE_EVENT_INDEX,
    'endpoint': '/device/event.json',
    'name': 'deviceevent',
    'basic' : true
  },
  {
    'index': DEVICE_RECALL_INDEX,
    'endpoint': '/device/recall.json',
    'name': 'devicerecall',
    'basic' : true
  },
  {
    'index': DEVICE_CLASSIFICATION_INDEX,
    'endpoint': '/device/classification.json',
    'name': 'deviceclass',
    'basic' : true
  },
  {
    'index': DEVICE_REGISTRATION_INDEX,
    'endpoint': '/device/registrationlisting.json',
    'name': 'devicereglist',
    'basic' : true
  },
  {
    'index': DEVICE_CLEARANCE_INDEX,
    'endpoint': '/device/510k.json',
    'name': 'deviceclearance',
    'basic' : true
  },
  {
    'index': DEVICE_PMA_INDEX,
    'endpoint': '/device/pma.json',
    'name': 'devicepma',
    'basic' : true
  },
  {
    'index': DEVICE_UDI_INDEX,
    'endpoint': '/device/udi.json',
    'name': 'deviceudi',
    'basic' : true
  },
  {
    'index': DRUG_EVENT_INDEX,
    'endpoint': '/drug/event.json',
    'name': 'drugevent',
    'basic' : true
  },
  {
    'index': DRUG_LABEL_INDEX,
    'endpoint': '/drug/label.json',
    'name': 'druglabel',
    'basic' : true
  },
  {
    'index': DRUG_LABEL_INDEX,
    'endpoint': '/drug/label.json',
    'name': 'druglabel',
    'basic' : true
  },
  {
    'index': FOOD_EVENT_INDEX,
    'endpoint': '/food/event.json',
    'name': 'foodevent',
    'basic' : true
  },
  {
    'index': ALL_ENFORCEMENT_INDEX,
    'endpoint': '/drug/enforcement.json',
    'name': 'recall'
  },
  {
    'index': ALL_ENFORCEMENT_INDEX,
    'endpoint': '/device/enforcement.json',
    'name': 'recall'
  },
  {
    'index': ALL_ENFORCEMENT_INDEX,
    'endpoint': '/food/enforcement.json',
    'name': 'recall'
  },
  {
    'index': EXPORT_DATA_INDEX,
    'endpoint': '/download.json',
    'name': 'openfdadata',
    'download': true
  }
];

var VALID_URLS = [
  'api.fda.gov/drug/',
  'api.fda.gov/food/',
  'api.fda.gov/device/',
  'api.fda.gov/drug/event.json',
  'api.fda.gov/drug/label.json',
  'api.fda.gov/drug/enforcement.json',
  'api.fda.gov/device/510k.json',
  'api.fda.gov/device/event.json',
  'api.fda.gov/device/enforcement.json',
  'api.fda.gov/device/udi.json',
  'api.fda.gov/device/recall.json',
  'api.fda.gov/device/classification.json',
  'api.fda.gov/device/registrationlisting.json',
  'api.fda.gov/device/pma.json',
  'api.fda.gov/food/enforcement.json',
  'api.fda.gov/food/event.json'
];

// For each endpoint, we track the success/failure status for the
// last REQUEST_HISTORY_LENGTH requests; this is used to determine
// the green/yellow/red status for an endpoint.
var REQUEST_HISTORY_LENGTH = 10;


// Set caching headers for Amazon Cloudfront
CacheMiddleware = function(seconds) {
  return function(request, response, next) {
    response.setHeader('Cache-Control', 'public, max-age=' + seconds);
    return next();
  };
};

// Tracks the health and latency of ES indices.
var index_info = {};

// Fetch index counts and last update times.
//
// This is run at startup and periodically during operation; the results
// are returned with search requests and via the status API.
var UpdateIndexInformation = function(client, index_info) {
  console.log("Updating index information.");
  client.search({
    index: PROCESS_METADATA_INDEX,
    type: 'last_run',
    fields: 'last_update_date'
  }).then(function(body) {
    var util = require('util');
    for (var i = 0; i < body.hits.hits.length; ++i) {
      var hit = body.hits.hits[i];
      var id = hit._id;
      index_info[id].last_updated = hit.fields.last_update_date[0];
    }
  }, function (error) {
    console.log('Failed to fetch index update times:: ', error.message);
  });

  // Fetch document counts
  _.map(ENDPOINTS, function(endpoint) {
    client
      .count({ index: endpoint.index })
      .then(function(endpoint, body) {
        index_info[endpoint.index].document_count = body.count;
      }.bind(null, endpoint));
  });
};

var TestAvailability = function() {
  _.map(ENDPOINTS, function(endpoint) {
    request(endpoint.endpoint, function(error, response, body) {
      var info = index_info[endpoint.index];
      info.status.push(!error);
    });
  });
};

var ErrorTypes = {
  NOT_FOUND: 'NOT_FOUND',
  SERVER_ERROR: 'SERVER_ERROR'
}

var app = express();
app.disable('x-powered-by');
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

// Initialize our index information.  This is returned by the status API.
_.map(ENDPOINTS, function(endpoint) {
  index_info[endpoint.index] = {
    last_updated: '2015-01-01',
    document_count: 1,
    latency: new Stats({ bucket_precision: 10, store_data: false }),
    status: []
  };
});


UpdateIndexInformation(client, index_info);

// Check API availability periodically
setInterval(UpdateIndexInformation.bind(null, client, index_info),
            60 * 1000 /* 1 minute */);

// Returns a JSON response indicating the status of each endpoint. The status
// includes the last time the index was updated, a green/yellow/red "status"
// field indicating the recent health of the endpoint, the number of requests to
// the endpoint, and the average latency of the endpoint in milliseconds.
app.get('/status', function(request, response) {
  // Allow the status page to get status info....
  response.setHeader('Access-Control-Allow-Origin', '*');

  var res = {};
  _.map(ENDPOINTS, function(endpoint) {
    var index = endpoint.index;
    var info = index_info[index];
    var errorCount = 0;
    for (var i = 0; i < info.status.length; ++i) {
      errorCount += info.status[i] == false ? 1 : 0;
    }

    var status = 'GREEN';
    if (errorCount > 1) {
      status = 'YELLOW';
    }
    if (errorCount > 3) {
      status = 'RED';
    }

    var requestCount = 0;
    var buckets = info.latency.distribution();
    for (var i = 0; i < buckets.length; ++i) {
      if (!buckets[i]) { continue; }
      requestCount += buckets[i].count;
    }

    res[index] = {
      endpoint: index, // yes, this should be endpoint, kept for compatibility with status page
      status: status,
      last_updated: info.last_updated,
      documents: info.document_count,
      requests: requestCount,
      latency: info.latency.amean(),
    };
  });

  response.json(_.values(res));
});

// endpoint for the API statistics page - cached in memory for 1 hour.
app.get('/usage.json', cache('1 hour'), function(req, res) {

  var end_at = req.query.start_at|| moment().format("YYYY-MM-DD");
  var start_at = req.query.end_at|| moment().subtract(30, 'day').format("YYYY-MM-DD");
  var prefix = req.query.prefix || '0/';
  var params = querystring.stringify({
      start_at: start_at,
      end_at: end_at,
      interval: 'day',
      prefix: prefix,
      query: {"condition":"AND","rules":[{"field":"gatekeeper_denied_code","id":"gatekeeper_denied_code","input":"select" ,"operator":"is_null","type":"string","value":null}]}
    });

  //NEVER expose this key to public
  var options = {
    method: "GET",
    url: "https://api.data.gov/api-umbrella/v1/analytics/drilldown.json?" + params,
    headers: {
      "X-Api-Key": process.env.API_UMBRELLA_KEY,
      "X-Admin-Auth-Token": process.env.API_UMBRELLA_ADMIN_TOKEN
    }
  }


  request(options, function (error, response, body) {
      usage = {
        table:[],
        stats :[],
        others:[],
        lastThirtyDayUsage: 0,
        indexInfo:{}
      };
      _.each(ENDPOINTS, function(endpoint) {
          var info = index_info[endpoint.index];
          usage.indexInfo[endpoint.name] = info.document_count || 10;
      });

      if (!error && response.statusCode == 200) {
          var data = JSON.parse(body);
          if (data.results) {
            var unwanted = 0;
            _.map(data.results, function(result){
                if (VALID_URLS.indexOf(result.path) > -1) {
                  usage.table.push(result);
                } else {
                  unwanted+= result.hits;
                  usage.others.push(result);
                }

            });

            if (unwanted > 0) {
              usage.table.push({
                "depth": 1,
                "path": "others",
                "terminal": true,
                "descendent_prefix": "2/api.fda.gov/drug/",
                "hits": unwanted
              });
            }

            _.each(usage.table, function(row){
              usage.lastThirtyDayUsage += row.hits;
            });

          }
          if (data.hits_over_time) {

              _.each(data.hits_over_time.rows, function(row){

                var stat = {totalCount: 0, paths:[]};
                usage.stats.push(stat);

                for (var i =0; i < row.c.length; i++) {
                  if (i === 0) {
                   stat.day = row.c[i].f;
                  } else {
                    stat.totalCount += row.c[i].v;
                    stat.paths.push({path: data.hits_over_time.cols[i].label, count: row.c[i].v});
                  }
                }

              });
          }
      } else {
        log.error(error);
        log.error("The response is :");
        log.error(response);
      }
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader('Cache-Control', 'public, max-age=' + 43200); //cache for 12 hours
      res.json(usage);

  });

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

TryToBuildElasticsearchParams = function(params, es_index, response) {
  try {
    var es_query = elasticsearch_query.BuildQuery(params);
    log.info(es_query, 'Elasticsearch Query');
  } catch (e) {
    log.error(e);
    if (e.name == elasticsearch_query.ELASTICSEARCH_QUERY_ERROR) {
      ApiError(response, 'BAD_REQUEST', e.message);
    } else {
      ApiError(response, 'BAD_REQUEST', '');
    }
    return null;
  }

  var index = es_index;
  if (params.staging) {
    index = es_index + '.staging';
  }
  // Added sort by _id to ensure consistent results
  // across servers
  var es_search_params = {
    index: es_index,
    body: es_query,
    sort: '_uid'
  };

  if (!params.count) {
    es_search_params.from = params.skip;
    es_search_params.size = params.limit;
  }

  return es_search_params;
};

TrySearch = function(index, params, es_search_params, response) {
  client.search(es_search_params)
  .then(function(body) {
    if (body.hits.hits.length == 0) {
      return ApiError(response, ErrorTypes.NOT_FOUND, 'No matches found!');
    }

    var requestTime = body.took;
    index_info[index].latency.push(requestTime);

    var response_json = {};
    response_json.meta = _.clone(META);

    if (index === 'foodevent') {
      response_json.meta.disclaimer = CAERS_DISCLAIMER
    }

    response_json.meta.last_updated = index_info[index].last_updated;

    // Search query
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
      return response.json(HTTP_CODE.OK, response_json);
    }

    // Count query
    if (body.facets.count.terms) {
      // Term facet count
      if (body.facets.count.terms.length == 0) {
        return ApiError(response, ErrorTypes.NOT_FOUND, 'Nothing to count');
      }

      // We add 1000 to the limit on all count queries in order to overcome
      // inaccurate results in the tail of the result, as such, we need to lop
      // off any results beyond the amount requested in the params.limit.
      var count_result = body.facets.count.terms;
      response_json.results = (count_result.length > params.limit) ?
                               count_result.slice(0, params.limit) :
                               count_result;
      return response.json(HTTP_CODE.OK, response_json);
    }

    // Date facet count
    if (body.facets.count.entries) {
      if (body.facets.count.entries.length == 0) {
        return ApiError(response, ErrorTypes.NOT_FOUND, 'Nothing to count');
      }

      for (i = 0; i < body.facets.count.entries.length; i++) {
        var day = moment.utc(body.facets.count.entries[i].time);
        body.facets.count.entries[i].time = day.format('YYYYMMDD');
      }

      response_json.results = body.facets.count.entries;
      return response.json(HTTP_CODE.OK, response_json);
    }

    return ApiError(response, ErrorTypes.NOT_FOUND, 'Nothing to count');
  }, function(error) {
    log.error(error);
    ApiError(response, ErrorTypes.SERVER_ERROR, 'Check your request and try again');
  });
};

GetDownload = function(response) {
  console.log("Getting download information...");
  var index = EXPORT_DATA_INDEX;

  client.get({
    index: index,
    type: 'downloads',
    id: 'current',
  }).then(function(body) {
    if (!body) {
      return ApiError(response, ErrorTypes.NOT_FOUND, 'No matches found!');
    }

    var response_json = {};

    response_json.meta = _.clone(META);
    response_json.meta.last_updated = index_info[index].last_updated;

    response_json.results = body._source ? _.clone(body._source) : {};

    return response.json(HTTP_CODE.OK, response_json);

  }, function (error) {
    console.log('Failed to fetch index update times:: ', error.message);
    ApiError(response, ErrorTypes.SERVER_ERROR, 'Check your request and try again');
  });
};


EnforcementEndpoint = function(noun) {
  app.get('/' + noun + '/enforcement.json', function(request, response) {
    LogRequest(request);
    SetHeaders(response);

    var params = TryToCheckApiParams(request, response);
    if (params === null) {
      return;
    }

    var product_type_filter = 'product_type:';
    if (noun == 'drug' || noun == 'device') {
      product_type_filter += noun + 's';
    } else if (noun == 'food') {
      product_type_filter += noun;
    }

    if (!params.search) {
      params.search = product_type_filter;
    } else {
      params.search += ' AND ' + product_type_filter;
    }

    var index = ALL_ENFORCEMENT_INDEX;
    var es_search_params =
      TryToBuildElasticsearchParams(params, index, response);
    if (es_search_params === null) {
      return;
    }

    TrySearch(index, params, es_search_params, response);
  });
};

EnforcementEndpoint('drug');
EnforcementEndpoint('food');
EnforcementEndpoint('device');

BasicEndpoint = function(data) {
  var endpoint = data['endpoint'];
  var index = data['index'];

  app.get(endpoint, function(request, response) {
    LogRequest(request);
    SetHeaders(response);

    var params = TryToCheckApiParams(request, response);
    if (params === null) {
      return;
    }

    var es_search_params =
      TryToBuildElasticsearchParams(params, index, response);
    if (es_search_params === null) {
      return;
    }

    TrySearch(index, params, es_search_params, response);
  });
};

// Make all of the basic endpoints
_.map(ENDPOINTS, function(endpoint) {
  if (endpoint.basic) {
    BasicEndpoint(endpoint);
  }
});

// Take endpoint config object and a prune boolean. If boolean is true, then
// we only want the specific endpoint's download data.
DownloadEndpoint = function(data, prune) {
  var endpoint = data['endpoint'];
  var index = data['index'];

  app.get(endpoint, function(request, response) {
    LogRequest(request);
    SetHeaders(response);

    GetDownload(response);
  });
};

// Make the download endpoint
_.map(ENDPOINTS, function(endpoint) {
  if (endpoint.download) {
    DownloadEndpoint(endpoint, false);
  }
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
