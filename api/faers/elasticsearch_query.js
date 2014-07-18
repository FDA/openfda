// Elasticsearch Query Builder

var ejs = require('elastic.js');

var ELASTICSEARCH_QUERY_ERROR = 'ElasticsearchQueryError';

// Supported characters:
// all letters and numbers
// . for long.field.names
// _ for other_fields
// : for fields
// ( ) for grouping
// " for quoting
// [ ] and { } for ranges
// >, < and = for ranges
// - for dates and boolean
// + for boolean
// space for terms
var SUPPORTED_QUERY_RE = '^[0-9a-zA-Z\.\_\:\(\)\"\\[\\]\{\}\\-\\+\>\<\= ]+$';

var DATE_FIELDS = [
  // FAERS
  'drugstartdate',
  'drugenddate',
  'patient.patientdeath.patientdeathdate',
  'receiptdate',
  'receivedate',
  'transmissiondate',

  // RES
  'report_date',
  'recall_initiation_date'
];

exports.SupportedQueryString = function(query) {
  var supported_query_re = new RegExp(SUPPORTED_QUERY_RE);
  return supported_query_re.test(query);
};

// For the openfda section, we have field_exact rather than field.exact stored
// in elasticsearch.
exports.ReplaceExact = function(search_or_count) {
  return search_or_count.replace(/openfda\.([\w\.]+).exact/g,
    'openfda.$1_exact');
};

exports.BuildQuery = function(params) {
  q = ejs.Request();

  if (!params.search && !params.count) {
    q.query(ejs.MatchAllQuery());
  }

  if (params.search) {
    if (!exports.SupportedQueryString(params.search)) {
      throw {
        name: ELASTICSEARCH_QUERY_ERROR,
        message: 'Search not supported: ' + params.search
      };
    }
    q.query(ejs.QueryStringQuery(exports.ReplaceExact(params.search)));
  }

  if (params.count) {
    if (DATE_FIELDS.indexOf(params.count) != -1) {
      q.facet(ejs.DateHistogramFacet('count').
        field(params.count).interval('day').order('time'));
    } else {
      var limit = parseInt(params.limit);
      q.facet(ejs.TermsFacet('count').
        fields([exports.ReplaceExact(params.count)]).size(limit));
    }
  }

  return q;
};
