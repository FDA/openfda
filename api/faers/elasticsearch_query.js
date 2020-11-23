// Elasticsearch Query Builder

var ejs = require('elastic.js');
var escape = require('escape-html');
const qs = require('qs')
const _ = require('underscore');
const client = require('./elasticsearch_client').client;
const jsonpath = require('jsonpath');
var ELASTICSEARCH_QUERY_ERROR = 'ElasticsearchQueryError';
const logging = require('./logging.js');
const log = logging.GetLogger();
const NodeCache = require( "node-cache" );
const fieldMappingCache = new NodeCache();

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
// @ for internal fields.
var SUPPORTED_QUERY_RE = '^[0-9a-zA-Z@%/\'\,\.\_\:\(\)\"\\[\\]\{\}\\-\\+\>\<\= ]+$';

var DATE_FIELDS = [
  // FAERS
  'drugstartdate',
  'drugenddate',
  'patient.patientdeath.patientdeathdate',
  'patientdeathdate',
  'receiptdate',
  'receivedate',
  'transmissiondate',

  // RES
  'report_date',
  'recall_initiation_date',
  'center_classification_date',
  'termination_date',

  // SPL
  'effective_time',

  // MAUDE
  'date_facility_aware',
  'date_manufacturer_received',
  'date_of_event',
  'date_received',
  'date_added',
  'date_changed',
  'date_report',
  'date_report_to_fda',
  'date_report_to_manufacturer',
  'date_returned_to_manufacturer',
  'device_date_of_manufacture',
  'baseline_date_ceased_marketing',
  'baseline_date_first_marketed',
  'expiration_date_of_device',
  'device_date_of_manufacturer',


  // R&L
  'products.created_date',

  // Device Recall
  'event_date_terminated',

  // Device PMA
  'decision_date',
  'fed_reg_notice_date',

  // Device UDI
  'package_discontinue_date',
  'publish_date',
  'public_version_date',
  'commercial_distribution_end_date',
  'identifiers.package_discontinue_date',
  'package_discontinue_date',

  // ADAE
  'original_receive_date',
  'onset_date',
  'first_exposure_date',
  'last_exposure_date',
  'manufacturing_date',
  'lot_expiration',
  'drug.first_exposure_date',
  'drug.last_exposure_date',
  'drug.manufacturing_date',
  'drug.lot_expiration',

  // Food Events
  'date_created',
  'date_started',

  // NSDE
  'marketing_start_date',
  'marketing_end_date',
  'inactivation_date',
  'reactivation_date',

  // NDC
  'marketing_start_date',
  'marketing_end_date',
  'listing_expiration_date',

  // Drugs@FDA
  'submissions.submission_status_date',
  'submissions.application_docs.date',

  // Serology
  'date_performed',

  //Tobacco Problem
  'date_submitted'

];

// Fields which should be rewritten from field.exact to field_exact
EXACT_FIELDS = [
  'establishment_type',
  'proprietary_name',
  'openfda.dosage_form',
  'openfda.rxcui',
  'openfda.package_ndc',
  'openfda.rxstring',
  'openfda.nui',
  'openfda.original_packager_product_ndc',
  'openfda.pharm_class_pe',
  'openfda.route',
  'openfda.product_ndc',
  'openfda.unii',
  'openfda.product_type',
  'openfda.pharm_class_moa',
  'openfda.pharm_class_cs',
  'openfda.application_number',
  'openfda.is_original_packager',
  'openfda.upc',
  'openfda.pharm_class_epc',
  'openfda.manufacturer_name',
  'openfda.rxtty',
  'openfda.spl_id',
  'openfda.brand_name',
  'openfda.spl_set_id',
  'openfda.generic_name',
  'openfda.substance_name',
  'openfda.dosage_form',
  'openfda.rxcui',
  'openfda.package_ndc',
  'openfda.rxstring',
  'openfda.nui',
  'openfda.original_packager_product_ndc',
  'openfda.pharm_class_pe',
  'openfda.route',
  'openfda.product_ndc',
  'openfda.unii',
  'openfda.pharm_class_moa',
  'openfda.product_type',
  'openfda.pharm_class_cs',
  'openfda.pharm_class_epc',
  'openfda.application_number',
  'openfda.is_original_packager',
  'openfda.upc',
  'openfda.rxtty',
  'openfda.manufacturer_name',
  'openfda.spl_id',
  'openfda.brand_name',
  'openfda.spl_set_id',
  'openfda.generic_name',
  'openfda.substance_name',
  'type_of_report',
  'patient.sequence_number_outcome',
  'remedial_action',
  'source_type',
  'patient.sequence_number_treatment',
  'patient.drug.openfda.brand_name',
  'patient.drug.openfda.rxstring',
  'patient.drug.openfda.pharm_class_pe',
  'patient.drug.openfda.manufacturer_name',
  'patient.drug.openfda.rxcui',
  'patient.drug.openfda.generic_name',
  'patient.drug.openfda.application_number',
  'patient.drug.openfda.nui',
  'patient.drug.openfda.substance_name',
  'patient.drug.openfda.product_ndc',
  'patient.drug.openfda.spl_id',
  'patient.drug.openfda.dosage_form',
  'patient.drug.openfda.pharm_class_cs',
  'patient.drug.openfda.package_ndc',
  'patient.drug.openfda.rxtty',
  'patient.drug.openfda.pharm_class_moa',
  'patient.drug.openfda.spl_set_id',
  'patient.drug.openfda.route',
  'patient.drug.openfda.product_type',
  'patient.drug.openfda.unii',
  'patient.drug.openfda.pharm_class_epc',
  'outcomes', //  Food Event
  'reactions'
];

const SORTABLE_FIELD_TYPES = ['keyword', 'short', 'integer', 'byte', 'date']

exports.ELASTICSEARCH_QUERY_ERROR = ELASTICSEARCH_QUERY_ERROR;

exports.SupportedQueryString = function(query) {
  var supported_query_re = new RegExp(SUPPORTED_QUERY_RE);
  return supported_query_re.test(query);
};

// For the openfda section, we have field_exact rather than field.exact stored
// in elasticsearch.
exports.ReplaceExact = function(search_or_count) {
  for (i = 0; i < EXACT_FIELDS.length; i++) {
    var field = EXACT_FIELDS[i];
    var field_before = field + '.exact';
    var field_after = field + '_exact';
    search_or_count = search_or_count.replace(new RegExp(field_before, 'g'),
      field_after);
  }

  return search_or_count;
};

// _missing_ is gone from Elasticsearch 5 and up, but we still have to support it in the API.
// We are replacing _missing_ with negated _exists_ behind the scenes.
exports.HandleDeprecatedClauses = function(search) {
    return search.replace(/_missing_:("?[\w.]+"?)/g, "(NOT (_exists_:$1))");
};

exports.BuildSort = async function(params, index) {
    var sort = '';
    if (params.sort) {
        params.sort = params.sort.trim()
        if (!exports.SupportedQueryString(params.sort)) {
            throw {
                name: ELASTICSEARCH_QUERY_ERROR,
                message: 'Sort not supported: ' + escape(params.sort)
            };
        }
      if (params.sort.indexOf('exact') == -1
        && !DATE_FIELDS.find(f => params.sort.split(':')[0].endsWith(f))
        && ! await isSortableField(params.sort.split(':')[0], index)) {
        throw {
          name: ELASTICSEARCH_QUERY_ERROR,
          message: 'Sorting allowed by non-analyzed fields only: ' + escape(params.sort.split(':')[0])
        };
      }
        sort = exports.ReplaceExact(params.sort);
    }

  return sort ? sort + ',_uid' : '_uid';
};

async function isSortableField(field, index) {
  if (index && field) {
    const cacheKey = `${index}.${field}`;
    if (!fieldMappingCache.has(cacheKey)) {
      fieldMappingCache.set(cacheKey, await isFieldTypeAcceptableForSorting(field, index));
    }
    return fieldMappingCache.get(cacheKey);
  }
  return false;
}

async function isFieldTypeAcceptableForSorting(field, index) {
  try {
    const mappings = jsonpath.query(await client.indices.getFieldMapping({
      index: index,
      local: true,
      fields: field
    }), '$..mapping.*');
    if (mappings.length > 0) {
      const fieldType = mappings[0].type;
      if (fieldType && SORTABLE_FIELD_TYPES.includes(fieldType)) {
        return true;
      }
    }
  } catch (e) {
    log.error(e);
    fieldMappingCache.flushAll();
  }
  return false;
}

var AddSearchAfter = function (ejsBody, params) {
  if (params.search_after) {
    ejsBody.search_after = _.values(qs.parse(params.search_after, {delimiter: ';'}))
  }
};

exports.BuildQuery = function (params) {
  const q = ejs.Request();

  if (!params.search && !params.count) {
    q.query(ejs.MatchAllQuery());
  } else {
    if (params.search) {
      if (!exports.SupportedQueryString(params.search)) {
        throw {
          name: ELASTICSEARCH_QUERY_ERROR,
          message: 'Search not supported: ' + escape(params.search)
        };
      }
      q.query(ejs.QueryStringQuery(exports.HandleDeprecatedClauses(exports.ReplaceExact(params.search))));
    }

    if (params.count) {
      if (DATE_FIELDS.indexOf(params.count) != -1) {
        //q.facet(ejs.DateHistogramFacet('count').
        //  field(params.count).interval('day').order('time'));
        q.agg(ejs.DateHistogramAggregation('histogram').field(params.count).interval('day')
          .order('_key', 'asc').format('yyyyMMdd').minDocCount(1));
      } else {
        // Adding 1000 extra to limit since we are using estimates rather than
        // actual counts. It turns out that the tail of estimates starts to
        // degenerate, so we need to ask for more than we want in order to chop
        // the degenerate tail off the result. If we ever allow more than 25k on
        // the limit, this number will need to be increased. We currently only
        // allow a max limit of 1k, so this setting is overkill.
        var limit = parseInt(params.limit) + 1000;
        q.agg(ejs.TermsAggregation('count').field(exports.ReplaceExact(params.count)).size(limit));
      }
    }
  }

  const qJson = q.toJSON();
  AddSearchAfter(qJson, params);
  return qJson;
};
