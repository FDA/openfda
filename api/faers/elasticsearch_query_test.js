// Elasticsearch Query Builder Test

var elasticsearch_query = require('./elasticsearch_query.js');

notSupported = function(test, query) {
  test.ok(!elasticsearch_query.SupportedQueryString(query),
          "Shouldn't be supported: " + query);
};

// All the different query string queries types from
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/
// query-dsl-query-string-query.html#query-string-syntax
// that we don't want to support at this time for performance reasons.
exports.testSupportedQueryString_NotSupported = function(test) {

  // regular expression
  notSupported(test, 'name:/joh?n(ath[oa]n)/');

  // fuzziness
  notSupported(test, 'quikc~ brwn~ foks~');

  // proximity
  notSupported(test, '"fox quick"~5');

  // boosting
  notSupported(test, 'quick^2 fox');

  notSupported(test, '"john smith"^2   (foo bar)^4');

  test.done();
};


supported = function(test, query) {
  test.ok(elasticsearch_query.SupportedQueryString(query),
          'Should be supported: ' + query);
};

// All the query string query types we want to support
exports.testSupportedQueryString_Supported = function(test) {
  // wildcard field is now SUPPORTED
  supported(test, 'city.\*:something');
  supported(test, 'book.\*:(quick brown)');
  // leading wildcard
  supported(test, '*ing');
  // alternative: count:>=10
  supported(test, 'count:[10 TO *]');
  // alternative: date:<2012-01-01
  supported(test, 'date:{* TO 2012/01/01}');

  supported(test, '@drugtype:human');

  supported(test, 'active');

  supported(test, 'status:active');

  supported(test, 'msg.status:active');

  supported(test, 'msg_status:active');

  supported(test, 'title:(quick brown)');

  supported(test, 'author:"John Smith"');

  // ranges
  supported(test, 'date:[2012-01-01 TO 2012-12-31]');
  supported(test, 'count:[1 TO 5]');
  supported(test, 'tag:{alpha TO omega}');
  supported(test, 'count:>=10');
  supported(test, 'date:<2012-01-01');

  // boolean operators
  supported(test, 'quick brown +fox -news');

  // missing check
  supported(test, '_missing_:title');

  // exist check
  supported(test, '_exists_:title');

  test.done();
};

exports.testBuildSort = async function (test) {
  test.equal(await elasticsearch_query.BuildSort({}), '_id')
  test.equal(await elasticsearch_query.BuildSort({sort: ''}), '_id')
  test.equal(await elasticsearch_query.BuildSort({sort: ' report_date '}), 'report_date,_id')
  test.equal(await elasticsearch_query.BuildSort({sort: 'report_date:asc'}), 'report_date:asc,_id')
  test.equal(await elasticsearch_query.BuildSort({sort: 'report_date:desc'}), 'report_date:desc,_id')
  test.equal(await elasticsearch_query.BuildSort({sort: 'report_date:desc,field.exact'}), 'report_date:desc,field.exact,_id')

  try {
    await elasticsearch_query.BuildSort({sort: 'report_date`receiptdate'});
    test.fail();
  } catch (e) {
    test.equal(e.name, elasticsearch_query.ELASTICSEARCH_QUERY_ERROR);
  }

  try {
    await elasticsearch_query.BuildSort({sort: 'id'});
    test.fail();
  } catch (e) {
    test.equal(e.name, elasticsearch_query.ELASTICSEARCH_QUERY_ERROR);
  }

  try {
    await elasticsearch_query.BuildSort({sort: 'openfda.brand_name'})
    test.fail();
  } catch (e) {
    test.equal(e.name, elasticsearch_query.ELASTICSEARCH_QUERY_ERROR);
  }

  try {
    await elasticsearch_query.BuildSort({sort: 'openfda.brand_name.exact'})
  } catch (e) {
    test.fail();
  }

  test.done();
}

exports.testHandleDeprecatedClauses = function (test) {
    // No section, with known exact top level
    test.ok(elasticsearch_query.HandleDeprecatedClauses(
        '_missing_:type_of_report.exact AND product_type:abc AND _missing_:"something_else"') ==
        '(NOT (_exists_:type_of_report.exact)) AND product_type:abc AND (NOT (_exists_:"something_else"))');


    test.done();
}


