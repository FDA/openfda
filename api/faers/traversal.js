const qs = require('qs')

exports.BuildLinkRelNext = function (request, params, esResponseBody) {

  function buildURL(modifiedQueryStr) {
    let nextPageUrl = (request.header('x-api-umbrella-request-id') ?
      'https://api.fda.gov' :
      request.protocol + '://' + request.get('host'))
      + request.path + '?' + qs.stringify(modifiedQueryStr);
    return nextPageUrl;
  }

  // Determine if we're using a pagination approach based on skip/limit combination
  // (which is currently limited to a 25K-record results window)
  // or an approach based on the search_after now available in ES5 (unlimited pagination)
  if (params.skip > 0) {
    // skip parameter is present, which means we cannot rely on search_after.
    // search_after and skip do not work together.
    let nextSkip = params.skip + params.limit;
    if (esResponseBody.hits.total.value > nextSkip) {
      const nextQuery = Object.assign({}, request.query);
      nextQuery.skip = nextSkip;
      return buildURL(nextQuery);
    }
  } else
  // If the current page contains fewer hits than the limit value, this means we have reached
  // the last page and there is no point in generating a Next link.
  if (esResponseBody.hits.hits.length && esResponseBody.hits.hits.length >= params.limit) {
    const nextQuery = Object.assign({}, request.query);
    nextQuery.search_after = qs.stringify(esResponseBody.hits.hits.slice(-1)[0].sort, {delimiter: ';'})
    return buildURL(nextQuery);
  }

  return '';
}
