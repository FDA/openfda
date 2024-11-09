import collections
import os
from unittest.case import SkipTest

import requests

ENDPOINT = os.getenv("OPENFDA_ENDPOINT_URL", "http://localhost:8000")
Count = collections.namedtuple("Count", ["term", "count"])


class CountList(list):
    def __repr__(self):
        return "\n".join([repr(c) for c in self])


def extract_counts(results):
    counts = CountList()
    for r in results:
        if "term" in r:
            counts.append(Count(r["term"], r["count"]))
        else:
            counts.append(Count(r["time"], r["count"]))
    return counts


def assert_total(query, minimum):
    meta, results = fetch(query)
    assert_greater_equal(
        meta["results"]["total"],
        minimum,
        "Query %s had fewer results than expected.  %s < %s"
        % (query, meta["results"]["total"], minimum),
    )


def assert_total_exact(query, total):
    meta, results = fetch(query)
    eq_(
        meta["results"]["total"],
        total,
        "Query %s had different number of results than expected.  %s != %s"
        % (query, meta["results"]["total"], total),
    )


def assert_count_top(query, expected_terms, N=10):
    """Verify that all of the terms in `terms` are found in the top-N count results."""
    meta, counts = fetch_counts(query)
    count_terms = set([count.term for count in counts[:N]])
    for term in expected_terms:
        assert term in count_terms, "Query %s missing expected term %s; terms=%s" % (
            query,
            term,
            "\n".join(list(count_terms)),
        )


def assert_count_contains(query, expected_terms):
    meta, counts = fetch_counts(query)
    count_terms = set([count.term for count in counts])

    for term in expected_terms:
        assert term in count_terms, "Query %s missing expected term %s; terms=%s" % (
            query,
            term,
            "\n".join(count_terms),
        )


def assert_unii(query, expected_result):
    meta, results = fetch(query)
    unii_list = []
    if "openfda" in list(results[0].keys()):
        for result in results[0]["openfda"]["unii"]:
            unii_list.append(result)
    elif "patient" in list(results[0].keys()):
        for result in results[0]["patient"]["drug"][0]["openfda"]["unii"]:
            unii_list.append(result)
    eq_(sorted(unii_list), sorted(expected_result))


def fetch(query):
    print("Fetching %s" % query)
    data = requests.get(ENDPOINT + query).json()
    return data.get("meta"), data.get("results")


def fetch_headers(query):
    print("Fetching headers for %s" % query)
    data = requests.get(query)
    return data.headers


def expect_error(query):
    print("Fetching %s" % query)
    data = requests.get(ENDPOINT + query).json()
    return data.get("error")


def json(query):
    print("Getting %s" % query)
    data = requests.get(ENDPOINT + query).json()
    return data


def fetch_counts(query):
    meta, results = fetch(query)
    return meta, extract_counts(results)


def not_circle(fn):
    "Skip this test when running under CI."

    def _fn(*args, **kw):
        if "CIRCLECI" in os.environ:
            raise SkipTest
        fn(*args, **kw)

    return _fn


def ok_(expr, msg=None):
    """Shorthand for assert. Saves 3 whole characters!"""
    if not expr:
        raise AssertionError(msg)


def eq_(a, b, msg=None):
    """Shorthand for 'assert a == b, "%r != %r" % (a, b)"""
    if not a == b:
        raise AssertionError(msg or "%r != %r" % (a, b))


def assert_greater(a, b, msg=None):
    """Just like self.assertTrue(a > b), but with a nicer default message."""
    if not a > b:
        standardMsg = "%s not greater than %s" % (repr(a), repr(b))
        fail(_formatMessage(msg, standardMsg))


def assert_greater_equal(a, b, msg=None):
    """Just like self.assertTrue(a >= b), but with a nicer default message."""
    if not a >= b:
        standardMsg = "%s not greater than or equal to %s" % (repr(a), repr(b))
        fail(_formatMessage(msg, standardMsg))


def fail(msg):
    raise AssertionError(msg)


def _formatMessage(msg, standardMsg):
    """Honour the longMessage attribute when generating failure messages.
    If longMessage is False this means:
    * Use only an explicit message if it is provided
    * Otherwise use the standard message for the assert

    If longMessage is True:
    * Use the standard message
    * If an explicit message is provided, plus ' : ' and the explicit message
    """
    if msg is None:
        return standardMsg
    try:
        # don't switch to '{}' formatting in Python 2.X
        # it changes the way unicode input is handled
        return "%s : %s" % (standardMsg, msg)
    except UnicodeDecodeError:
        return "%s : %s" % (repr(standardMsg), repr(msg))
