from openfda.tests import api_test_helpers
from openfda.tests.api_test_helpers import *


def assert_enough_results(query, count):
    meta, results = api_test_helpers.fetch(query)
    assert (
        meta["results"]["total"] > count
    ), "Query %s returned fewer results than expected: %d vs %d" % (
        query,
        count,
        meta["results"]["total"],
    )


def test_device_pma_counts():
    assert_enough_results("/device/pma.json?search=_exists_:openfda", 30000)


def test_device_510k_counts():
    assert_enough_results("/device/510k.json?search=_exists_:openfda", 14000)


def test_food_enformancement_counts():
    assert_enough_results("/food/enforcement.json?search=_missing_:openfda", 8900)


def test_device_classification():
    assert_enough_results("/device/classification.json?search=_exists_:openfda", 4300)


def test_device_reg():
    assert_enough_results(
        "/device/registrationlisting.json?search=_exists_:products.openfda", 21000
    )


def test_device_event():
    assert_enough_results("/device/event.json?search=_exists_:device.openfda", 4450000)


def test_device_enforcement():
    assert_enough_results("/device/enforcement.json?search=_missing_:openfda", 8500)


def test_device_recall():
    assert_enough_results("/device/recall.json?search=_exists_:openfda", 42000)


def test_drug_event():
    assert_enough_results(
        "/drug/event.json?search=_exists_:patient.drug.openfda", 10000000
    )


def test_drug_label():
    assert_enough_results("/drug/label.json?search=_exists_:openfda", 78000)


def test_drug_enforcement():
    assert_enough_results("/drug/enforcement.json?search=_exists_:openfda", 900)


def test_zero_limit_handling():
    meta, results = fetch(
        "/food/event.json?search=date_created:[20170220+TO+20170225]+AND+reactions:OVARIAN+CANCER&sort=date_created:asc"
    )
    eq_(len(results), 1)

    meta, results = fetch(
        "/food/event.json?search=date_created:[20170220+TO+20170225]+AND+reactions:OVARIAN+CANCER&sort=date_created:asc&limit=0"
    )
    eq_(len(results), 0)
    ok_(meta["results"]["total"] > 0)


"""
# Commenting this test out. Elasticsearch 6 no longer throws parse exceptions for bad dates in queries.
def test_detailed_error_message():
  error = expect_error(
    '/food/enforcement.json?limit=1&search=report_date:[2000-10-01+TO+2013-09-31]')
  eq_(error['details'], "[parse_exception] failed to parse date field [2013-09-31] with format [basic_date||date||epoch_millis]")
"""


def test_limitless_count():
    def test_limitless_field(field):
        meta, results = fetch("/drug/label.json?count=%s" % (field))
        # Default is still 100 even for limitless fields unless specified explicitly
        eq_(len(results), 100)

        meta, results = fetch("/drug/label.json?count=%s&limit=4000" % (field))
        eq_(len(results), 4000)

        # 2147482647 is the max, so 2147482648 is an error
        error = expect_error("/drug/label.json?count=%s&limit=2147482648" % (field))
        eq_(error["message"], "Limit cannot exceed 1000 results for count requests.")

    for field in [
        "openfda.generic_name.exact",
        "openfda.brand_name.exact",
        "openfda.substance_name.exact",
    ]:
        test_limitless_field(field)

    # Other fields should still be subject to 1000 max limit
    error = expect_error("/drug/label.json?count=active_ingredient&limit=1001")
    eq_(error["message"], "Limit cannot exceed 1000 results for count requests.")


def test_skip_limit_ranges():
    meta, results = fetch("/drug/label.json?skip=25000&limit=1000")
    eq_(len(results), 1000)

    error = expect_error("/drug/label.json?skip=25000&limit=1001")
    eq_(
        error["message"],
        "Limit cannot exceed 1000 results for search requests. Use the skip or search_after param to get additional results.",
    )
