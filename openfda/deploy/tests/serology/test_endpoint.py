# coding=utf-8
import inspect
import sys

from openfda.tests.api_test_helpers import *


def test_total_count():
    assert_total("/device/covid19serology.json", 6380)


def test_basic_search():
    meta, results = fetch(
        "/device/covid19serology.json?search=manufacturer.exact:Euroimmun+AND+date_performed:([20200421+TO+20200421])"
        '+AND+panel:"Panel 1"+AND+sample_id:C0001+AND+sample_no:[1+TO+1]+AND+lot_number:E200330DT'
    )
    eq_(len(results), 1)

    row = results[0]
    # Verify base logic.
    eq_("Euroimmun", row.get("manufacturer"))
    eq_("SARS-COV-2 ELISA (IgG)", row.get("device"))
    eq_("4/21/2020", row.get("date_performed"))
    eq_("E200330DT", row.get("lot_number"))
    eq_("Panel 1", row.get("panel"))
    eq_("1", row.get("sample_no"))
    eq_("C0001", row.get("sample_id"))
    eq_("Serum", row.get("type"))
    eq_("Negatives", row.get("group"))
    eq_("NA", row.get("days_from_symptom"))
    eq_("NA", row.get("igm_result"))
    eq_("Negative", row.get("igg_result"))
    eq_("NA", row.get("iga_result"))
    eq_("NA", row.get("pan_result"))
    eq_("NA", row.get("igm_igg_result"))
    eq_("Pass", row.get("control"))
    eq_("0", row.get("igm_titer"))
    eq_("0", row.get("igg_titer"))
    eq_("Negative", row.get("igm_truth"))
    eq_("Negative", row.get("igg_truth"))
    eq_("Negative", row.get("antibody_truth"))
    eq_("NA", row.get("igm_agree"))
    eq_("TN", row.get("igg_agree"))
    eq_("NA", row.get("iga_agree"))
    eq_("NA", row.get("pan_agree"))
    eq_("NA", row.get("igm_igg_agree"))
    eq_("NA", row.get("igm_iga_result"))
    eq_("NA", row.get("igm_iga_agree"))
    eq_("TN", row.get("antibody_agree"))


def test_exact_fields():
    assert_total(
        "/device/covid19serology.json?search=date_performed:[20200420+TO+20200422]", 110
    )
    meta, results = fetch(
        "/device/covid19serology.json?search=date_performed:[20221225+TO+20230422]"
    )
    eq_(results, None)


def test_date_ranges():
    assert_total(
        '/device/covid19serology.json?search=manufacturer.exact:Euroimmun+AND+device.exact:"SARS-COV-2 ELISA (IgG)"',
        110,
    )
    assert_total(
        '/device/covid19serology.json?search=manufacturer:Euroimmun+AND+device:"SARS-COV-2 ELISA (IgG)"',
        110,
    )
    assert_total(
        '/device/covid19serology.json?search=manufacturer:Euroimmun+AND+device:"SARS-COV-2"',
        110,
    )

    meta, results = fetch(
        '/device/covid19serology.json?search=manufacturer.exact:Euroimmun+AND+device.exact:"SARS-COV-2"'
    )
    eq_(results, None)


def test_sample_no_ranges():
    assert_total("/device/covid19serology.json?search=sample_no:[1+TO+5]&limit=100", 20)
    assert_total(
        "/device/covid19serology.json?search=sample_no:[1+TO+15]&limit=100", 60
    )


if __name__ == "__main__":
    all_functions = inspect.getmembers(sys.modules[__name__], inspect.isfunction)
    for key, func in all_functions:
        if key.find("test_") > -1:
            func()
