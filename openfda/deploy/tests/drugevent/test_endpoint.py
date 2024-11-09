
from openfda.tests.api_test_helpers import *


EXACT_FIELDS = [
    "openfda.application_number.exact",
    "openfda.brand_name.exact",
    "openfda.generic_name.exact",
    "openfda.manufacturer_name.exact",
    "openfda.nui.exact",
    "openfda.pharm_class_cs.exact",
    "openfda.pharm_class_epc.exact",
    "openfda.pharm_class_moa.exact",
    "openfda.pharm_class_pe.exact",
    "openfda.product_type.exact",
    "openfda.route.exact",
    "openfda.rxcui.exact",
    "openfda.substance_name.exact",
    "openfda.unii.exact",
]


def test_exact_field_queries_after_fda_253():
    for field in EXACT_FIELDS:
        print(field)
        meta, counts = fetch_counts(
            "/drug/event.json?count=patient.drug.%s&limit=1" % field
        )
        eq_(len(counts), 1)


def test_not_analyzed_0():
    assert_total(
        "/drug/event.json?search=patient.drug.openfda.spl_id:d08e6fbf-9162-47cd-9cf6-74ea24d48214",
        65410,
    )


def test_date_1():
    assert_total("/drug/event.json?search=receivedate:20040319", 2900)


def test_date_range_2():
    assert_total("/drug/event.json?search=receivedate:([20040319+TO+20041231])", 166000)


def test_openfda_3():
    assert_total("/drug/event.json?search=patient.drug.openfda.unii:A4P49JAZ9H", 1463)


def test_2018q3_present():
    assert_total("/drug/event.json?search=safetyreportid.exact:9995402", 1)


def test_drug_event_unii():
    assert_unii(
        "/drug/event.json?search=safetyreportid:10003301", ["RM1CE97Z4N", "WK2XYI10QM"]
    )


def test_drug_generic_name_count():
    assert_total("/drug/event.json?search=patient.drug.medicinalproduct:TOVIAZ", 7700)
    assert_total(
        "/drug/event.json?search=patient.drug.openfda.generic_name:%22dimethyl%20fumarate%22",
        90000,
    )


def test_histogram():
    meta, counts = fetch_counts(
        "/drug/event.json?search=receivedate:[20040101+TO+20180611]&count=receivedate"
    )
    eq_(counts[0].term, "20040101")
    eq_(counts[0].count, 1)
    eq_(counts[1].term, "20040102")
    ok_(counts[1].count > 500)
    eq_(counts[2].term, "20040103")
    eq_(counts[2].count, 2)
    assert_greater_equal(len(counts), 4920)


# Uncomment once FDA-250 is fixed.


def test_nonsteriodial_drug_reactions():
    assert_count_top(
        "/drug/event.json?search=patient.drug.openfda.pharm_class_epc:"
        '"nonsteroidal+anti-inflammatory+drug"&count=patient.reaction.reactionmeddrapt.exact',
        ["DRUG INEFFECTIVE", "NAUSEA", "FATIGUE", "PAIN"],
    )


def test_nonsteriodial_drug_reactions():
    assert_count_top(
        "/drug/event.json?search=patient.drug.openfda.pharm_class_epc:"
        '"nonsteroidal+anti-inflammatory+drug"&count=patient.reaction.reactionmeddrapt.exact',
        ["DIZZINESS", "NAUSEA", "FATIGUE"],
    )

    assert_count_contains(
        "/drug/event.json?search=patient.drug.openfda.pharm_class_epc:"
        '"nonsteroidal+anti-inflammatory+drug"&count=patient.reaction.reactionmeddrapt.exact&limit=1000',
        ["HEADACHE", "HEART RATE INCREASED", "SEPSIS"],
    )


def test_date_formats_after_migration():
    assert_total(
        '/drug/event.json?search=safetyreportid:"10141822"+AND+'
        "receiptdate:20181008+AND+receiptdate:2018-10-08+AND+transmissiondate:"
        "[20190203+TO+20190205]+AND+transmissiondate:[2019-02-03+TO+2019-02-05]",
        1,
    )


def test_drugrecurrence_is_always_an_array():
    meta, results = fetch("/drug/event.json?search=safetyreportid:14340262&limit=1")

    event = results[0]
    eq_(len(event["patient"]["drug"]), 1)
    eq_(type(event["patient"]["drug"][0]["drugrecurrence"]), type([]))
    eq_(len(event["patient"]["drug"][0]["drugrecurrence"]), 4)

    drugrecurrence = sorted(
        event["patient"]["drug"][0]["drugrecurrence"],
        key=lambda k: k["drugrecuraction"],
    )
    eq_(drugrecurrence[0]["drugrecuraction"], "Drug intolerance")
    eq_(drugrecurrence[1]["drugrecuraction"], "Dyspnoea")
    eq_(
        drugrecurrence[2]["drugrecuraction"],
        "Type III immune complex mediated reaction",
    )
    eq_(drugrecurrence[3]["drugrecuraction"], "Urticaria")

    meta, results = fetch("/drug/event.json?search=safetyreportid:17394804&limit=1")
    event = results[0]
    eq_(len(event["patient"]["drug"]), 1)
    eq_(type(event["patient"]["drug"][0]["drugrecurrence"]), type([]))
    eq_(len(event["patient"]["drug"][0]["drugrecurrence"]), 1)
    eq_(
        event["patient"]["drug"][0]["drugrecurrence"][0]["drugrecuraction"],
        "Embolism venous",
    )
