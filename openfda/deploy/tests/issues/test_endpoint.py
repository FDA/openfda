from openfda.tests.api_test_helpers import *

from openfda.tests.api_test_helpers import *


def test_uppercase_filter():
    assert_count_top(
        "/drug/event.json?search=patient.drug.openfda.generic_name:ASPIRIN&limit=100&count=patient.reaction.reactionmeddrapt.exact&skip=0",
        ["NAUSEA", "DYSPNOEA", "FATIGUE", "DRUG INEFFECTIVE", "DIZZINESS", "DIARRHOEA"],
    )


def test_spl_harmonization():
    assert_total("/drug/label.json?search=openfda.brand_name:humira", 1)


def test_exact():
    assert_total("/drug/label.json?search=openfda.brand_name.exact:Humira", 1)


def test_missing_generic():
    assert_total(
        "/drug/event.json?search=patient.drug.openfda.generic_name:(%22FUROSEMIDE%22)+AND+patient.reaction.reactionmeddrapt:(%22RENAL+FAILURE%22)+AND+receivedate:(%5b20120101+TO+20120131%5d)&limit=1&skip=0",
        5,
    )


def test_missing_clause_es5():
    assert_total_exact(
        "/device/recall.json?search="
        "_missing_:openfda+AND+root_cause_description:Design"
        "+AND+_missing_:pma_numbers"
        "+AND+event_date_terminated:[2012-01-8+TO+2012-03-30]"
        "+AND+NOT+_missing_:firm_fei_number+AND+product_res_number.exact:Z-0866-2011",
        1,
    )


def test_malformed_dates():
    meta, counts = fetch_counts(
        "/device/registrationlisting.json?search=_exists_:(products.created_date)&count=products.created_date&skip=0"
    )
    import arrow

    dt = arrow.parser.DateTimeParser()
    # ensure our dates are all valid,
    for count in counts:
        try:
            dt.parse(count.term, "YYYYMMDD")
        except:
            assert False, "Invalid date: %s" % count.term
