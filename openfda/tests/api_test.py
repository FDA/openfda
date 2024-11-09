#!/usr/bin/env python

"""
Quick API tests to run against a new index build.

These test certain "quasi-invariants" about the index data: e.g. we should have
more reports for etanecerpt than for naltrexone.  They are by no means a complete
test of the full system.  Before pushing out a new index, the
`refresh_test_responses` script should also be run to compare the index changes
against a wider test set.
"""

import os
import pprint

from openfda.tests.api_test_helpers import *

ENDPOINT = os.getenv("OPENFDA_ENDPOINT_URL", "http://localhost:8000")


@not_circle
def test_humira_class():
    "Checks for a bug related to incorrect splitting of the pharm_class field."
    data = _fetch(
        "/drug/event.json?count=pharm_class_epc_exact&search=patient.drug.medicinalproduct:humira"
    )
    assert data["results"][0]["term"] == "Tumor Necrosis Factor Blocker [EPC]"


@not_circle
def test_reactionmeddrapt_exact():
    "Test for correct tokenization of the exact field."
    data = _fetch("/drug/event.json?count=patient.reaction.reactionmeddrapt.exact")
    assert data["results"][0]["term"] == "DRUG INEFFECTIVE"


@not_circle
def test_faers_generic_name():
    "Sanity test generic drug counts.  Counts may change, but ordering should in general be consistent."
    data = _fetch("/drug/event.json?count=patient.drug.openfda.generic_name.exact")
    pprint.pprint(data["results"])
    counts = extract_counts(data)
    assert counts[0].term == "ETANERCEPT"
    assert counts[1].term == "ADALIMUMAB"
    assert counts[2].term == "ASPIRIN"
    assert counts[3].term == "INTERFERON BETA-1A"


@not_circle
def test_openfda_join_humira():
    "Verifies that we joined correctly for a specific report."
    data = _fetch(
        "/drug/event.json?search=reportduplicate.duplicatenumb.exact:GB-ABBVIE-14P-167-1209507-00"
    )
    results = data["results"][0]
    pprint.pprint(results)
    drugs = results["patient"]["drug"]
    assert len(drugs) == 2
    assert drugs[0]["openfda"]["spl_id"]
    assert drugs[1]["openfda"]["spl_id"]


@not_circle
def test_pagination():
    # Check that the header exists when skip & limit are not provided.
    ndc_data = fetch_headers(
        ENDPOINT + '/drug/ndc.json?search=marketing_start_date:"20060707"'
    )
    data_link = ndc_data["Link"]
    assert (
        data_link
        == "<"
        + ENDPOINT
        + '/drug/ndc.json?search=marketing_start_date%3A%2220060707%22&skip=1&limit=1>; rel="next"'
    )

    # Check that the header doesn't exist on the last entry.
    while data_link:
        ndc_data = fetch_headers(data_link[1:-1])
        if "Link" in ndc_data:
            data_link = ndc_data["Link"]
        else:
            data_link = None

    assert data_link == None

    # Check that the skip & limit parameters are included when provided.
    event_data = fetch_headers(ENDPOINT + "/drug/event.json?skip=100&limit=100")
    assert (
        event_data["Link"]
        == "<" + ENDPOINT + '/drug/event.json?skip=200&limit=100>; rel="next"'
    )
