# coding=utf-8
import inspect
import sys

from openfda.tests.api_test_helpers import *

EXACT_FIELDS = [
    "openfda.application_number.exact",
    "openfda.brand_name.exact",
    "openfda.generic_name.exact",
    "openfda.manufacturer_name.exact",
    "openfda.nui.exact",
    "openfda.package_ndc.exact",
    "openfda.pharm_class_cs.exact",
    "openfda.pharm_class_epc.exact",
    "openfda.pharm_class_moa.exact",
    "openfda.product_ndc.exact",
    "openfda.product_type.exact",
    "openfda.route.exact",
    "openfda.rxcui.exact",
    "openfda.spl_id.exact",
    "openfda.spl_set_id.exact",
    "openfda.substance_name.exact",
    "openfda.unii.exact",
]


def test_exact_field_queries_after_fda_253():
    for field in EXACT_FIELDS:
        meta, counts = fetch_counts("/drug/drugsfda.json?count=%s&limit=1" % field)
        eq_(len(counts), 1)


def test_total_count():
    assert_total("/drug/drugsfda.json", 24000)


def test_malformed_dosage_in_products():
    meta, results = fetch('/drug/drugsfda.json?search=application_number:"ANDA206006"')
    eq_(len(results), 1)

    app = results[0]
    products = sorted(app["products"], key=lambda p: p["product_number"])
    eq_(len(products), 1)

    drug = products[0]
    ingredients = drug["active_ingredients"]
    eq_(len(ingredients), 3)
    eq_(drug["brand_name"], "LANSOPRAZOLE, AMOXICILLIN AND CLARITHROMYCIN (COPACKAGED)")
    eq_(drug["dosage_form"], "CAPSULE, TABLET, CAPSULE, DELAYED REL PELLETS")
    eq_(drug["product_number"], "001")
    eq_(drug.get("route"), "ORAL")


def test_unknowns_in_product():
    meta, results = fetch('/drug/drugsfda.json?search=application_number:"ANDA204845"')
    eq_(len(results), 1)

    app = results[0]
    products = sorted(app["products"], key=lambda p: p["product_number"])
    eq_(len(products), 1)

    drug = products[0]
    ingredients = drug["active_ingredients"]
    eq_(len(ingredients), 1)
    eq_(ingredients[0]["name"], "FOSAPREPITANT DIMEGLUMINE")
    eq_(ingredients[0]["strength"], "UNKNOWN")
    eq_(drug["brand_name"], "FOSAPREPITANT DIMEGLUMINE")
    eq_(drug["dosage_form"], "UNKNOWN")
    eq_(drug["product_number"], "001")
    eq_(drug["route"], "UNKNOWN")


def test_orphan_submission():
    meta, results = fetch('/drug/drugsfda.json?search=application_number:"NDA008107"')
    eq_(len(results), 1)

    app = results[0]
    submissions = sorted(app["submissions"], key=lambda p: int(p["submission_number"]))
    sub = submissions[0]
    eq_(sub["submission_property_type"][0]["code"], "Orphan")


def test_submission_public_notes():
    meta, results = fetch('/drug/drugsfda.json?search=application_number:"NDA020229"')
    eq_(len(results), 1)

    app = results[0]
    submissions = sorted(app["submissions"], key=lambda p: int(p["submission_number"]))
    eq_(len(submissions), 6)

    sub = submissions[0]
    eq_(sub["submission_public_notes"], "Withdrawn FR Effective 11/03/2016")


def test_multiple_application_docs():
    meta, results = fetch(
        '/drug/drugsfda.json?search=_exists_:submissions.application_docs.title+AND+application_number:"ANDA076356"'
    )
    eq_(len(results), 1)

    app = results[0]
    submissions = sorted(app["submissions"], key=lambda p: int(p["submission_number"]))
    eq_(len(submissions), 19)
    sub = submissions[0]

    docs = sorted(sub["application_docs"], key=lambda p: int(p["id"]))
    eq_(len(docs), 2)

    doc = docs[0]
    eq_(doc["id"], "23820")
    eq_(doc["type"], "Other Important Information from FDA")
    eq_(doc.get("title"), None)
    eq_(doc["date"], "20050728")
    eq_(
        doc["url"],
        "http://www.fda.gov/Drugs/DrugSafety/PostmarketDrugSafetyInformationforPatientsandProviders/ucm094305.htm",
    )

    doc = docs[1]
    eq_(doc["id"], "24054")
    eq_(doc["type"], "Other")
    eq_(doc["title"], "REMS")
    eq_(doc["date"], "20150702")
    eq_(
        doc["url"],
        "http://www.accessdata.fda.gov/scripts/cder/rems/index.cfm?event=RemsDetails.page&REMS=24",
    )


def test_application_docs():
    meta, results = fetch(
        '/drug/drugsfda.json?search=_exists_:submissions.application_docs.title+AND+application_number:"ANDA083827"'
    )
    eq_(len(results), 1)

    app = results[0]
    submissions = sorted(app["submissions"], key=lambda p: int(p["submission_number"]))
    eq_(len(submissions), 1)
    sub = submissions[0]

    docs = sub["application_docs"]
    eq_(len(docs), 1)

    doc = docs[0]
    eq_(doc["id"], "53918")
    eq_(doc["type"], "Other")
    eq_(doc["title"], "Safety Labeling Change Order Letter")
    eq_(doc["date"], "20180501")
    eq_(
        doc["url"],
        "https://www.accessdata.fda.gov/drugsatfda_docs/appletter/slc/2018/086502, 086503, 086501, 086500, 086499, 086498,083827_SLCOrderLtr.pdf",
    )


def test_one_record_in_detail():
    meta, results = fetch(
        '/drug/drugsfda.json?search=application_number:"ANDA208705"&limit=100'
    )
    eq_(len(results), 1)

    app = results[0]
    eq_(app["application_number"], "ANDA208705")
    eq_(app["sponsor_name"], "ALEMBIC PHARMS LTD")

    products = sorted(app["products"], key=lambda p: p["product_number"])
    eq_(len(products), 2)

    drug = products[0]
    ingredients = drug["active_ingredients"]
    eq_(len(ingredients), 1)
    eq_(ingredients[0]["name"], "CHOLINE FENOFIBRATE")
    eq_(ingredients[0]["strength"], "EQ 45MG FENOFIBRIC ACID")
    eq_(drug["brand_name"], "FENOFIBRIC ACID")
    eq_(drug["dosage_form"], "CAPSULE, DELAYED RELEASE")
    eq_(drug["marketing_status"], "Prescription")
    eq_(drug["product_number"], "001")
    eq_(drug["reference_drug"], "No")
    eq_(drug["reference_standard"], "No")
    eq_(drug["route"], "ORAL")
    eq_(drug["te_code"], "AB")

    drug = products[1]
    ingredients = drug["active_ingredients"]
    eq_(len(ingredients), 1)
    eq_(ingredients[0]["name"], "CHOLINE FENOFIBRATE")
    eq_(ingredients[0]["strength"], "EQ 135MG FENOFIBRIC ACID")
    eq_(drug["brand_name"], "FENOFIBRIC ACID")
    eq_(drug["dosage_form"], "CAPSULE, DELAYED RELEASE")
    eq_(drug["marketing_status"], "Prescription")
    eq_(drug["product_number"], "002")
    eq_(drug["reference_drug"], "No")
    eq_(drug["reference_standard"], "No")
    eq_(drug["route"], "ORAL")
    eq_(drug["te_code"], "AB")

    openfda = app["openfda"]
    eq_(openfda["application_number"], ["ANDA208705"])
    eq_(openfda["brand_name"], ["FENOFIBRIC ACID"])

    submissions = sorted(app["submissions"], key=lambda p: int(p["submission_number"]))
    eq_(len(submissions), 4)

    sub = submissions[0]
    eq_(sub["submission_class_code"], "UNKNOWN")
    eq_(sub.get("submission_class_code_description"), None)
    eq_(sub["submission_number"], "1")
    eq_(sub.get("submission_public_notes"), None)
    eq_(sub["submission_status"], "AP")
    eq_(sub["submission_status_date"], "20170512")
    eq_(sub["review_priority"], "STANDARD")
    eq_(sub["submission_type"], "ORIG")

    sub = submissions[1]
    eq_(sub["submission_class_code"], "LABELING")
    eq_(sub["submission_class_code_description"], "Labeling")
    eq_(sub["submission_number"], "7")
    eq_(sub.get("submission_public_notes"), None)
    eq_(sub["submission_status"], "AP")
    eq_(sub["submission_status_date"], "20190919")
    eq_(sub["review_priority"], "STANDARD")
    eq_(sub["submission_type"], "SUPPL")

    sub = submissions[2]
    eq_(sub["submission_class_code"], "LABELING")
    eq_(sub["submission_class_code_description"], "Labeling")
    eq_(sub["submission_number"], "9")
    eq_(sub.get("submission_public_notes"), None)
    eq_(sub["submission_status"], "AP")
    eq_(sub["submission_status_date"], "20190919")
    eq_(sub["review_priority"], "STANDARD")
    eq_(sub["submission_type"], "SUPPL")

    sub = submissions[3]
    eq_(sub["submission_class_code"], "LABELING")
    eq_(sub["submission_class_code_description"], "Labeling")
    eq_(sub["submission_number"], "11")
    eq_(sub.get("submission_public_notes"), None)
    eq_(sub["submission_status"], "AP")
    eq_(sub["submission_status_date"], "20190919")
    eq_(sub["review_priority"], "STANDARD")
    eq_(sub["submission_type"], "SUPPL")


if __name__ == "__main__":
    all_functions = inspect.getmembers(sys.modules[__name__], inspect.isfunction)
    for key, func in all_functions:
        if key.find("test_") > -1:
            func()
