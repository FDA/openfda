# coding=utf-8
import inspect
import sys

from openfda.tests.api_test_helpers import *

EXACT_FIELDS = [
    "openfda.manufacturer_name.exact",
    "openfda.nui.exact",
    "openfda.pharm_class_cs.exact",
    "openfda.pharm_class_epc.exact",
    "openfda.pharm_class_moa.exact",
    "openfda.pharm_class_pe.exact",
    "openfda.rxcui.exact",
    "openfda.spl_set_id.exact",
    "openfda.unii.exact",
]


def test_exact_field_queries_after_fda_253():
    for field in EXACT_FIELDS:
        print(field)
        meta, counts = fetch_counts("/drug/ndc.json?count=%s&limit=1" % field)
        eq_(len(counts), 1)


def test_total_count():
    assert_total("/drug/ndc.json", 100000)


def test_record_with_multiple_packaging():
    meta, results = fetch("/drug/ndc.json?search=product_ndc:46122-062")
    eq_(len(results), 1)

    ndc = results[0]
    eq_(ndc["product_id"], "46122-062_e37f7dcd-aecc-49d4-881c-f2b490bdd5b2")
    eq_(ndc["product_ndc"], "46122-062")
    eq_(ndc["spl_id"], "e37f7dcd-aecc-49d4-881c-f2b490bdd5b2")
    eq_(ndc["product_type"], "HUMAN OTC DRUG")
    eq_(ndc["finished"], True)
    eq_(ndc["brand_name"], "Acetaminophen")
    eq_(ndc["brand_name_base"], "Acetaminophen")
    eq_(ndc.get("brand_name_suffix"), None)
    eq_(ndc["generic_name"], "Acetaminophen")
    eq_(ndc["dosage_form"], "TABLET, FILM COATED, EXTENDED RELEASE")
    eq_(ndc["route"][0], "ORAL")
    eq_(ndc["marketing_start_date"], "20020430")
    eq_(ndc.get("marketing_end_date"), None)
    eq_(ndc["marketing_category"], "ANDA")
    eq_(ndc["application_number"], "ANDA076200")
    eq_(ndc["labeler_name"], "Amerisource Bergen")
    eq_(ndc["active_ingredients"][0]["name"], "ACETAMINOPHEN")
    eq_(ndc["active_ingredients"][0]["strength"], "650 mg/1")
    eq_(ndc.get("pharm_class"), None)
    eq_(ndc.get("dea_schedule"), None)
    eq_(ndc["listing_expiration_date"], "20221231")

    packaging = sorted(ndc["packaging"], key=lambda k: k["package_ndc"])
    eq_(len(packaging), 2)
    eq_(packaging[0]["package_ndc"], "46122-062-71")
    eq_(
        packaging[0]["description"],
        "50 TABLET, FILM COATED, EXTENDED RELEASE in 1 BOTTLE (46122-062-71)",
    )
    eq_(packaging[0]["marketing_start_date"], "20020430")
    eq_(packaging[0]["sample"], False)

    eq_(packaging[1]["package_ndc"], "46122-062-78")
    eq_(
        packaging[1]["description"],
        "100 TABLET, FILM COATED, EXTENDED RELEASE in 1 BOTTLE (46122-062-78)",
    )
    eq_(packaging[1]["marketing_start_date"], "20020430")
    eq_(packaging[1]["sample"], False)

    openfda = ndc["openfda"]
    eq_(openfda["is_original_packager"][0], True)
    eq_(openfda["manufacturer_name"][0], "Amerisource Bergen")
    eq_(openfda["rxcui"][0], "1148399")
    eq_(openfda["spl_set_id"][0], "c0f4d7f1-aa17-4233-bc47-41c280fdd7ce")
    eq_(openfda["unii"][0], "362O9ITL9D")

    # Updates to SPL made barcode unparseable; hence commenting out next line
    # eq_(openfda["upc"][0], "0087701408991")


def test_date_fields():
    assert_total("/drug/ndc.json?search=_exists_:marketing_start_date", 10000)
    assert_total("/drug/ndc.json?search=_exists_:marketing_end_date", 3000)


def test_exact_fields():
    assert_total(
        "/drug/ndc.json?search=openfda.brand_name.exact:Amoxicillin and Clavulanate Potassium",
        1,
    )


def test_minimum_CVS_brand_drugs():
    assert_total("/drug/ndc.json?search=brand_name_suffix:Rite Aid", 2)


def test_openfda_fields():
    meta, results = fetch(
        "/drug/ndc.json?search=_exists_:openfda.nui+AND+_exists_:openfda.rxcui+AND+_exists_:openfda.upc&limit=1"
    )

    eq_(len(results), 1)

    ndc = results[0]
    ok_(ndc["openfda"]["upc"] is not None)
    ok_(ndc["openfda"]["rxcui"] is not None)
    ok_(ndc["openfda"]["nui"] is not None)


def test_pharm_class_exists():
    assert_total("/drug/ndc.json?search=_exists_:openfda.pharm_class_pe", 10)
    assert_total("/drug/ndc.json?search=_exists_:openfda.pharm_class_moa", 10)
    assert_total("/drug/ndc.json?search=_exists_:openfda.pharm_class_epc", 10)
    assert_total("/drug/ndc.json?search=_exists_:openfda.pharm_class_cs", 10)


def test_dea_scheduled_numbers():
    meta, results = fetch("/drug/ndc.json?count=dea_schedule")

    eq_(len(results), 5)

    for each_result in results:
        ok_(each_result["count"] > 1)


def test_lotion_products():
    meta, results = fetch('/drug/ndc.json?search=dosage_form.exact:"LOTION"&limit=1')

    eq_(len(results), 1)

    eq_(results[0]["dosage_form"], "LOTION")


def test_DENTAL_route_drugs():
    assert_total('/drug/ndc.json?search=route:"DENTAL"', 1000)


def test_drugs_expiring_in_date_range():
    assert_total(
        "/drug/ndc.json?search=marketing_end_date:([20001001+TO+20251231])", 3900
    )


def test_drugs_with_original_packager():
    assert_total("/drug/ndc.json?search=openfda.is_original_packager:true", 1000)


# def test_unfinished_record_with_multiple_packaging():
#   meta, results = fetch(
#     '/drug/ndc.json?search=product_ndc:0002-0485')
#   eq_(len(results), 1)
#
#   ndc = results[0]
#   eq_(ndc["product_id"], "0002-0485_a30f38b1-df2b-4bad-a9fc-93ca5f77e3dd")
#   eq_(ndc["product_ndc"], "0002-0485")
#   eq_(ndc["spl_id"], "a30f38b1-df2b-4bad-a9fc-93ca5f77e3dd")
#   eq_(ndc["product_type"], "BULK INGREDIENT")
#   eq_(ndc["finished"], False)
#   eq_(ndc.get("brand_name"), None)
#   eq_(ndc.get("brand_name_base"), None)
#   eq_(ndc.get("brand_name_suffix"), None)
#   eq_(ndc["generic_name"], "Insulin human")
#   eq_(ndc["dosage_form"], "CRYSTAL")
#   eq_(ndc.get("route"), None)
#   eq_(ndc["marketing_start_date"], "20150401")
#   eq_(ndc.get("marketing_end_date"), None)
#   eq_(ndc["marketing_category"], "BULK INGREDIENT")
#   eq_(ndc.get("application_number"), None)
#   eq_(ndc["labeler_name"], "Eli Lilly and Company")
#   eq_(ndc["active_ingredients"][0]['name'], "INSULIN HUMAN")
#   eq_(ndc["active_ingredients"][0]['strength'], "1 g/g")
#   eq_(ndc.get("pharm_class"), None)
#   eq_(ndc.get("dea_schedule"), None)
#   eq_(ndc.get("listing_expiration_date"), "20191231")
#
#   packaging = sorted(ndc["packaging"], key=lambda k: k['package_ndc'])
#   eq_(len(packaging), 2)
#
#   eq_(packaging[0]["package_ndc"], "0002-0485-04")
#   eq_(packaging[0]["description"], "4 BOTTLE in 1 CONTAINER (0002-0485-04)  > 2000 g in 1 BOTTLE")
#
#   eq_(packaging[1]["package_ndc"], "0002-0485-18")
#   eq_(packaging[1]["description"], "1 BOTTLE in 1 CONTAINER (0002-0485-18)  > 2000 g in 1 BOTTLE")
#
#   eq_(len(ndc["openfda"]), 0)

# Test removed as there are not multple dosage forms in NDC records anymore.
# def test_multiple_dosage_forms():
#   meta, results = fetch(
#     '/drug/ndc.json?search=product_ndc:65954-560')
#   eq_(len(results), 1)
#
#   ok_(len(results[0]['dosage_form'].split(",")) > 1, "There are not more than 1 dosage forms")

# def test_multiple_pharm_classes():
#   meta, results = fetch(
#     '/drug/ndc.json?search=product_ndc:65923-249')
#   eq_(len(results), 1)
#
#   ndc = results[0]
#   openfda = ndc["openfda"]
#   ok_(openfda is not None)
#   eq_(len(openfda["pharm_class_moa"]), 1)
#   eq_(len(openfda["pharm_class_cs"]), 1)
#   eq_(len(openfda["pharm_class_pe"]), 2)
#   eq_(len(openfda["pharm_class_epc"]), 2)
#   eq_(len(openfda["nui"]), 6)


def test_drug_with_most_number_of_packaging():
    meta, results = fetch("/drug/ndc.json?search=product_ndc:66083-741")
    eq_(len(results), 1)

    ndc = results[0]
    packaging = sorted(ndc["packaging"], key=lambda k: k["package_ndc"])
    eq_(len(packaging), 9)

    eq_(packaging[8]["package_ndc"], "66083-741-12")
    eq_(packaging[8]["description"], "640 L in 1 CYLINDER (66083-741-12)")
    eq_(packaging[8]["marketing_start_date"], "20110214")
    eq_(packaging[8]["sample"], False)


def test_active_ingredients():
    meta, results = fetch("/drug/ndc.json?search=product_ndc:0942-6316")

    eq_(len(results), 1)

    ndc = results[0]
    active_ingredients = sorted(ndc["active_ingredients"], key=lambda k: k["name"])
    eq_(len(active_ingredients), 5)

    eq_(active_ingredients[0]["name"], "ADENINE")
    eq_(active_ingredients[0]["strength"], "19.3 mg/70mL")

    eq_(active_ingredients[1]["name"], "ANHYDROUS CITRIC ACID")
    eq_(active_ingredients[1]["strength"], "209 mg/70mL")

    eq_(active_ingredients[2]["name"], "DEXTROSE MONOHYDRATE")
    eq_(active_ingredients[2]["strength"], "2.23 g/70mL")

    eq_(active_ingredients[3]["name"], "SODIUM PHOSPHATE, MONOBASIC, MONOHYDRATE")
    eq_(active_ingredients[3]["strength"], "155 mg/70mL")

    eq_(active_ingredients[4]["name"], "TRISODIUM CITRATE DIHYDRATE")
    eq_(active_ingredients[4]["strength"], "1.84 g/70mL")


if __name__ == "__main__":
    all_functions = inspect.getmembers(sys.modules[__name__], inspect.isfunction)
    for key, func in all_functions:
        if key.find("test_") > -1:
            func()
