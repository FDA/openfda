
from openfda.tests.api_test_helpers import *


EXACT_FIELDS = [
    "openfda.application_number.exact",
    "openfda.brand_name.exact",
    "openfda.generic_name.exact",
    "openfda.is_original_packager.exact",
    "openfda.manufacturer_name.exact",
    "openfda.nui.exact",
    "openfda.package_ndc.exact",
    "openfda.pharm_class_cs.exact",
    "openfda.pharm_class_epc.exact",
    "openfda.pharm_class_moa.exact",
    "openfda.pharm_class_pe.exact",
    "openfda.product_ndc.exact",
    "openfda.product_type.exact",
    "openfda.route.exact",
    "openfda.rxcui.exact",
    "openfda.spl_id.exact",
    "openfda.spl_set_id.exact",
    "openfda.substance_name.exact",
    "openfda.unii.exact",
    "openfda.upc.exact",
]


def test_exact_field_queries_after_fda_253():
    for field in EXACT_FIELDS:
        print(field)
        meta, counts = fetch_counts("/drug/label.json?count=%s&limit=1" % field)
        eq_(len(counts), 1)


def test_not_analyzed_0():
    assert_total("/drug/label.json?search=id:d08e6fbf-9162-47cd-9cf6-74ea24d48214", 1)


def test_exact_count_0():
    assert_count_top(
        "/drug/label.json?count=openfda.brand_name.exact",
        ["Oxygen", "Ibuprofen", "Gabapentin", "Allergy Relief", "Hand Sanitizer"],
    )


def test_date_1():
    assert_total("/drug/label.json?search=effective_time:20140320", 15)


def test_date_range_2():
    assert_total(
        "/drug/label.json?search=effective_time:([20140320+TO+20141231])", 3900
    )
    assert_total(
        "/drug/label.json?search=effective_time:([2014-03-20+TO+2014-12-31])", 3900
    )


def test_openfda_3():
    assert_total("/drug/label.json?search=openfda.is_original_packager:true", 53000)


def test_package_ndc_unii():
    assert_unii(
        "/drug/label.json?search=openfda.substance_name:Methoxsalen", ["U4VJ29L7BQ"]
    )


def test_drug_label_unii():
    assert_unii(
        "/drug/label.json?search=openfda.substance_name:ofloxacin", ["A4P49JAZ9H"]
    )


def test_unii_multiple_substances():
    assert_unii(
        "/drug/label.json?search=set_id:0002a6ef-6e5e-4a34-95d5-ebaab222496f",
        ["39M11XPH03", "1G4GK01F67", "S7V92P67HO", "X7BCI5P86H", "6YR2608RSU"],
    )


def test_count_after_es2_migration():
    assert_count_top(
        "/drug/label.json?count=openfda.product_type.exact",
        ["HUMAN OTC DRUG", "HUMAN PRESCRIPTION DRUG"],
        2,
    )

    meta, counts = fetch_counts("/drug/label.json?count=openfda.product_type.exact")
    assert_greater(counts[0].count, counts[1].count)


def test_multiple_upc():
    # Todo: fix the build
    # meta, results = fetch(
    # '/drug/label.json?search=openfda.spl_set_id.exact:0b0be196-0c62-461c-94f4-9a35339b4501')
    meta, results = fetch(
        "/drug/label.json?search=openfda.spl_set_id.exact:003161f8-aed4-4d04-b6a3-6afc56e2817a"
    )
    eq_(len(results), 1)
    upcs = results[0]["openfda"]["upc"]
    upcs.sort()
    eq_(["0860005203642", "0860005203659"], upcs)


def test_single_upc():
    meta, results = fetch(
        "/drug/label.json?search=openfda.spl_set_id.exact:00006ebc-ec2b-406c-96b7-a3cc422e933f"
    )
    eq_(len(results), 1)
    upcs = results[0]["openfda"]["upc"]
    eq_(["0740640391006"], upcs)


def test_pharma_classes():
    meta, results = fetch(
        "/drug/label.json?search=openfda.spl_set_id.exact:15b0b56d-6188-4937-b541-902022e35b24"
    )
    eq_(len(results), 1)
    openfda = results[0]["openfda"]
    eq_(["IBUPROFEN"], openfda["generic_name"])
    eq_(["WK2XYI10QM"], openfda["unii"])
    eq_(["Cyclooxygenase Inhibitors [MoA]"], openfda["pharm_class_moa"])
    eq_(["Anti-Inflammatory Agents, Non-Steroidal [CS]"], openfda["pharm_class_cs"])
    eq_(["Nonsteroidal Anti-inflammatory Drug [EPC]"], openfda["pharm_class_epc"])

    meta, results = fetch(
        "/drug/label.json?search=openfda.spl_set_id.exact:143a5ba8-0ee9-43be-a0da-2c455c1a46bd"
    )
    eq_(len(results), 1)
    openfda = results[0]["openfda"]
    eq_(["BENZONATATE"], openfda["generic_name"])
    eq_(["5P4DHS6ENR"], openfda["unii"])
    eq_(
        ["Decreased Tracheobronchial Stretch Receptor Activity [PE]"],
        openfda["pharm_class_pe"],
    )
    eq_(["Non-narcotic Antitussive [EPC]"], openfda["pharm_class_epc"])

    meta, results = fetch(
        "/drug/label.json?search=openfda.unii.exact:10X0709Y6I&limit=1"
    )
    eq_(len(results), 1)
    openfda = results[0]["openfda"]
    eq_(["BISACODYL"], openfda["substance_name"])
    eq_(["10X0709Y6I"], openfda["unii"])
    eq_(
        [
            "Increased Large Intestinal Motility [PE]",
            "Stimulation Large Intestine Fluid/Electrolyte Secretion [PE]",
        ],
        sorted(openfda["pharm_class_pe"]),
    )
    eq_(["Stimulant Laxative [EPC]"], openfda["pharm_class_epc"])
