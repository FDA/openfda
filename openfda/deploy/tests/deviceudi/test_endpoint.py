# coding=utf-8
import inspect
import sys

from openfda.tests.api_test_helpers import *


def test_package_type():
    meta, results = fetch("/device/udi.json?search=identifiers.id:M20609070")
    eq_(len(results), 1)

    udi = results[0]
    eq_(udi["identifiers"][1]["package_type"], "Box")
    eq_(udi["identifiers"][1]["type"], "Package")


def test_size_value_as_string_fda82():
    meta, results = fetch("/device/udi.json?search=identifiers.id:10801741065450")
    eq_(len(results), 1)

    udi = results[0]
    eq_(udi["device_sizes"][0]["unit"], "Inch")
    eq_(udi["device_sizes"][0]["value"], "3/16")
    eq_(udi["device_sizes"][0]["type"], "Outer Diameter")


def test_booleans():
    # identifiers.package_status.exact
    meta, results = fetch("/device/udi.json?search=is_direct_marking_exempt:true")
    ok_(
        meta["results"]["total"]
        >= 51 + 10777 + 13555 + 2447 + 17088 + 3955 + 3383 + 6036 + 5168 + 2265 + 913
    )


def test_exact_fields():
    # identifiers.package_status.exact
    meta, results = fetch(
        '/device/udi.json?search=identifiers.package_status.exact:"In Commercial Distribution"'
    )
    ok_(meta["results"]["total"] >= 36642)

    meta, results = fetch(
        '/device/udi.json?search=identifiers.package_status.exact:"Not in Commercial Distribution"'
    )
    ok_(meta["results"]["total"] >= 79)

    meta, results = fetch(
        "/device/udi.json?search=identifiers.package_status.exact:Commercial DDistribution"
    )
    eq_(meta, None)
    eq_(results, None)


def test_date_fields():
    # package_discontinue_date
    meta, results = fetch(
        "/device/udi.json?search=identifiers.package_discontinue_date:20170430&limit=1"
    )
    eq_(meta["results"]["total"], 6)
    meta, results = fetch(
        '/device/udi.json?search=identifiers.package_discontinue_date:"2017-04-30"&limit=1'
    )
    eq_(meta["results"]["total"], 6)

    udi = results[0]
    ok_(
        udi["identifiers"][0]["id"]
        in [
            "50699753498278",
            "00699753498273",
            "00850490007061",
            "00850490007054",
            "00850490007085",
            "00850490007078",
            "00863101000306",
            "10863101000303",
            "20863101000300",
            "50699753473626",
            "00751774077704",
            "00850490007078",
            "00492450303419",
            "00492450302887",
            "00699753473621",
        ]
    )

    # publish_date
    meta, results = fetch(
        "/device/udi.json?search=publish_date:[2014-12-31+TO+2015-01-03]&limit=100"
    )
    eq_(meta["results"]["total"], 4)
    meta, results = fetch(
        "/device/udi.json?search=publish_date:[20141231+TO+20150103]&limit=100"
    )
    eq_(meta["results"]["total"], 4)

    meta, results = fetch(
        "/device/udi.json?search=publish_date:[2014-05-01+TO+2014-06-30]&limit=100"
    )
    eq_(meta["results"]["total"], 3)
    meta, results = fetch(
        "/device/udi.json?search=publish_date:[20140501+TO+20140630]&limit=100"
    )
    eq_(meta["results"]["total"], 3)
    meta, results = fetch(
        "/device/udi.json?search=publish_date:[20140501%20TO%2020140630]&limit=100"
    )
    eq_(meta["results"]["total"], 3)
    meta, results = fetch(
        "/device/udi.json?search=publish_date:[20140501 TO 20140630]&limit=100"
    )
    eq_(meta["results"]["total"], 3)
    meta, results = fetch(
        '/device/udi.json?search=publish_date:([19900501 TO 20180630])+AND+commercial_distribution_end_date:["19900101" TO "20191231"]&limit=100'
    )
    ok_(meta["results"]["total"] > 13000)

    # commercial_distribution_end_date
    meta, results = fetch(
        "/device/udi.json?search=commercial_distribution_end_date:[2014-01-01+TO+2014-12-31]&limit=100"
    )
    eq_(meta["results"]["total"], 145)


def test_identifiers():
    meta, results = fetch(
        '/device/udi.json?search=brand_name:"Cardiovascular Procedure Kit"+AND+version_or_model_number.exact:"66483"'
    )
    eq_(len(results), 1)

    udi = results[0]
    eq_(udi["identifiers"][0]["id"], "50699753498278")
    eq_(udi["identifiers"][0]["type"], "Package")
    eq_(udi["identifiers"][0]["issuing_agency"], "GS1")
    eq_(udi["identifiers"][0]["unit_of_use_id"], "00699753498273")
    eq_(udi["identifiers"][0]["quantity_per_package"], "4")
    eq_(udi["identifiers"][0]["package_discontinue_date"], "2017-05-01")
    eq_(udi["identifiers"][0]["package_status"], "Not in Commercial Distribution")

    eq_(udi["identifiers"][1]["id"], "00699753498273")
    eq_(udi["identifiers"][1]["type"], "Primary")
    eq_(udi["identifiers"][1]["issuing_agency"], "GS1")
    eq_(udi["identifiers"][1].get("unit_of_use_id"), None)
    eq_(udi["identifiers"][1].get("quantity_per_package"), None)
    eq_(udi["identifiers"][1].get("package_discontinue_date"), None)
    eq_(udi["identifiers"][1].get("package_status"), None)


def test_openfda():
    meta, results = fetch("/device/udi.json?search=identifiers.id:00085412096094")
    eq_(len(results), 1)

    udi = results[0]

    # Fei and premarket data is supressed for initial release.
    eq_(udi.get("fei_number"), None)
    eq_(udi.get("pma_submissions"), None)
    eq_(udi.get("pma_submissions"), None)
    eq_(udi.get("pma_submissions"), None)

    eq_(udi["product_codes"][0]["code"], "LMF")
    eq_(udi["product_codes"][0]["name"], "Agent, Absorbable Hemostatic, Collagen Based")
    eq_(udi["product_codes"][0]["openfda"]["device_class"], "3")
    eq_(
        udi["product_codes"][0]["openfda"]["device_name"].lower(),
        "agent, absorbable hemostatic, collagen based",
    )
    eq_(
        udi["product_codes"][0]["openfda"]["medical_specialty_description"],
        "General, Plastic Surgery",
    )
    eq_(udi["product_codes"][0]["openfda"]["regulation_number"], "878.4490")
    eq_(udi["product_codes"][0]["openfda"].get("registration_number"), None)
    eq_(udi["product_codes"][0]["openfda"].get("fei_number"), None)
    eq_(udi["product_codes"][0]["openfda"].get("pma_number"), None)


def test_storage():
    meta, results = fetch("/device/udi.json?search=identifiers.id:00887517614384")
    eq_(len(results), 1)

    udi = results[0]
    eq_(udi["storage"][0]["type"], "Storage Environment Temperature")
    eq_(udi["storage"][0]["high"]["unit"], "Degrees Celsius")
    eq_(udi["storage"][0]["high"]["value"], "30")
    eq_(udi["storage"][0]["low"]["unit"], "Degrees Celsius")
    eq_(udi["storage"][0]["low"]["value"], "15")

    # KEEP DRY
    meta, results = fetch("/device/udi.json?search=identifiers.id:00886874103951")
    eq_(len(results), 1)

    udi = results[0]
    eq_(udi["storage"][0]["special_conditions"], "KEEP DRY")


def test_commercial_distribution_end_date():
    meta, results = fetch(
        '/device/udi.json?search=commercial_distribution_status:"Not+in+Commercial+Distribution"+AND+brand_name:DURACUFF+AND+identifiers.id:04051948012811'
    )
    eq_(len(results), 1)

    udi = results[0]
    eq_(udi.get("commercial_distribution_end_date"), "2021-10-12")
    eq_(udi["commercial_distribution_status"], "Not in Commercial Distribution")


def test_catalog_number():
    meta, results = fetch("/device/udi.json?search=catalog_number:521554BE2")
    eq_(len(results), 1)

    udi = results[0]
    eq_(udi["catalog_number"], "14-521554BE2")

    # Make sure exact search also works.
    meta, results = fetch("/device/udi.json?search=catalog_number.exact:14-521554BE2")
    eq_(len(results), 1)

    udi = results[0]
    eq_(udi["catalog_number"], "14-521554BE2")


def test_device_sizes():
    meta, results = fetch("/device/udi.json?search=identifiers.id:00886874104361")
    eq_(len(results), 1)

    udi = results[0]
    eq_(udi["device_sizes"][0]["unit"], "Millimeter")
    eq_(udi["device_sizes"][0]["value"], "4.3")
    eq_(udi["device_sizes"][0]["type"], "Depth")

    meta, results = fetch("/device/udi.json?search=identifiers.id:00888480590095")
    eq_(len(results), 1)

    udi = results[0]
    eq_(udi["device_sizes"][0].get("unit"), None)
    eq_(udi["device_sizes"][0].get("value"), None)
    eq_(udi["device_sizes"][0]["type"], "Device Size Text, specify")
    eq_(udi["device_sizes"][0]["text"], "4.5 X 24MM BLUNT FXD SCREW 2PK")


def test_CoRoent():
    meta, results = fetch(
        "/device/udi.json?search=identifiers.id:00887517567062+AND+identifiers.type:Primary+AND+identifiers.issuing_agency.exact:GS1"
    )
    eq_(len(results), 1)

    udi = results[0]
    eq_(udi["record_status"], "Published")
    eq_(udi["publish_date"], "2015-10-24")
    eq_(udi.get("commercial_distribution_end_date"), None)
    eq_(udi["commercial_distribution_status"], "In Commercial Distribution")
    eq_(udi["identifiers"][0]["id"], "00887517567062")
    eq_(udi["identifiers"][0]["type"], "Primary")
    eq_(udi["identifiers"][0]["issuing_agency"], "GS1")
    eq_(udi["brand_name"], "CoRoent")
    eq_(udi["version_or_model_number"], "5163284")
    eq_(udi.get("catalog_number"), None)
    eq_(udi["device_count_in_base_package"], "1")
    eq_(udi["device_description"], str("CoRoent Ant TLIF PEEK, 16x13x28mm 4°"))
    eq_(udi["is_direct_marking_exempt"], "false")
    eq_(udi["is_pm_exempt"], "false")
    eq_(udi["is_hct_p"], "false")
    eq_(udi["is_kit"], "false")
    eq_(udi["is_combination_product"], "false")
    eq_(udi["is_single_use"], "false")
    eq_(udi["has_lot_or_batch_number"], "true")
    eq_(udi["has_serial_number"], "false")
    eq_(udi["has_manufacturing_date"], "false")
    eq_(udi["has_expiration_date"], "false")
    eq_(udi["has_donation_id_number"], "false")
    eq_(udi["is_labeled_as_nrl"], "false")
    eq_(udi["is_labeled_as_no_nrl"], "false")
    eq_(udi["mri_safety"], "Labeling does not contain MRI Safety Information")
    eq_(udi["is_rx"], "true")
    eq_(udi["is_otc"], "false")
    eq_(udi["customer_contacts"][0]["phone"], "+1(858)909-1800")
    eq_(udi["customer_contacts"][0]["email"], "RA_UDI@nuvasive.com")
    eq_(udi["customer_contacts"][0].get("ext"), None)
    eq_(udi["gmdn_terms"][0]["name"], "Metallic spinal fusion cage, non-sterile")
    eq_(
        udi["gmdn_terms"][0]["definition"],
        "A non-sterile device intended to help fuse segments of the spine to treat anatomical abnormalities of the vertebrae, typically due to degenerative intervertebral disks [i.e., degenerative disc disease (DDD)]. The device is typically designed as a small, hollow and/or porous, threaded or fenestrated cylinder (or other geometric form) made of metal [usually titanium (Ti)] that is implanted between the bones or bone grafts of the spine, to provide mechanical stability and sufficient space for therapeutic spinal bone fusion to occur. Disposable devices associated with implantation may be included. This device must be sterilized prior to use.",
    )

    eq_(udi["product_codes"][0]["code"], "MAX")
    eq_(
        udi["product_codes"][0]["name"],
        "Intervertebral fusion device with bone graft, lumbar",
    )
    eq_(udi["product_codes"][0]["openfda"]["device_class"], "2")
    eq_(
        udi["product_codes"][0]["openfda"]["device_name"],
        "Intervertebral Fusion Device With Bone Graft, Lumbar",
    )
    eq_(
        udi["product_codes"][0]["openfda"]["medical_specialty_description"],
        "Orthopedic",
    )
    eq_(udi["product_codes"][0]["openfda"]["regulation_number"], "888.3080")

    eq_(udi["product_codes"][1]["code"], "MQP")
    eq_(udi["product_codes"][1]["name"], "SPINAL VERTEBRAL BODY REPLACEMENT DEVICE")
    eq_(udi["product_codes"][1]["openfda"]["device_class"], "2")
    eq_(
        udi["product_codes"][1]["openfda"]["device_name"],
        "Spinal Vertebral Body Replacement Device",
    )
    eq_(
        udi["product_codes"][1]["openfda"]["medical_specialty_description"],
        "Orthopedic",
    )
    eq_(udi["product_codes"][1]["openfda"]["regulation_number"], "888.3060")

    eq_(udi.get("device_sizes"), None)
    eq_(udi.get("storage"), None)
    eq_(udi["sterilization"]["is_sterile"], "false")
    eq_(udi["sterilization"]["is_sterilization_prior_use"], "true")
    eq_(
        udi["sterilization"]["sterilization_methods"],
        "Moist Heat or Steam Sterilization",
    )

    # Fei and premarket data is supressed for initial release.
    eq_(udi.get("fei_number"), None)
    eq_(udi.get("pma_submissions"), None)
    eq_(udi.get("pma_submissions"), None)
    eq_(udi.get("pma_submissions"), None)


def test_total_count():
    assert_total("/device/udi.json", 533467)


def test_product_code_count():
    assert_count_top(
        "/device/udi.json?count=product_codes.code.exact", ["MNI", "MNH", "KWP", "KWQ"]
    )


def test_mri_safety_count():
    assert_count_top(
        "/device/udi.json?count=mri_safety.exact",
        [
            "Labeling does not contain MRI Safety Information",
            "MR Conditional",
            "MR Unsafe",
            "MR Safe",
        ],
    )


if __name__ == "__main__":
    all_functions = inspect.getmembers(sys.modules[__name__], inspect.isfunction)
    for key, func in all_functions:
        if key.find("test_") > -1:
            func()
