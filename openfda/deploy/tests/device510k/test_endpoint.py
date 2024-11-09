from openfda.tests.api_test_helpers import *


def test_openfda_section():
    meta, results = fetch(
        "/device/510k.json?search=openfda.registration_number:3006630150"
    )
    assert results[0]["openfda"], results


def test_not_analyzed():
    assert_total("/device/510k.json?search=openfda.regulation_number:874.3450", 70)


def test_date():
    assert_total("/device/510k.json?search=date_received:20000410", 30)


def test_date_range():
    assert_total(
        "/device/510k.json?search=date_received:([20000410+TO+20001231])", 2384
    )


def test_exact_count():
    assert_count_top(
        "/device/510k.json?count=advisory_committee_description.exact",
        [
            "Cardiovascular",
            "General Hospital",
            "Clinical Chemistry",
            "General, Plastic Surgery",
        ],
    )


def test_openfda_count():
    assert_count_top(
        "/device/510k.json?count=openfda.device_name.exact",
        [
            "Powered Laser Surgical Instrument",
            "Latex Patient Examination Glove",
            "Electrosurgical, Cutting & Coagulation & Accessories",
            "System, Image Processing, Radiological",
            "Hearing Aid, Air Conduction",
            "Laparoscope, General & Plastic Surgery",
        ],
    )
