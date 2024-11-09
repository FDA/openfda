
from openfda.tests.api_test_helpers import *


def test_not_analyzed_0():
    assert_total(
        "/device/registrationlisting.json?search=products.openfda.device_class:2",
        111749,
    )


def test_exact_count_0():
    assert_count_top(
        "/device/registrationlisting.json?count=registration.name.exact",
        [
            "Sterigenics U.S., LLC",
            "Isomedix Operations, Inc.",
            "Sterigenics US LLC",
            "ISOMEDIX OPERATIONS INC.",
            "C.R. BARD, INC.",
        ],
    )


def test_exact_count_1():
    assert_count_top(
        "/device/registrationlisting.json?count=proprietary_name.exact",
        ["Integra", "Miltex", "PILLING", "Aesculap", "Jarit"],
    )


def test_date_1():
    assert_total(
        "/device/registrationlisting.json?search=products.created_date:20080117", 150
    )


def test_date_range_2():
    assert_total(
        "/device/registrationlisting.json?search=products.created_date:([20080117+TO+20081231])",
        14000,
    )


def test_openfda_3():
    assert_total(
        "/device/registrationlisting.json?search=products.openfda.regulation_number:876.1500",
        3912,
    )
