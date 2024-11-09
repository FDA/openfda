
from openfda.tests.api_test_helpers import *


def test_not_analyzed_0():
    assert_total("/device/pma.json?search=openfda.regulation_number:876.5270", 290)


def test_exact_count_0():
    assert_count_top(
        "/device/pma.json?count=supplement_type.exact",
        [
            "30-Day Notice",
            "Normal 180 Day Track",
            "Real-Time Process",
            "",
            "135 Review Track For 30-Day Notice",
        ],
    )


def test_date_1():
    assert_total("/device/pma.json?search=decision_date:20131122", 11)


def test_date_range_2():
    assert_total("/device/pma.json?search=decision_date:([20131122+TO+20131231])", 305)


def test_openfda_3():
    assert_total("/device/pma.json?search=openfda.registration_number:3006630150", 836)
