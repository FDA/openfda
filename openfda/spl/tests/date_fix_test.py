#!/usr/bin/python
from openfda.tests.api_test_helpers import *
from arrow.parser import ParserError
import openfda.spl.fix_date as fix_date


# Validates that combinations of invalid dates are corrected.
def test_invalid_dates():
    eq_(fix_date.validate_date("2011-04-00"), "2011-04-01")
    eq_(fix_date.validate_date("2011-05-32"), "2011-05-31")
    eq_(fix_date.validate_date("2011-04-31"), "2011-04-30")
    eq_(fix_date.validate_date("2011-04-32"), "2011-04-30")
    eq_(fix_date.validate_date("2011-02-30"), "2011-02-28")
    eq_(fix_date.validate_date("2016-02-30"), "2016-02-29")
    eq_(fix_date.validate_date("2011-02-330"), "2011-02-28")
    eq_(fix_date.validate_date("2011-00-30"), "2011-01-30")
    eq_(fix_date.validate_date("2011-22-30"), "2011-12-30")
    eq_(fix_date.validate_date("2011-223-30"), "2011-12-30")
    eq_(fix_date.validate_date("10-12-30"), "0010-12-30")
    eq_(fix_date.validate_date("1-0-0"), "0001-01-01")
    eq_(fix_date.validate_date("2019-12-30"), "2019-12-30")
    eq_(fix_date.validate_date("2019-13-0"), "2019-12-01")


# Verifies that alpha character raise an exception.
def test_alpha_character():
    try:
        fix_date.validate_date("10-12-a")
        fail("ParserError expected")
    except ParserError:
        pass
