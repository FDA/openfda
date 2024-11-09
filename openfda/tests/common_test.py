import random
import string
import unittest
from openfda import common
from six.moves import range


class TestCommonMethods(unittest.TestCase):

    def test_extract_date(self):
        assert common.extract_date("20121110") == "2012-11-10"
        assert common.extract_date("201211103422") == "2012-11-10"
        assert common.extract_date("20121610") == "2012-01-10"
        assert common.extract_date("20120010") == "2012-01-10"
        assert common.extract_date("20121132") == "2012-11-01"
        assert common.extract_date("2012000001") == "2012-01-01"
        assert common.extract_date("20561132") == "1900-11-01"
        assert common.extract_date("18001132") == "1900-11-01"
        assert common.extract_date("") == "1900-01-01"
        assert common.extract_date(None) == "1900-01-01"

    def test_shell_cmd(self):
        tmpFile = "/tmp/" + (
            "".join(
                random.choice(string.ascii_uppercase + string.digits) for _ in range(32)
            )
        )
        common.shell_cmd("touch %(tmpFile)s" % locals())
        assert len(common.shell_cmd("ls %(tmpFile)s" % locals())) > 0
        assert common.shell_cmd("ls %(tmpFile)s" % locals()).startswith(
            tmpFile.encode()
        )

    def test_shell_cmd_quiet(self):
        tmpFile = "/tmp/" + (
            "".join(
                random.choice(string.ascii_uppercase + string.digits) for _ in range(32)
            )
        )
        common.shell_cmd_quiet("touch %(tmpFile)s" % locals())
        assert common.shell_cmd_quiet("ls %(tmpFile)s" % locals()).startswith(
            tmpFile.encode()
        )
