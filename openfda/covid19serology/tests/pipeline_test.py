#!/usr/bin/env python
# coding=utf-8
import shutil
import tempfile
import unittest
from pickle import *
import leveldb

import openfda.ndc.pipeline
from openfda.ndc.pipeline import *
from openfda.covid19serology.pipeline import SerologyCSV2JSON
from openfda.tests.api_test_helpers import *


class SerologyPipelineTests(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        openfda.covid19serology.pipeline.SEROLOGY_TEST_SYNC_DIR = join(
            self.test_dir, "covid19serology/s3_sync"
        )
        openfda.covid19serology.pipeline.SEROLOGY_TEST_JSON_DB_DIR = join(
            self.test_dir, "covid19serology/json.db"
        )

        os.makedirs(openfda.covid19serology.pipeline.SEROLOGY_TEST_SYNC_DIR)
        shutil.copyfile(
            os.path.join(
                dirname(os.path.abspath(__file__)),
                "20200421_Euroimmun_SARS-COV-2_ELISA_(IgG)_results.csv",
            ),
            os.path.join(
                openfda.covid19serology.pipeline.SEROLOGY_TEST_SYNC_DIR, "input.csv"
            ),
        )

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_csv_to_json(self):
        csv2json = SerologyCSV2JSON()
        csv2json.run()

        ldb = leveldb.LevelDB(
            os.path.join(csv2json.output().path, "shard-00000-of-00001.db")
        )
        data = list(ldb.RangeIter())
        eq_(110, len(data))

        # Verify base logic.
        row = loads(data[0][1])
        eq_("Euroimmun", row.get("manufacturer"))
        eq_("SARS-COV-2 ELISA (IgG)", row.get("device"))
        eq_("4/21/2020", row.get("date_performed"))
        eq_("E200330DT", row.get("lot_number"))
        eq_("Panel 1", row.get("panel"))
        eq_("1", row.get("sample_no"))
        eq_("C0001", row.get("sample_id"))
        eq_("Serum", row.get("type"))
        eq_("Negatives", row.get("group"))
        eq_("NA", row.get("days_from_symptom"))
        eq_("NA", row.get("igm_result"))
        eq_("Negative", row.get("igg_result"))
        eq_("NA", row.get("iga_result"))
        eq_("NA", row.get("pan_result"))
        eq_("NA", row.get("igm_igg_result"))
        eq_("Pass", row.get("control"))
        eq_("0", row.get("igm_titer"))
        eq_("0", row.get("igg_titer"))
        eq_("Negative", row.get("igm_truth"))
        eq_("Negative", row.get("igg_truth"))
        eq_("Negative", row.get("antibody_truth"))
        eq_("NA", row.get("igm_agree"))
        eq_("TN", row.get("igg_agree"))
        eq_("NA", row.get("iga_agree"))
        eq_("NA", row.get("pan_agree"))
        eq_("NA", row.get("igm_igg_agree"))
        eq_("TN", row.get("antibody_agree"))

        # Verify some variations.
        row = loads(data[55][1])
        eq_("29", row.get("days_from_symptom"))
        eq_("400", row.get("igm_titer"))
        eq_("1600", row.get("igg_titer"))


def main(argv):
    unittest.main(argv=argv)


if __name__ == "__main__":
    unittest.main()
