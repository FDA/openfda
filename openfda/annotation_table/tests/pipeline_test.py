#!/usr/bin/env python
# coding=utf-8
import tempfile
import unittest

import simplejson

import openfda.annotation_table.pipeline
from openfda.annotation_table.pipeline import *
from openfda.spl.pipeline import *
from openfda.tests.api_test_helpers import *

RUN_DIR = dirname(os.path.abspath(__file__))


class AnnotationPipelineTests(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        openfda.annotation_table.pipeline.data_dir = os.path.join(
            self.test_dir, openfda.annotation_table.pipeline.data_dir
        )
        openfda.annotation_table.pipeline.BASE_DIR = os.path.join(
            self.test_dir, openfda.annotation_table.pipeline.BASE_DIR
        )
        openfda.annotation_table.pipeline.SPL_S3_DIR = os.path.join(
            self.test_dir, openfda.annotation_table.pipeline.SPL_S3_DIR
        )
        openfda.annotation_table.pipeline.TMP_DIR = os.path.join(
            self.test_dir, openfda.annotation_table.pipeline.TMP_DIR
        )
        openfda.annotation_table.pipeline.SPL_SET_ID_INDEX = join(
            openfda.annotation_table.pipeline.BASE_DIR, "spl_index.db"
        )

        openfda.spl.pipeline.SPL_S3_LOCAL_DIR = os.path.join(
            self.test_dir, openfda.spl.pipeline.SPL_S3_LOCAL_DIR
        )
        openfda.spl.pipeline.SPL_INDEX_DIR = os.path.join(
            self.test_dir, openfda.spl.pipeline.SPL_INDEX_DIR
        )

        common.shell_cmd("mkdir -p %s", openfda.annotation_table.pipeline.data_dir)
        common.shell_cmd("mkdir -p %s", openfda.annotation_table.pipeline.BASE_DIR)
        common.shell_cmd("mkdir -p %s", openfda.annotation_table.pipeline.TMP_DIR)

        common.shell_cmd("mkdir -p %s", openfda.spl.pipeline.SPL_S3_LOCAL_DIR)
        common.shell_cmd("mkdir -p %s", openfda.spl.pipeline.SPL_INDEX_DIR)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_multiple_barcodes_handling(self):
        common.shell_cmd(
            "cp -rf %s/* %s",
            os.path.join(RUN_DIR, "data/multi_upc_test/annotation"),
            openfda.annotation_table.pipeline.BASE_DIR,
        )
        common.shell_cmd(
            "cp -rf %s/* %s",
            os.path.join(RUN_DIR, "data/multi_upc_test/spl"),
            (dirname(openfda.spl.pipeline.SPL_S3_LOCAL_DIR)),
        )

        RXNorm2JSON().run()
        UNIIHarmonizationJSON().run()
        UNII2JSON().run()
        NDC2JSON().run()

        splToIndex = DetermineSPLToIndex()
        splToIndex.run()

        spl2json = SPL2JSON()
        spl2json.spl_path = openfda.spl.pipeline.SPL_S3_LOCAL_DIR
        spl2json.run()

        SPLSetIDIndex().run()
        GenerateCurrentSPLJSON().run()
        UpcXml2JSON().run()
        CombineHarmonization().run()
        annotate_json = AnnotateJSON()
        annotate_json.run()

        # Make sure there are now 3, not 1, UPCs for this SPL ID stored in LevelDB.
        db = parallel.ShardedDB.open(
            join(
                openfda.annotation_table.pipeline.BASE_DIR,
                openfda.annotation_table.pipeline.UPC_EXTRACT_DB,
            )
        )
        db_iter = db.range_iter(None, None)
        (k, v) = next(db_iter)
        ok_(isinstance(v, list))
        eq_(k, "64f8040f-938d-4236-8e22-c838c9b5f8da")
        eq_(len(v), 3)
        sorted(v, key=lambda upc: upc["upc"])
        eq_(v[0]["upc"], "0300694200305")
        eq_(v[1]["upc"], "0300694210304")
        eq_(v[2]["upc"], "0300694220303")

        # Make sure harmonized.json also contains 3 UPCs.
        harmonized = open(
            join(openfda.annotation_table.pipeline.BASE_DIR, "harmonized.json"), "r"
        )
        for jsonLine in harmonized:
            obj = simplejson.loads(jsonLine)
            eq_(obj["id"], "64f8040f-938d-4236-8e22-c838c9b5f8da")
            eq_(obj["upc"], ["0300694200305", "0300694210304", "0300694220303"])

        # Make sure the drug label JSON got openFDA section showing all 3 UPCs.
        db = parallel.ShardedDB.open(annotate_json.output().path)
        db_iter = db.range_iter(None, None)
        (k, v) = next(db_iter)
        v["openfda"]["upc"].sort()
        eq_(v["openfda"]["upc"], ["0300694200305", "0300694210304", "0300694220303"])


def main(argv):
    unittest.main(argv=argv)


if __name__ == "__main__":
    unittest.main()
