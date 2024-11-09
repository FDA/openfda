#!/usr/bin/env python
# coding=utf-8
import shutil
import tempfile
import unittest
from os.path import dirname

import leveldb

import openfda.drugsfda.pipeline
from openfda.drugsfda.pipeline import *
from openfda.tests.api_test_helpers import *


class DrugsFDAPipelineTests(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        openfda.drugsfda.pipeline.BASE_DIR = self.test_dir
        openfda.annotation_table.pipeline.BASE_DIR = self.test_dir
        openfda.drugsfda.pipeline.EXTRACTED_DIR = join(self.test_dir, "extracted/")
        openfda.drugsfda.pipeline.RAW_DATA_FILE = join(
            self.test_dir, "raw/drugsatfda.zip"
        )
        openfda.drugsfda.pipeline.PRODUCTS_DB = join(self.test_dir, "json/products.db")
        openfda.drugsfda.pipeline.APPLICATIONS_DB = join(
            self.test_dir, "json/applications.db"
        )
        openfda.drugsfda.pipeline.APPLICATIONS_DOCS_DB = join(
            self.test_dir, "json/applicationsdocs.db"
        )
        openfda.drugsfda.pipeline.SUBMISSIONS_DB = join(
            self.test_dir, "json/submissions.db"
        )
        openfda.drugsfda.pipeline.SUBMISSION_PROPERTY_TYPE_DB = join(
            self.test_dir, "json/submissionpropertytype.db"
        )
        openfda.drugsfda.pipeline.MARKETING_STATUS_DB = join(
            self.test_dir, "json/marketingstatus.db"
        )
        openfda.drugsfda.pipeline.TE_DB = join(self.test_dir, "json/te.db")
        openfda.drugsfda.pipeline.MERGED_DB = join(self.test_dir, "json/merged.db")
        openfda.drugsfda.pipeline.ANNOTATED_DB = join(
            self.test_dir, "json/annotated.db"
        )
        print("Temp dir is " + self.test_dir)

        os.mkdir(dirname(os.path.abspath(openfda.drugsfda.pipeline.RAW_DATA_FILE)))
        shutil.copyfile(
            os.path.join(dirname(os.path.abspath(__file__)), "drugsatfda.zip"),
            openfda.drugsfda.pipeline.RAW_DATA_FILE,
        )

        zip_filename = os.path.join(
            dirname(os.path.abspath(__file__)), "harmonized.json.zip"
        )
        output_dir = self.test_dir
        os.system("unzip -o %(zip_filename)s -d %(output_dir)s" % locals())

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_annotated_json(self):
        ExtractDrugsFDAFiles().run()
        Applications2JSON().run()
        Products2JSON().run()
        MarketingStatus2JSON().run()
        TE2JSON().run()
        Submissions2JSON().run()
        SubmissionPropertyType2JSON().run()
        ApplicationsDocs2JSON().run()
        MergeAll().run()
        AnnotateDrugsFDA().run()
        ldb = leveldb.LevelDB(
            os.path.join(AnnotateDrugsFDA().output().path, "shard-00000-of-00001.db")
        )
        data = list(ldb.RangeIter())

        # We should have exactly 23267 records in the DB
        eq_(len(data), 23267)


def main(argv):
    unittest.main(argv=argv)


if __name__ == "__main__":
    unittest.main()
