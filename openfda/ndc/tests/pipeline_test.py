#!/usr/bin/env python
# coding=utf-8

import math
import pickle
import shutil
import tempfile
import unittest

import leveldb

import openfda.ndc.pipeline
from openfda.ndc.pipeline import *
from openfda.tests.api_test_helpers import *


class NDCPipelineTests(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        openfda.ndc.pipeline.BASE_DIR = self.test_dir
        openfda.ndc.pipeline.RAW_DIR = join(openfda.ndc.pipeline.BASE_DIR, "raw")
        openfda.ndc.pipeline.MERGED_PACKAGES = join(
            openfda.ndc.pipeline.BASE_DIR, "merge/package.csv"
        )
        openfda.ndc.pipeline.MERGED_PRODUCTS = join(
            openfda.ndc.pipeline.BASE_DIR, "merge/product.csv"
        )
        openfda.ndc.pipeline.NDC_PACKAGE_DB = join(
            openfda.ndc.pipeline.BASE_DIR, "json/package.db"
        )
        openfda.ndc.pipeline.NDC_PRODUCT_DB = join(
            openfda.ndc.pipeline.BASE_DIR, "json/product.db"
        )
        openfda.ndc.pipeline.NDC_MERGE_DB = join(
            openfda.ndc.pipeline.BASE_DIR, "json/merged.db"
        )

        print(("Temp dir is " + self.test_dir))

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_merge_product_with_packaging(self):
        os.mkdir(os.path.join(self.test_dir, "extracted"))
        shutil.copyfile(
            os.path.join(dirname(os.path.abspath(__file__)), "product_trimmed.txt"),
            os.path.join(self.test_dir, "extracted/product.txt"),
        )
        shutil.copyfile(
            os.path.join(
                dirname(os.path.abspath(__file__)), "unfinished_product_trimmed.txt"
            ),
            os.path.join(self.test_dir, "extracted/unfinished_product.txt"),
        )
        shutil.copyfile(
            os.path.join(dirname(os.path.abspath(__file__)), "package_trimmed.txt"),
            os.path.join(self.test_dir, "extracted/package.txt"),
        )
        shutil.copyfile(
            os.path.join(
                dirname(os.path.abspath(__file__)), "unfinished_package_trimmed.txt"
            ),
            os.path.join(self.test_dir, "extracted/unfinished_package.txt"),
        )

        merge = MergeProductNDC()
        merge.run()

        merge = MergePackageNDC()
        merge.run()

        toJSON = NDCPackage2JSON()
        toJSON.run()

        toJSON = NDCProduct2JSON()
        toJSON.run()

        merge = MergeProductsAndPackaging()
        merge.run()

        ldb = leveldb.LevelDB(
            os.path.join(merge.output().path, "shard-00000-of-00001.db")
        )
        data = list(ldb.RangeIter())

        # Verify base logic.
        pkg = pickle.loads(data[0][1])
        eq_("0002-0800_ab2a5006-1880-4b6c-9640-dc2ec1066449", pkg.get("product_id"))
        eq_("ab2a5006-1880-4b6c-9640-dc2ec1066449", pkg.get("spl_id"))
        eq_("0002-0800", pkg.get("product_ndc"))
        eq_("HUMAN OTC DRUG", pkg.get("product_type"))
        eq_("Sterile Diluent", pkg.get("brand_name"))
        eq_("Sterile Diluent", pkg.get("brand_name_base"))
        eq_(None, pkg.get("brand_name_suffix"))
        eq_("diluent", pkg.get("generic_name"))
        eq_("INJECTION, SOLUTION", pkg.get("dosage_form"))
        eq_(["SUBCUTANEOUS"], pkg.get("route"))
        eq_("19870710", pkg.get("marketing_start_date"))
        eq_(None, pkg.get("marketing_end_date"))
        eq_("NDA", pkg.get("marketing_category"))
        eq_("NDA018781", pkg.get("application_number"))
        eq_("Eli Lilly and Company", pkg.get("labeler_name"))
        eq_([{"strength": "1 mL/mL", "name": "WATER"}], pkg.get("active_ingredients"))
        eq_(None, pkg.get("strength"))
        eq_(None, pkg.get("unit"))
        eq_(None, pkg.get("pharm_class"))
        eq_(None, pkg.get("dea_schedule"))
        eq_("20191231", pkg.get("listing_expiration_date"))
        ok_(pkg.get("finished"))

        # Verify packaging got properly attached.
        eq_(1, len(pkg["packaging"]))
        eq_("0002-0800-01", pkg["packaging"][0]["package_ndc"])
        eq_(
            "1 VIAL in 1 CARTON (0002-0800-01)  > 10 mL in 1 VIAL",
            pkg["packaging"][0]["description"],
        )
        eq_("19870710", pkg["packaging"][0]["marketing_start_date"])
        eq_(None, pkg["packaging"][0].get("marketing_end_date"))
        ok_(not pkg["packaging"][0]["sample"])

        # Verify multiple packaging per product also work as expected
        pkg = pickle.loads(data[1][1])
        eq_("0002-1200_4bd46cbe-cdc1-4329-a8e7-22816bd7fc33", pkg.get("product_id"))

        eq_(2, len(pkg["packaging"]))
        eq_("0002-1200-30", pkg["packaging"][0]["package_ndc"])
        eq_(
            "1 VIAL, MULTI-DOSE in 1 CAN (0002-1200-30)  > 30 mL in 1 VIAL, MULTI-DOSE",
            pkg["packaging"][0]["description"],
        )
        eq_("20120601", pkg["packaging"][0]["marketing_start_date"])
        eq_("20190203", pkg["packaging"][0]["marketing_end_date"])
        ok_(pkg["packaging"][0]["sample"])
        eq_("0002-1200-50", pkg["packaging"][1]["package_ndc"])
        eq_(
            "1 VIAL, MULTI-DOSE in 1 CAN (0002-1200-50)  > 50 mL in 1 VIAL, MULTI-DOSE",
            pkg["packaging"][1]["description"],
        )
        eq_("20120601", pkg["packaging"][1]["marketing_start_date"])
        eq_(None, pkg["packaging"][1].get("marketing_end_date"))
        ok_(not pkg["packaging"][1]["sample"])

        # Verify unfinished drugs.
        pkg = pickle.loads(data[3][1])
        eq_("0002-0485_6d8a935c-fc7b-44c6-92d1-2337b959d1ce", pkg.get("product_id"))
        eq_("6d8a935c-fc7b-44c6-92d1-2337b959d1ce", pkg.get("spl_id"))
        eq_("0002-0485", pkg.get("product_ndc"))
        eq_("BULK INGREDIENT", pkg.get("product_type"))
        eq_(None, pkg.get("brand_name"))
        eq_(None, pkg.get("brand_name_base"))
        eq_(None, pkg.get("brand_name_suffix"))
        eq_("Insulin human", pkg.get("generic_name"))
        eq_("CRYSTAL", pkg.get("dosage_form"))
        eq_(None, pkg.get("route"))
        eq_("20150401", pkg.get("marketing_start_date"))
        eq_(None, pkg.get("marketing_end_date"))
        eq_("BULK INGREDIENT", pkg.get("marketing_category"))
        eq_(None, pkg.get("application_number"))
        eq_("Eli Lilly and Company", pkg.get("labeler_name"))
        eq_(
            [{"strength": "1 g/g", "name": "INSULIN HUMAN"}],
            pkg.get("active_ingredients"),
        )
        eq_(None, pkg.get("strength"))
        eq_(None, pkg.get("unit"))
        eq_(None, pkg.get("pharm_class"))
        eq_(None, pkg.get("dea_schedule"))
        eq_("20181231", pkg.get("listing_expiration_date"))
        ok_(not pkg.get("finished"))

        eq_(2, len(pkg["packaging"]))
        eq_("0002-0485-04", pkg["packaging"][0]["package_ndc"])
        eq_(
            "4 BOTTLE in 1 CONTAINER (0002-0485-04)  > 2000 g in 1 BOTTLE",
            pkg["packaging"][0]["description"],
        )
        eq_(None, pkg["packaging"][0].get("marketing_start_date"))
        eq_(None, pkg["packaging"][0].get("marketing_end_date"))
        eq_(None, pkg["packaging"][0].get("sample"))

        eq_("0002-0485-18", pkg["packaging"][1]["package_ndc"])
        eq_(
            "1 BOTTLE in 1 CONTAINER (0002-0485-18)  > 2000 g in 1 BOTTLE",
            pkg["packaging"][1]["description"],
        )
        eq_(None, pkg["packaging"][1].get("marketing_start_date"))
        eq_(None, pkg["packaging"][1].get("marketing_end_date"))
        eq_(None, pkg["packaging"][1].get("sample"))

    def test_product_ndc_to_json(self):
        os.mkdir(os.path.join(self.test_dir, "extracted"))
        shutil.copyfile(
            os.path.join(dirname(os.path.abspath(__file__)), "product_trimmed.txt"),
            os.path.join(self.test_dir, "extracted/product.txt"),
        )
        shutil.copyfile(
            os.path.join(
                dirname(os.path.abspath(__file__)), "unfinished_product_trimmed.txt"
            ),
            os.path.join(self.test_dir, "extracted/unfinished_product.txt"),
        )

        merge = MergeProductNDC()
        merge.run()
        mergedPath = os.path.join(self.test_dir, "merge/product.csv")

        toJSON = NDCProduct2JSON()
        toJSON.run()

        ldb = leveldb.LevelDB(
            os.path.join(toJSON.output().path, "shard-00000-of-00001.db")
        )
        data = list(ldb.RangeIter())

        # Verify base logic.
        pkg = pickle.loads(data[0][1])
        eq_("0002-0800_ab2a5006-1880-4b6c-9640-dc2ec1066449", pkg.get("product_id"))
        eq_("ab2a5006-1880-4b6c-9640-dc2ec1066449", pkg.get("spl_id"))
        eq_("0002-0800", pkg.get("product_ndc"))
        eq_("HUMAN OTC DRUG", pkg.get("product_type"))
        eq_("Sterile Diluent", pkg.get("brand_name"))
        eq_("Sterile Diluent", pkg.get("brand_name_base"))
        eq_(None, pkg.get("brand_name_suffix"))
        eq_("diluent", pkg.get("generic_name"))
        eq_("INJECTION, SOLUTION", pkg.get("dosage_form"))
        eq_(["SUBCUTANEOUS"], pkg.get("route"))
        eq_("19870710", pkg.get("marketing_start_date"))
        eq_(None, pkg.get("marketing_end_date"))
        eq_("NDA", pkg.get("marketing_category"))
        eq_("NDA018781", pkg.get("application_number"))
        eq_("Eli Lilly and Company", pkg.get("labeler_name"))
        eq_([{"strength": "1 mL/mL", "name": "WATER"}], pkg.get("active_ingredients"))
        eq_(None, pkg.get("strength"))
        eq_(None, pkg.get("unit"))
        eq_(None, pkg.get("pharm_class"))
        eq_(None, pkg.get("dea_schedule"))
        eq_("20191231", pkg.get("listing_expiration_date"))
        ok_(pkg.get("finished"))

        # Verify some corner cases.
        pkg = pickle.loads(data[1][1])
        eq_("0002-1200_4bd46cbe-cdc1-4329-a8e7-22816bd7fc33", pkg.get("product_id"))
        eq_("Amyvid III", pkg.get("brand_name"))
        eq_("Amyvid", pkg.get("brand_name_base"))
        eq_("III", pkg.get("brand_name_suffix"))
        eq_(
            ["INTRALESIONAL", "INTRAMUSCULAR", "INTRASYNOVIAL", "SOFT TISSUE"],
            pkg.get("route"),
        )
        eq_(
            ["Radioactive Diagnostic Agent [EPC]", "Positron Emitting Activity [MoA]"],
            pkg.get("pharm_class"),
        )
        eq_("DEA", pkg.get("dea_schedule"))
        eq_("20180910", pkg.get("marketing_end_date"))
        eq_(
            [
                {"strength": "70 mg/1", "name": "ALENDRONATE SODIUM"},
                {"strength": "5600 [iU]/1", "name": "CHOLECALCIFEROL"},
                {"strength": "100", "name": "FLUORIDE"},
                {"name": "ZINC"},
            ],
            pkg.get("active_ingredients"),
        )

        # Verify unfinished drugs.
        pkg = pickle.loads(data[5][1])
        eq_("0002-1401_70b2e81d-fb7e-48bb-b97d-7f2f26f18c41", pkg.get("product_id"))
        eq_("70b2e81d-fb7e-48bb-b97d-7f2f26f18c41", pkg.get("spl_id"))
        eq_("0002-1401", pkg.get("product_ndc"))
        eq_("DRUG FOR FURTHER PROCESSING", pkg.get("product_type"))
        eq_(None, pkg.get("brand_name"))
        eq_(None, pkg.get("brand_name_base"))
        eq_(None, pkg.get("brand_name_suffix"))
        eq_("Dulaglutide", pkg.get("generic_name"))
        eq_("INJECTION, SOLUTION", pkg.get("dosage_form"))
        eq_(None, pkg.get("route"))
        eq_("20140922", pkg.get("marketing_start_date"))
        eq_(None, pkg.get("marketing_end_date"))
        eq_("DRUG FOR FURTHER PROCESSING", pkg.get("marketing_category"))
        eq_(None, pkg.get("application_number"))
        eq_("Eli Lilly and Company", pkg.get("labeler_name"))
        eq_(
            [{"strength": ".75 mg/.5mL", "name": "DULAGLUTIDE"}],
            pkg.get("active_ingredients"),
        )
        eq_(None, pkg.get("strength"))
        eq_(None, pkg.get("unit"))
        eq_(None, pkg.get("pharm_class"))
        eq_(None, pkg.get("dea_schedule"))
        eq_("20181231", pkg.get("listing_expiration_date"))
        ok_(not pkg.get("finished"))

    def test_package_ndc_to_json(self):
        os.mkdir(os.path.join(self.test_dir, "extracted"))
        shutil.copyfile(
            os.path.join(dirname(os.path.abspath(__file__)), "package_trimmed.txt"),
            os.path.join(self.test_dir, "extracted/package.txt"),
        )
        shutil.copyfile(
            os.path.join(
                dirname(os.path.abspath(__file__)), "unfinished_package_trimmed.txt"
            ),
            os.path.join(self.test_dir, "extracted/unfinished_package.txt"),
        )

        merge = MergePackageNDC()
        merge.run()
        mergedPath = os.path.join(self.test_dir, "merge/package.csv")

        toJSON = NDCPackage2JSON()
        toJSON.run()

        ldb = leveldb.LevelDB(
            os.path.join(toJSON.output().path, "shard-00000-of-00001.db")
        )
        data = list(ldb.RangeIter())
        pkg = pickle.loads(data[0][1])
        eq_("0002-0800_ab2a5006-1880-4b6c-9640-dc2ec1066449", pkg.get("product_id"))
        eq_("0002-0800", pkg.get("product_ndc"))
        eq_("0002-0800-01", pkg.get("package_ndc"))
        eq_(
            "1 VIAL in 1 CARTON (0002-0800-01)  > 10 mL in 1 VIAL",
            pkg.get("description"),
        )
        eq_("19870710", pkg.get("marketing_start_date"))
        eq_(None, pkg.get("marketing_end_date"))
        eq_(False, pkg.get("sample"))
        eq_(True, pkg.get("finished"))

        pkg = pickle.loads(data[1][1])
        eq_("20190203", pkg.get("marketing_end_date"))
        eq_(True, pkg.get("sample"))

        pkg = pickle.loads(data[3][1])
        eq_("0002-0485_6d8a935c-fc7b-44c6-92d1-2337b959d1ce", pkg.get("product_id"))
        eq_("0002-0485", pkg.get("product_ndc"))
        eq_("0002-0485-04", pkg.get("package_ndc"))
        eq_(
            "4 BOTTLE in 1 CONTAINER (0002-0485-04)  > 2000 g in 1 BOTTLE",
            pkg.get("description"),
        )
        eq_(None, pkg.get("marketing_start_date"))
        eq_(None, pkg.get("marketing_end_date"))
        eq_(None, pkg.get("sample"))
        eq_(False, pkg.get("finished"))

    def test_merge_product_ndc(self):
        os.mkdir(os.path.join(self.test_dir, "extracted"))
        shutil.copyfile(
            os.path.join(dirname(os.path.abspath(__file__)), "product.txt"),
            os.path.join(self.test_dir, "extracted/product.txt"),
        )
        shutil.copyfile(
            os.path.join(dirname(os.path.abspath(__file__)), "unfinished_product.txt"),
            os.path.join(self.test_dir, "extracted/unfinished_product.txt"),
        )

        merge = MergeProductNDC()
        merge.run()

        mergedPath = os.path.join(self.test_dir, "merge/product.csv")
        ok_(os.path.isfile(mergedPath))

        dtype = {
            "STARTMARKETINGDATE": np.unicode,
            "ENDMARKETINGDATE": np.unicode,
            "APPLICATIONNUMBER": np.unicode,
            "ACTIVE_NUMERATOR_STRENGTH": np.unicode,
            "LISTING_RECORD_CERTIFIED_THROUGH": np.unicode,
            "PROPRIETARYNAMESUFFIX": np.unicode,
        }
        csv = pd.read_csv(
            mergedPath, sep="\t", index_col=False, encoding="utf-8", dtype=dtype
        )
        # Obviously, the merged file has to have the sum of lines in both source files.
        ok_(len(csv.index) == 121856 + 21478)

        # Make sure every line from finished product.txt made it correctly into the merged CSV.
        finished = pd.read_csv(
            os.path.join(self.test_dir, "extracted/product.txt"),
            sep="\t",
            index_col=False,
            encoding="utf-8",
            dtype=dtype,
        )
        truncated = csv.truncate(after=len(finished.index) - 1)
        # Next line ensures all of the finished lines got Y in the FINISHED column indeed.
        eq_(truncated["FINISHED"].unique(), ["Y"])

        # Drop the last column, after which the two frames must match precisely.
        truncated = truncated.drop(columns=["FINISHED"])
        ok_(finished.equals(truncated))

        # Now, make sure every line from unfinished package.txt made it correctly into the merged CSV.
        unfinished = pd.read_csv(
            os.path.join(self.test_dir, "extracted/unfinished_product.txt"),
            sep="\t",
            index_col=False,
            encoding="utf-8",
            dtype=dtype,
        )
        truncated = csv.truncate(before=len(finished.index)).reindex()
        # Next line ensures all of the finished lines got N in the FINISHED column indeed.
        eq_(truncated["FINISHED"].unique(), ["N"])

        # Drop the last column, after which the two frames must match precisely.
        truncated = truncated.drop(
            columns=[
                "FINISHED",
                "PROPRIETARYNAME",
                "PROPRIETARYNAMESUFFIX",
                "ROUTENAME",
                "APPLICATIONNUMBER",
                "PHARM_CLASSES",
                "NDC_EXCLUDE_FLAG",
            ]
        )
        eq_(unfinished.to_dict(orient="list"), truncated.to_dict(orient="list"))

    def test_merge_package_ndc(self):
        os.mkdir(os.path.join(self.test_dir, "extracted"))
        shutil.copyfile(
            os.path.join(dirname(os.path.abspath(__file__)), "package.txt"),
            os.path.join(self.test_dir, "extracted/package.txt"),
        )
        shutil.copyfile(
            os.path.join(dirname(os.path.abspath(__file__)), "unfinished_package.txt"),
            os.path.join(self.test_dir, "extracted/unfinished_package.txt"),
        )

        merge = MergePackageNDC()
        merge.run()

        mergedPath = os.path.join(self.test_dir, "merge/package.csv")
        ok_(os.path.isfile(mergedPath))

        dtype = {"STARTMARKETINGDATE": np.unicode, "ENDMARKETINGDATE": np.unicode}
        csv = pd.read_csv(
            mergedPath, sep="\t", index_col=False, encoding="utf-8", dtype=dtype
        )
        # Obviously, the merged file has to have the sum of lines in both source files.
        ok_(len(csv.index) == 234309 + 35321)

        # Make sure every line from finished package.txt made it correctly into the merged CSV.
        finished = pd.read_csv(
            os.path.join(self.test_dir, "extracted/package.txt"),
            sep="\t",
            index_col=False,
            encoding="utf-8",
            dtype=dtype,
        )
        truncated = csv.truncate(after=len(finished.index) - 1)
        # Next line ensures all of the finished lines got Y in the FINISHED column indeed.
        eq_(truncated["FINISHED"].unique(), ["Y"])

        # Drop the last column, after which the two frames must match precisely.
        truncated = truncated.drop(columns=["FINISHED"])
        ok_(finished.equals(truncated))

        # Finally, test a random line manually. Just to make sure there isn't a CSV reading bug equally breaking both data sets.
        ok_(csv.at[22, "PRODUCTID"] == "0002-3235_48f72160-f6f7-4890-8471-041da7cdd3a7")
        ok_(csv.at[22, "PRODUCTNDC"] == "0002-3235")
        ok_(csv.at[22, "NDCPACKAGECODE"] == "0002-3235-60")
        ok_(
            csv.at[22, "PACKAGEDESCRIPTION"]
            == "60 CAPSULE, DELAYED RELEASE in 1 BOTTLE (0002-3235-60) "
        )
        ok_(csv.at[22, "STARTMARKETINGDATE"] == "20040824")
        ok_(math.isnan(csv.at[22, "ENDMARKETINGDATE"]))
        ok_(csv.at[22, "NDC_EXCLUDE_FLAG"] == "N")
        ok_(csv.at[22, "SAMPLE_PACKAGE"] == "N")

        # Now, make sure every line from unfinished package.txt made it correctly into the merged CSV.
        unfinished = pd.read_csv(
            os.path.join(self.test_dir, "extracted/unfinished_package.txt"),
            sep="\t",
            index_col=False,
            encoding="utf-8",
            dtype=dtype,
        )
        truncated = csv.truncate(before=len(finished.index)).reindex()
        # Next line ensures all of the finished lines got N in the FINISHED column indeed.
        eq_(truncated["FINISHED"].unique(), ["N"])

        # Drop the last column, after which the two frames must match precisely.
        truncated = truncated.drop(
            columns=[
                "FINISHED",
                "STARTMARKETINGDATE",
                "ENDMARKETINGDATE",
                "NDC_EXCLUDE_FLAG",
                "SAMPLE_PACKAGE",
            ]
        )
        eq_(unfinished.to_dict(orient="list"), truncated.to_dict(orient="list"))

        # Finally, test a random line manually. Just to make sure there isn't a CSV reading bug equally breaking both data sets.
        ok_(
            csv.at[234309, "PRODUCTID"]
            == "0002-0485_6d8a935c-fc7b-44c6-92d1-2337b959d1ce"
        )
        ok_(csv.at[234309, "PRODUCTNDC"] == "0002-0485")
        ok_(csv.at[234309, "NDCPACKAGECODE"] == "0002-0485-04")
        ok_(
            csv.at[234309, "PACKAGEDESCRIPTION"]
            == "4 BOTTLE in 1 CONTAINER (0002-0485-04)  > 2000 g in 1 BOTTLE"
        )
        ok_(math.isnan(csv.at[234309, "STARTMARKETINGDATE"]))
        ok_(math.isnan(csv.at[234309, "ENDMARKETINGDATE"]))
        ok_(math.isnan(csv.at[234309, "NDC_EXCLUDE_FLAG"]))
        ok_(math.isnan(csv.at[234309, "SAMPLE_PACKAGE"]))

    def test_fix_utf8_issues(self):
        testfile = os.path.join(self.test_dir, "badutf8.txt")
        shutil.copyfile(
            os.path.join(dirname(os.path.abspath(__file__)), "badutf8.txt"), testfile
        )

        extract = ExtractNDCFiles()
        extract.fix_utf8_issues(testfile)

        file = open(testfile, "r")
        data = file.read()
        file.close()
        ok_(data == 'This is a "bad" UTF-8 file.')

    def test_extract_ndc(self):
        os.mkdir(os.path.join(self.test_dir, "raw"))
        shutil.copyfile(
            os.path.join(dirname(os.path.abspath(__file__)), "finished.zip"),
            os.path.join(self.test_dir, "raw/finished.zip"),
        )
        shutil.copyfile(
            os.path.join(dirname(os.path.abspath(__file__)), "unfinished.zip"),
            os.path.join(self.test_dir, "raw/unfinished.zip"),
        )

        extract = ExtractNDCFiles()
        extract.run()

        eq_(
            os.stat(os.path.join(self.test_dir, "extracted/package.txt")).st_size,
            31980050,
        )
        eq_(
            os.stat(os.path.join(self.test_dir, "extracted/product.txt")).st_size,
            41165614,
        )
        eq_(
            os.stat(
                os.path.join(self.test_dir, "extracted/unfinished_package.txt")
            ).st_size,
            3853326,
        )
        eq_(
            os.stat(
                os.path.join(self.test_dir, "extracted/unfinished_product.txt")
            ).st_size,
            4493918,
        )

    def test_download_ndc(self):
        downloadNDC = DownloadNDCFiles()
        downloadNDC.run()
        ok_(os.path.isfile(os.path.join(self.test_dir, "raw/finished.zip")))
        ok_(os.path.isfile(os.path.join(self.test_dir, "raw/unfinished.zip")))
        ok_(os.stat(os.path.join(self.test_dir, "raw/finished.zip")).st_size > 0)
        ok_(os.stat(os.path.join(self.test_dir, "raw/unfinished.zip")).st_size > 0)


def main(argv):
    unittest.main(argv=argv)


if __name__ == "__main__":
    unittest.main()
