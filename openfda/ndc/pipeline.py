#!/usr/local/bin/python

"""
Pipeline for converting NDC product and package data sets (TXT) to JSON and importing into Elasticsearch.
"""
import logging
import os
import re
from os.path import join, dirname
from urllib.parse import urljoin
from urllib.request import urlopen
import luigi
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

from openfda import common, config, parallel, index_util
from openfda.annotation_table.pipeline import CombineHarmonization
from openfda.common import newest_file_timestamp
from openfda.ndc.annotate import AnnotateMapper

NDC_DOWNLOAD_PAGE = "https://www.fda.gov/drugs/drug-approvals-and-databases/national-drug-code-directory"
BASE_DIR = join(config.data_dir(), "ndc")
RAW_DIR = join(BASE_DIR, "raw")

MERGED_PACKAGES = join(BASE_DIR, "merge/package.csv")
MERGED_PRODUCTS = join(BASE_DIR, "merge/product.csv")

NDC_PACKAGE_DB = join(BASE_DIR, "json/package.db")
NDC_PRODUCT_DB = join(BASE_DIR, "json/product.db")
NDC_MERGE_DB = join(BASE_DIR, "json/merged.db")
NDC_ANNOTATED_DB = join(BASE_DIR, "json/annotated.db")


class DownloadNDCFiles(luigi.Task):
    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(RAW_DIR)

    def run(self):
        finished_ndc_url = None
        unfinished_ndc_url = None

        soup = BeautifulSoup(urlopen(NDC_DOWNLOAD_PAGE).read(), "lxml")
        for a in soup.find_all(href=re.compile(".*.zip")):
            if "ndc database file" in a.text.lower() and "text" in a["href"]:
                finished_ndc_url = urljoin("https://www.fda.gov", a["href"])
            if "ndc unfinished" in a.text.lower() and "unfinished.zip" in a["href"]:
                unfinished_ndc_url = urljoin("https://www.fda.gov", a["href"])

        if not finished_ndc_url:
            logging.fatal("NDC finished database file not found!")
        if not unfinished_ndc_url:
            logging.fatal("NDC unfinished drugs database file not found!")

        common.download(finished_ndc_url, join(RAW_DIR, "finished.zip"))
        common.download(unfinished_ndc_url, join(RAW_DIR, "unfinished.zip"))


class ExtractNDCFiles(luigi.Task):
    def requires(self):
        return DownloadNDCFiles()

    def output(self):
        return luigi.LocalTarget(join(BASE_DIR, "extracted"))

    def run(self):
        zip_filename = join(BASE_DIR, "raw/finished.zip")
        output_dir = self.output().path
        os.system("mkdir -p %s" % output_dir)
        os.system("unzip -o %(zip_filename)s -d %(output_dir)s" % locals())

        zip_filename = join(BASE_DIR, "raw/unfinished.zip")
        os.system("unzip -o %(zip_filename)s -d %(output_dir)s" % locals())

        # Fix annoying UTF-8 issues in product.txt
        self.fix_utf8_issues(join(output_dir, "product.txt"))
        self.fix_utf8_issues(join(output_dir, "unfinished_product.txt"))

    def fix_utf8_issues(self, filename):
        file = open(filename, "r", encoding="utf-8", errors="ignore")
        data = file.read()
        file.close()

        file = open(filename, "w", encoding="utf-8")
        file.write(common.convert_unicode(data))
        file.flush()
        file.close()


class MergePackageNDC(luigi.Task):
    def requires(self):
        return ExtractNDCFiles()

    def output(self):
        return luigi.LocalTarget(MERGED_PACKAGES)

    def run(self):
        output_dir = dirname(MERGED_PACKAGES)
        os.system("mkdir -p %s" % output_dir)

        dtype = {"STARTMARKETINGDATE": np.unicode, "ENDMARKETINGDATE": np.unicode}
        finished = pd.read_csv(
            join(self.input().path, "package.txt"),
            sep="\t",
            index_col=False,
            encoding="utf-8",
            dtype=dtype,
        )
        unfinished = pd.read_csv(
            join(self.input().path, "unfinished_package.txt"),
            sep="\t",
            index_col=False,
            encoding="utf-8",
            dtype=dtype,
        )

        finished["FINISHED"] = "Y"
        unfinished["FINISHED"] = "N"
        merged = finished.append(unfinished, ignore_index=True, sort=False)
        merged.to_csv(MERGED_PACKAGES, sep="\t", encoding="utf-8", index=False)


class MergeProductNDC(luigi.Task):
    def requires(self):
        return ExtractNDCFiles()

    def output(self):
        return luigi.LocalTarget(MERGED_PRODUCTS)

    def run(self):
        output_dir = dirname(MERGED_PRODUCTS)
        os.system("mkdir -p %s" % output_dir)

        dtype = {
            "STARTMARKETINGDATE": np.unicode,
            "ENDMARKETINGDATE": np.unicode,
            "APPLICATIONNUMBER": np.unicode,
            "ACTIVE_NUMERATOR_STRENGTH": np.unicode,
            "LISTING_RECORD_CERTIFIED_THROUGH": np.unicode,
            "PROPRIETARYNAMESUFFIX": np.unicode,
        }
        finished = pd.read_csv(
            join(self.input().path, "product.txt"),
            sep="\t",
            index_col=False,
            encoding="utf-8",
            dtype=dtype,
        )
        unfinished = pd.read_csv(
            join(self.input().path, "unfinished_product.txt"),
            sep="\t",
            index_col=False,
            encoding="utf-8",
            dtype=dtype,
        )

        finished["FINISHED"] = "Y"
        unfinished["FINISHED"] = "N"
        merged = finished.append(unfinished, ignore_index=True, sort=False)
        merged.to_csv(MERGED_PRODUCTS, sep="\t", encoding="utf-8", index=False)


class NDCPackage2JSONMapper(parallel.Mapper):
    rename_map = {
        "PRODUCTID": "product_id",
        "PRODUCTNDC": "product_ndc",
        "NDCPACKAGECODE": "package_ndc",
        "PACKAGEDESCRIPTION": "description",
        "STARTMARKETINGDATE": "marketing_start_date",
        "ENDMARKETINGDATE": "marketing_end_date",
        "SAMPLE_PACKAGE": "sample",
        "FINISHED": "finished",
    }

    booleans = ["SAMPLE_PACKAGE", "FINISHED"]

    def map(self, key, value, output):
        def _cleaner(k, v):
            """Helper function to rename keys and purge any keys that are not in
            the map.
            """
            if k in self.booleans:
                v = v in ["Y", "y"] if v is not None and v != "" else None
            v = v.strip() if isinstance(v, str) else v
            if k in self.rename_map and v is not None and v != "":
                return (self.rename_map[k], v)

        new_value = common.transform_dict(value, _cleaner)
        output.add(key, new_value)


class NDCPackage2JSON(luigi.Task):
    def requires(self):
        return MergePackageNDC()

    def output(self):
        return luigi.LocalTarget(NDC_PACKAGE_DB)

    def run(self):
        parallel.mapreduce(
            parallel.Collection.from_glob(
                self.input().path, parallel.CSVDictLineInput(delimiter="\t")
            ),
            mapper=NDCPackage2JSONMapper(),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
            num_shards=1,
        )


class NDCProduct2JSONMapper(parallel.Mapper):
    rename_map = {
        "PRODUCTID": "product_id",
        "PRODUCTNDC": "product_ndc",
        "PRODUCTTYPENAME": "product_type",
        "PROPRIETARYNAME": "brand_name",
        "PROPRIETARYNAMESUFFIX": "brand_name_suffix",
        "NONPROPRIETARYNAME": "generic_name",
        "DOSAGEFORMNAME": "dosage_form",
        "ROUTENAME": "route",
        "STARTMARKETINGDATE": "marketing_start_date",
        "ENDMARKETINGDATE": "marketing_end_date",
        "MARKETINGCATEGORYNAME": "marketing_category",
        "APPLICATIONNUMBER": "application_number",
        "LABELERNAME": "labeler_name",
        "SUBSTANCENAME": "active_ingredients",
        "ACTIVE_NUMERATOR_STRENGTH": "strength",
        "ACTIVE_INGRED_UNIT": "unit",
        "PHARM_CLASSES": "pharm_class",
        "DEASCHEDULE": "dea_schedule",
        "LISTING_RECORD_CERTIFIED_THROUGH": "listing_expiration_date",
        "FINISHED": "finished",
    }

    booleans = ["FINISHED"]

    def map(self, key, value, output):
        def _cleaner(k, v):
            """Helper function to rename keys and purge any keys that are not in
            the map.
            """
            if k in self.booleans:
                v = v in ["Y", "y"] if v is not None and v != "" else None
            v = v.strip() if isinstance(v, str) else v
            if k in self.rename_map and v is not None and v != "":
                return (self.rename_map[k], v)

        json = common.transform_dict(value, _cleaner)

        # SPL ID
        json["spl_id"] = json["product_id"].split("_")[1]

        # Brand name parsing
        json["brand_name_base"] = json.get("brand_name")
        if json.get("brand_name_suffix"):
            json["brand_name"] = (
                (json.get("brand_name") if json.get("brand_name") is not None else "")
                + " "
                + json["brand_name_suffix"]
            )

        # Route is a multi-value field easily parseable
        if json.get("route"):
            json["route"] = re.sub(";\s+", ";", json["route"]).split(";")

        # Pharm class field is a multi-value field easily parseable
        if json.get("pharm_class"):
            json["pharm_class"] = re.sub(",\s+", ",", json["pharm_class"]).split(",")

        # Turn active ingredients into an array of objects as per the mapping.
        if json.get("active_ingredients"):
            ingredientList = re.sub(";\s+", ";", json["active_ingredients"]).split(";")
            json["active_ingredients"] = []

            strengthList = (
                re.sub(";\s+", ";", json["strength"]).split(";")
                if json.get("strength")
                else []
            )
            unitList = (
                re.sub(";\s+", ";", json["unit"]).split(";") if json.get("unit") else []
            )
            for idx, name in enumerate(ingredientList):
                ingredient = {"name": name}
                if len(strengthList) > idx:
                    ingredient["strength"] = strengthList[idx] + (
                        " " + unitList[idx] if len(unitList) > idx else ""
                    )
                json["active_ingredients"].append(ingredient)
        else:
            # Delete to avoid complaints from Elasticsearch.
            if json.get("active_ingredients") is not None:
                del json["active_ingredients"]

        if json.get("strength") is not None:
            del json["strength"]
        if json.get("unit") is not None:
            del json["unit"]

        output.add(key, json)


class NDCProduct2JSON(luigi.Task):
    def requires(self):
        return MergeProductNDC()

    def output(self):
        return luigi.LocalTarget(NDC_PRODUCT_DB)

    def run(self):
        parallel.mapreduce(
            parallel.Collection.from_glob(
                self.input().path, parallel.CSVDictLineInput(delimiter="\t")
            ),
            mapper=NDCProduct2JSONMapper(),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
            num_shards=1,
        )


class ProductAndPackagingMergingMapper(parallel.Mapper):

    def __init__(self, package_db):
        parallel.Mapper.__init__(self)
        self.package_db = package_db

        # Build an inverse index: product_id --> a list of packages with that product id.
        # Otherwise it is TOO slow.
        self.inverse_index = {}
        for key, value in self.package_db.items():
            if value.get("product_id"):
                product_id = value["product_id"]
                if self.inverse_index.get(product_id) is None:
                    self.inverse_index[product_id] = []
                self.inverse_index[product_id].append(value)

    def map(self, key, value, output):
        id = value["product_id"]
        value["packaging"] = []

        for pkg in (
            self.inverse_index[id] if self.inverse_index.get(id) is not None else []
        ):
            del pkg["finished"]
            del pkg["product_id"]
            del pkg["product_ndc"]
            value["packaging"].append(pkg)

        output.add(key, value)


class MergeProductsAndPackaging(luigi.Task):
    def requires(self):
        return [NDCPackage2JSON(), NDCProduct2JSON()]

    def output(self):
        return luigi.LocalTarget(NDC_MERGE_DB)

    def run(self):
        # Read the entire packaging DB in memory for speed.
        package_db = parallel.ShardedDB.open(self.input()[0].path).as_dict()

        parallel.mapreduce(
            parallel.Collection.from_sharded(self.input()[1].path),
            mapper=ProductAndPackagingMergingMapper(package_db=package_db),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
            num_shards=1,
        )


class AnnotateJSON(luigi.Task):
    def requires(self):
        return [MergeProductsAndPackaging(), CombineHarmonization()]

    def output(self):
        return luigi.LocalTarget(NDC_ANNOTATED_DB)

    def run(self):
        input_db = self.input()[0].path
        harmonized_file = self.input()[1].path

        parallel.mapreduce(
            parallel.Collection.from_sharded(input_db),
            mapper=AnnotateMapper(harmonized_file),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
            num_shards=1,
        )


class LoadJSON(index_util.LoadJSONBase):
    index_name = "ndc"
    mapping_file = "./schemas/ndc_mapping.json"
    data_source = AnnotateJSON()
    use_checksum = False
    optimize_index = True
    last_update_date = lambda _: newest_file_timestamp(RAW_DIR)


if __name__ == "__main__":
    luigi.run()
