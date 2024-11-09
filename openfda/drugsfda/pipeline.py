#!/usr/local/bin/python

"""
Pipeline for converting Drugs@FDA files to JSON and importing into Elasticsearch.
"""
import glob
import os
import re
from os.path import join
import logging
import arrow
import luigi

from openfda import common, config, parallel, index_util
from openfda.annotation_table.pipeline import CombineHarmonization
from openfda.common import first_file_timestamp
from openfda.drugsfda.annotate import AnnotateMapper

DOWNLOAD_FILE = "https://www.fda.gov/media/89850/download"
BASE_DIR = join(config.data_dir(), "drugsfda")
EXTRACTED_DIR = join(BASE_DIR, "extracted")
RAW_DATA_FILE = join(BASE_DIR, "raw/drugsfda.zip")

PRODUCTS_DB = join(BASE_DIR, "json/products.db")
APPLICATIONS_DB = join(BASE_DIR, "json/applications.db")
APPLICATIONS_DOCS_DB = join(BASE_DIR, "json/applicationsdocs.db")
SUBMISSIONS_DB = join(BASE_DIR, "json/submissions.db")
SUBMISSION_PROPERTY_TYPE_DB = join(BASE_DIR, "json/submissionpropertytype.db")
MARKETING_STATUS_DB = join(BASE_DIR, "json/marketingstatus.db")
ANNOTATED_DB = join(BASE_DIR, "json/annotated.db")
TE_DB = join(BASE_DIR, "json/te.db")
MERGED_DB = join(BASE_DIR, "json/merged.db")


class DownloadDrugsFDAFiles(luigi.Task):
    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(RAW_DATA_FILE)

    def run(self):
        common.download(DOWNLOAD_FILE, RAW_DATA_FILE)


class ExtractDrugsFDAFiles(luigi.Task):
    def requires(self):
        return DownloadDrugsFDAFiles()

    def output(self):
        return luigi.LocalTarget(EXTRACTED_DIR)

    def run(self):
        zip_filename = RAW_DATA_FILE
        output_dir = self.output().path
        os.system("unzip -o %(zip_filename)s -d %(output_dir)s" % locals())


class CleanDrugsFDAFiles(luigi.Task):
    def requires(self):
        return ExtractDrugsFDAFiles()

    def output(self):
        return luigi.LocalTarget(EXTRACTED_DIR)

    def run(self):
        for filename in glob.glob(self.input().path + "/ApplicationDocs.txt"):
            logging.info("Pre-processing %s", filename)
            filtered = filename + ".filtered"
            out = open(filtered, "w")
            line_num = 0
            bad_lines = 0
            with open(filename, "rU", errors="ignore") as fp:
                for line in fp:
                    line = line.strip()
                    if line_num < 1:
                        # First line is usually the header
                        out.write(line)
                    else:
                        if len(line.strip()) > 0:
                            if re.search(r"^\d{1,}", line):
                                # Properly formatted line. Append it and move on.
                                out.write("\n" + line)
                            else:
                                # Bad line, most likely due to an unescaped carriage return. Tuck it onto the previous line
                                out.write(" " + line)
                                bad_lines += 1

                    line_num += 1

            logging.info("Issues found & fixed: %s", bad_lines)
            out.close()
            os.remove(filename)
            os.rename(filtered, filename)


class Applications2JSONMapper(parallel.Mapper):
    rename_map = {
        "ApplNo": "application_no",
        "ApplType": "application_type",
        "ApplPublicNotes": "application_public_notes",
        "SponsorName": "sponsor_name",
    }

    def map(self, key, value, output):
        def _cleaner(k, v):
            """Helper function to rename keys and purge any keys that are not in
            the map.
            """
            v = v.strip() if isinstance(v, str) else v
            if k in self.rename_map and v is not None and v != "":
                return (self.rename_map[k], v)

        json = common.transform_dict(value, _cleaner)

        if json.get("application_public_notes") != None:
            del json["application_public_notes"]

        if json.get("application_no") and json.get("application_type"):
            json["application_number"] = json.get("application_type") + json.get(
                "application_no"
            )
            del json["application_type"]
            del json["application_no"]

        output.add(key, json)


class Product2JSONMapper(parallel.Mapper):
    rename_map = {
        "ApplNo": "application_number",
        "ProductNo": "product_number",
        "Form": "df_and_route",
        "Strength": "strength",
        "ReferenceDrug": "reference_drug",
        "DrugName": "brand_name",
        "ActiveIngredient": "active_ingredients",
        "ReferenceStandard": "reference_standard",
    }

    VALUE_MAPPINGS = {
        "reference_drug": {"0": "No", "1": "Yes", "2": "TBD"},
        "reference_standard": {"0": "No", "1": "Yes"},
    }

    def map(self, key, value, output):
        def _cleaner(k, v):
            """Helper function to rename keys and purge any keys that are not in
            the map.
            """
            v = v.strip() if isinstance(v, str) else v
            if k in self.rename_map and v is not None and v != "":
                new_key = self.rename_map[k]
                if new_key in self.VALUE_MAPPINGS and v in self.VALUE_MAPPINGS[new_key]:
                    v = self.VALUE_MAPPINGS[new_key][v]
                return (new_key, v)

        json = common.transform_dict(value, _cleaner)

        # Turn active ingredients into an array of objects as per the mapping.
        if json.get("active_ingredients"):
            ingredientList = re.sub(";\s+", ";", json["active_ingredients"]).split(";")
            json["active_ingredients"] = []

            strengthList = (
                re.sub(";\s+", ";", json["strength"]).split(";")
                if json.get("strength")
                else []
            )

            for idx, name in enumerate(ingredientList):
                ingredient = {"name": name}
                if len(strengthList) > idx:
                    ingredient["strength"] = strengthList[idx]
                json["active_ingredients"].append(ingredient)
        else:
            # Delete to avoid complaints from Elasticsearch.
            if json.get("active_ingredients") is not None:
                del json["active_ingredients"]

        if json.get("strength") is not None:
            del json["strength"]

        # Split dosage and form into two distinct fields.
        if json.get("df_and_route") and len(json["df_and_route"].split(";")) == 2:
            json["dosage_form"] = json["df_and_route"].split(";")[0].strip()
            json["route"] = json["df_and_route"].split(";")[1].strip()
        # Sometimes the entire entry is Unknown. Indicate this for both df & route.
        elif json.get("df_and_route") and "UNKNOWN" in json["df_and_route"]:
            json["dosage_form"] = json["df_and_route"]
            json["route"] = json["df_and_route"]
        # Sometimes the entire only contains dosage form.
        else:
            json["dosage_form"] = json["df_and_route"]
            json["route"] = None

        # Delete the field either way
        del json["df_and_route"]

        # Assign application number as the key, since all three drugs@FDA files can be joined by this key.
        key = build_products_key(json["application_number"], json)
        del json["application_number"]

        output.add(key, json)


def build_products_key(app_number, json):
    return "%s-%s" % (app_number, json["product_number"])


class MarketingStatus2JSONMapper(parallel.Mapper):
    def __init__(self, doc_lookup):
        parallel.Mapper.__init__(self)
        self.doc_lookup = doc_lookup

    rename_map = {
        "MarketingStatusID": "marketing_status_id",
        "ApplNo": "application_number",
        "ProductNo": "product_number",
    }

    def map(self, key, value, output):
        def _cleaner(k, v):
            """Helper function to rename keys and purge any keys that are not in
            the map.
            """
            v = v.strip() if isinstance(v, str) else v
            if k in self.rename_map and v is not None and v != "":
                return (self.rename_map[k], v)

        json = common.transform_dict(value, _cleaner)

        if json.get("marketing_status_id"):
            json["marketing_status"] = self.doc_lookup[json["marketing_status_id"]]
            del json["marketing_status_id"]

        # Assign application number as the key, since all three drugs@FDA files can be joined by this key.
        key = build_products_key(json["application_number"], json)
        del json["application_number"], json["product_number"]

        output.add(key, json)


class TE2JSONMapper(parallel.Mapper):
    def __init__(self, doc_lookup):
        parallel.Mapper.__init__(self)
        self.doc_lookup = doc_lookup

    rename_map = {
        "ApplNo": "application_number",
        "ProductNo": "product_number",
        "MarketingStatusID": "marketing_status_id",
        "TECode": "te_code",
    }

    def map(self, key, value, output):
        def _cleaner(k, v):
            """Helper function to rename keys and purge any keys that are not in
            the map.
            """
            v = v.strip() if isinstance(v, str) else v
            if k in self.rename_map and v is not None and v != "":
                return (self.rename_map[k], v)

        json = common.transform_dict(value, _cleaner)

        if json.get("marketing_status_id"):
            json["marketing_status"] = self.doc_lookup[json["marketing_status_id"]]
            del json["marketing_status_id"]

        # Assign application number as the key, since all three drugs@FDA files can be joined by this key.
        key = build_products_key(json["application_number"], json)
        del json["application_number"], json["product_number"]

        output.add(key, json)


class Submissions2JSONMapper(parallel.Mapper):
    def __init__(self, doc_lookup):
        parallel.Mapper.__init__(self)
        self.doc_lookup = doc_lookup

    rename_map = {
        "ApplNo": "application_number",
        "SubmissionClassCodeID": "submission_class_code_id",
        "SubmissionType": "submission_type",
        "SubmissionNo": "submission_number",
        "SubmissionStatus": "submission_status",
        "SubmissionStatusDate": "submission_status_date",
        "SubmissionsPublicNotes": "submission_public_notes",
        "ReviewPriority": "review_priority",
    }

    def map(self, key, value, output):
        def _cleaner(k, v):
            """Helper function to rename keys and purge any keys that are not in
            the map.
            """
            v = common.convert_unicode(v.strip()) if isinstance(v, str) else v
            if k in self.rename_map and v is not None and v != "":
                return (self.rename_map[k], v)

        json = common.transform_dict(value, _cleaner)

        if (
            json.get("submission_class_code_id")
            and json.get("submission_class_code_id") is not None
        ):
            json["submission_class_code"] = self.doc_lookup[
                json["submission_class_code_id"]
            ][0]
            descr = self.doc_lookup[json["submission_class_code_id"]][1].rstrip()
            if descr:
                json["submission_class_code_description"] = descr
            del json["submission_class_code_id"]

        # Convert date to format used throughout openFDA (yyyymmdd)
        if json.get("submission_status_date"):
            json["submission_status_date"] = arrow.get(
                json["submission_status_date"]
            ).strftime("%Y%m%d")

        # Assign application number as the key, since all three drugs@FDA files can be joined by this key.
        key = build_submissions_key(json["application_number"], json)
        del json["application_number"]

        output.add(key, json)


def build_submissions_key(app_number, json):
    return "%s-%s-%s" % (app_number, json["submission_type"], json["submission_number"])


class SubmissionPropertyType2JSONMapper(parallel.Mapper):
    rename_map = {
        "ApplNo": "application_number",
        "SubmissionType": "submission_type",
        "SubmissionNo": "submission_number",
        "SubmissionPropertyTypeCode": "code",
    }

    def map(self, key, value, output):
        def _cleaner(k, v):
            """Helper function to rename keys and purge any keys that are not in
            the map.
            """
            v = v.strip() if isinstance(v, str) else v
            if k in self.rename_map and v is not None and v != "" and v != "Null":
                return (self.rename_map[k], v)

        json = common.transform_dict(value, _cleaner)

        # Assign application number as the key, since all three drugs@FDA files can be joined by this key.
        key = build_submissions_key(json["application_number"], json)
        del (
            json["application_number"],
            json["submission_number"],
            json["submission_type"],
        )
        if json != {}:
            output.add(key, json)


class ApplicationsDocs2JSONMapper(parallel.Mapper):
    def __init__(self, doc_lookup):
        parallel.Mapper.__init__(self)
        self.doc_lookup = doc_lookup

    rename_map = {
        "ApplicationDocsID": "id",
        "ApplicationDocsTypeID": "type_id",
        "ApplNo": "application_number",
        "SubmissionType": "submission_type",
        "SubmissionNo": "submission_number",
        "ApplicationDocsTitle": "title",
        "ApplicationDocsURL": "url",
        "ApplicationDocsDate": "date",
    }

    def map(self, key, value, output):
        def _cleaner(k, v):
            """Helper function to rename keys and purge any keys that are not in
            the map.
            """
            v = v.strip() if isinstance(v, str) else v
            if k in self.rename_map and v is not None and v != "":
                new_key = self.rename_map[k]
                if not (new_key == "title" and v == "0"):
                    return (new_key, v)

        json = common.transform_dict(value, _cleaner)

        json["type"] = self.doc_lookup[json["type_id"]]
        del json["type_id"]

        # Convert date to format used throughout openFDA (yyyymmdd)
        json["date"] = (
            arrow.get(json["date"]).strftime("%Y%m%d")
            if json.get("date") is not None
            else ""
        )
        json["url"] = (
            common.convert_unicode(json["url"]) if json.get("url") is not None else ""
        )

        # Assign application number as the key, since all three drugs@FDA files can be joined by this key.
        key = build_submissions_key(json["application_number"], json)
        del (
            json["application_number"],
            json["submission_number"],
            json["submission_type"],
        )

        output.add(key, json)


class Applications2JSON(luigi.Task):
    def requires(self):
        return CleanDrugsFDAFiles()

    def output(self):
        return luigi.LocalTarget(APPLICATIONS_DB)

    def run(self):
        parallel.mapreduce(
            parallel.Collection.from_glob(
                join(self.input().path, "Applications.txt"),
                parallel.CSVDictLineInput(delimiter="\t"),
            ),
            mapper=Applications2JSONMapper(),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
        )


class Products2JSON(luigi.Task):
    def requires(self):
        return CleanDrugsFDAFiles()

    def output(self):
        return luigi.LocalTarget(PRODUCTS_DB)

    def run(self):
        parallel.mapreduce(
            parallel.Collection.from_glob(
                join(self.input().path, "Products.txt"),
                parallel.CSVDictLineInput(delimiter="\t"),
            ),
            mapper=Product2JSONMapper(),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
        )


class MarketingStatus2JSON(luigi.Task):
    def requires(self):
        return CleanDrugsFDAFiles()

    def output(self):
        return luigi.LocalTarget(MARKETING_STATUS_DB)

    def run(self):
        with open(join(EXTRACTED_DIR, "MarketingStatus_Lookup.txt")) as fin:
            rows = (line.split("\t") for line in fin)
            doc_lookup = {row[0]: row[1] for row in rows}

        parallel.mapreduce(
            parallel.Collection.from_glob(
                join(self.input().path, "MarketingStatus.txt"),
                parallel.CSVDictLineInput(delimiter="\t"),
            ),
            mapper=MarketingStatus2JSONMapper(doc_lookup=doc_lookup),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
        )


class TE2JSON(luigi.Task):
    def requires(self):
        return CleanDrugsFDAFiles()

    def output(self):
        return luigi.LocalTarget(TE_DB)

    def run(self):
        with open(join(EXTRACTED_DIR, "MarketingStatus_Lookup.txt")) as fin:
            rows = (line.split("\t") for line in fin)
            doc_lookup = {row[0]: row[1] for row in rows}

        parallel.mapreduce(
            parallel.Collection.from_glob(
                join(self.input().path, "TE.txt"),
                parallel.CSVDictLineInput(delimiter="\t"),
            ),
            mapper=TE2JSONMapper(doc_lookup=doc_lookup),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
        )


class Submissions2JSON(luigi.Task):
    def requires(self):
        return CleanDrugsFDAFiles()

    def output(self):
        return luigi.LocalTarget(SUBMISSIONS_DB)

    def run(self):
        with open(join(EXTRACTED_DIR, "SubmissionClass_Lookup.txt")) as fin:
            rows = (line.split("\t") for line in fin)
            doc_lookup = {row[0]: [row[1], row[2]] for row in rows}

        parallel.mapreduce(
            parallel.Collection.from_glob(
                join(self.input().path, "Submissions.txt"),
                parallel.CSVDictLineInput(delimiter="\t"),
            ),
            mapper=Submissions2JSONMapper(doc_lookup=doc_lookup),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
        )


class SubmissionPropertyType2JSON(luigi.Task):
    def requires(self):
        return CleanDrugsFDAFiles()

    def output(self):
        return luigi.LocalTarget(SUBMISSION_PROPERTY_TYPE_DB)

    def run(self):
        parallel.mapreduce(
            parallel.Collection.from_glob(
                join(self.input().path, "SubmissionPropertyType.txt"),
                parallel.CSVDictLineInput(delimiter="\t"),
            ),
            mapper=SubmissionPropertyType2JSONMapper(),
            reducer=parallel.ListReducer(),
            output_prefix=self.output().path,
        )


class ApplicationsDocs2JSON(luigi.Task):
    def requires(self):
        return CleanDrugsFDAFiles()

    def output(self):
        return luigi.LocalTarget(APPLICATIONS_DOCS_DB)

    def run(self):
        with open(join(EXTRACTED_DIR, "ApplicationsDocsType_Lookup.txt")) as fin:
            rows = (line.split("\t") for line in fin)
            doc_lookup = {row[0]: row[1].rstrip() for row in rows}

        parallel.mapreduce(
            parallel.Collection.from_glob(
                join(self.input().path, "ApplicationDocs.txt"),
                parallel.CSVDictLineInput(delimiter="\t"),
            ),
            mapper=ApplicationsDocs2JSONMapper(doc_lookup=doc_lookup),
            reducer=parallel.ListReducer(),
            output_prefix=self.output().path,
        )


class MergeAllMapper(parallel.Mapper):
    def __init__(
        self,
        applications_db_path,
        products_db_path,
        applications_docs_db_path,
        submissions_db_path,
        submissions_property_type_db_path,
        marketing_status_path,
        te_db_path,
    ):
        self.applications_db_path = applications_db_path
        self.products_db_path = products_db_path
        self.applications_docs_db_path = applications_docs_db_path
        self.submissions_db_path = submissions_db_path
        self.submissions_property_type_db_path = submissions_property_type_db_path
        self.marketing_status_db_path = marketing_status_path
        self.te_db_path = te_db_path

    def map_shard(self, map_input, map_output):
        # Transform product DB into a dictionary keyed by application number
        self.products_dict = {}
        for key, product in parallel.ShardedDB.open(self.products_db_path).range_iter(
            None, None
        ):
            split = key.split("-")
            app_key = split[0]
            products_arr = (
                []
                if self.products_dict.get(app_key) is None
                else self.products_dict.get(app_key)
            )
            products_arr.append(product)
            self.products_dict[app_key] = products_arr

        # Transform all sub-product DBs into a dictionary keyed by application number & product number
        self.marketing_status_dict = {}
        for key, value in parallel.ShardedDB.open(
            self.marketing_status_db_path
        ).range_iter(None, None):
            self.marketing_status_dict[key] = value

        self.te_dict = {}
        for key, value in parallel.ShardedDB.open(self.te_db_path).range_iter(
            None, None
        ):
            self.te_dict[key] = value

        # Transform submissions DB into a dictionary keyed by application number
        self.submissions_dict = {}
        for key, submission in parallel.ShardedDB.open(
            self.submissions_db_path
        ).range_iter(None, None):
            split = key.split("-")
            app_key = split[0]
            submissions_arr = (
                []
                if self.submissions_dict.get(app_key) is None
                else self.submissions_dict.get(app_key)
            )
            submissions_arr.append(submission)
            self.submissions_dict[app_key] = submissions_arr

        # Transform all sub-submission DBs into a dictionary keyed by application number & submission number
        self.submissions_property_type_dict = {}
        for key, value in parallel.ShardedDB.open(
            self.submissions_property_type_db_path
        ).range_iter(None, None):
            self.submissions_property_type_dict[key] = value

        self.applications_docs_dict = {}
        for key, value in parallel.ShardedDB.open(
            self.applications_docs_db_path
        ).range_iter(None, None):
            self.applications_docs_dict[key] = value

        parallel.Mapper.map_shard(self, map_input, map_output)

    def map(self, key, application, out):
        self.add_products(application)
        self.add_submissions(application)
        out.add(key, application)

    def add_products(self, application):
        key = re.sub("[^0-9]", "", application["application_number"])
        products = self.products_dict.get(key)
        if products:
            products = self.add_marketing_status(products, key)
            products = self.add_te(products, key)
            application["products"] = products

    def add_marketing_status(self, products, app_key):
        for product in products:
            key = build_products_key(app_key, product)
            if key in self.marketing_status_dict:
                marketing_json = self.marketing_status_dict.get(key)
                product["marketing_status"] = marketing_json[
                    "marketing_status"
                ].rstrip()
        return products

    def add_te(self, products, app_key):
        for product in products:
            key = build_products_key(app_key, product)
            if key in self.te_dict:
                te_json = self.te_dict.get(key)
                if te_json.get("te_code"):
                    product["te_code"] = te_json["te_code"].rstrip()
                if "marketing_status" not in product and "marketing_status" in te_json:
                    product["marketing_status"] = te_json["marketing_status"].rstrip()
        return products

    def add_submissions(self, application):
        key = re.sub("[^0-9]", "", application["application_number"])
        submissions = self.submissions_dict.get(key)
        if submissions:
            submissions = self.add_submissions_property_type(submissions, key)
            submissions = self.add_applications_docs(submissions, key)
            application["submissions"] = submissions

    def add_submissions_property_type(self, submissions, app_key):
        for submission in submissions:
            key = build_submissions_key(app_key, submission)
            if key in self.submissions_property_type_dict:
                prop_type = self.submissions_property_type_dict.get(key)
                submission["submission_property_type"] = prop_type
        return submissions

    def add_applications_docs(self, submissions, app_key):
        for submission in submissions:
            key = build_submissions_key(app_key, submission)
            if key in self.applications_docs_dict:
                submission["application_docs"] = self.applications_docs_dict.get(key)
        return submissions


class MergeAll(luigi.Task):
    def requires(self):
        return [
            Applications2JSON(),
            Products2JSON(),
            ApplicationsDocs2JSON(),
            Submissions2JSON(),
            SubmissionPropertyType2JSON(),
            MarketingStatus2JSON(),
            TE2JSON(),
        ]

    def output(self):
        return luigi.LocalTarget(MERGED_DB)

    def run(self):
        applications_db = self.input()[0].path
        products_db = self.input()[1].path
        applications_docs_db = self.input()[2].path
        submissions_db = self.input()[3].path
        submissions_property_type_db = self.input()[4].path
        marketing_status = self.input()[5].path
        te_db = self.input()[6].path

        parallel.mapreduce(
            parallel.Collection.from_sharded(applications_db),
            mapper=MergeAllMapper(
                applications_db,
                products_db,
                applications_docs_db,
                submissions_db,
                submissions_property_type_db,
                marketing_status,
                te_db,
            ),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
            map_workers=1,
            num_shards=1,
        )  # TODO: improve the code to avoid having to limit number of shards to one


class AnnotateDrugsFDA(luigi.Task):
    def requires(self):
        return [MergeAll(), CombineHarmonization()]

    def output(self):
        return luigi.LocalTarget(ANNOTATED_DB)

    def run(self):
        input_db = self.input()[0].path
        harmonized_file = self.input()[1].path

        parallel.mapreduce(
            parallel.Collection.from_sharded(input_db),
            mapper=AnnotateMapper(harmonized_file),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
            num_shards=1,
        )  # TODO: improve the code to avoid having to limit number of shards to one


class LoadJSON(index_util.LoadJSONBase):
    index_name = "drugsfda"
    mapping_file = "./schemas/drugsfda_mapping.json"
    data_source = AnnotateDrugsFDA()
    use_checksum = False
    optimize_index = True
    last_update_date = lambda _: first_file_timestamp(os.path.dirname(RAW_DATA_FILE))


if __name__ == "__main__":
    luigi.run()
