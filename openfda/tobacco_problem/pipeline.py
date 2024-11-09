#!/usr/local/bin/python

"""
Pipeline for converting CSV tobacco product problem data to JSON and importing into Elasticsearch.
"""

import logging
import os
import re
import glob
from os.path import join, dirname
from urllib.parse import urljoin
from urllib.request import urlopen
import arrow
import luigi
import pandas as pd
from bs4 import BeautifulSoup

from openfda import common, config, parallel, index_util
from openfda.common import newest_file_timestamp


TOBACCO_PROBLEM_DOWNLOAD_PAGE = "https://www.fda.gov/tobacco-products/tobacco-science-research/tobacco-product-problem-reports"
TOBACCO_PROBLEM_EXTRACT_DB = "tobacco_problem/tobacco_problem.db"
TOBACCO_RAW_DIR = config.data_dir("tobacco_problem/raw")


class DownloadTobaccoProblem(luigi.Task):
    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(
            config.data_dir("tobacco_problem/json/tobacco_problem_raw.json")
        )

    def run(self):
        logging.basicConfig(level=logging.INFO)
        output_dir = TOBACCO_RAW_DIR
        os.system("mkdir -p %s" % output_dir)

        # Download all csv source files (current year and archived years).
        soup = BeautifulSoup(urlopen(TOBACCO_PROBLEM_DOWNLOAD_PAGE).read(), "lxml")
        for a in soup.find_all(title=re.compile("\d{4}.*tppr", re.IGNORECASE)):
            file_name = a["title"] if ".csv" in a["title"] else (a["title"] + ".csv")
            common.download(
                urljoin("https://www.fda.gov", a["href"]), join(output_dir, file_name)
            )

        # Combine CSV files into a single json.
        all_csv_files = [i for i in glob.glob((output_dir + "/*.{}").format("csv"))]
        logging.info("Reading csv files: %s", (all_csv_files))
        os.system("mkdir -p %s" % dirname(self.output().path))
        df = pd.concat(
            pd.read_csv(f, encoding="cp1252", skiprows=3).rename(
                columns={"REPORT_ID (ICSR)": "REPORT_ID", "REPORT ID": "REPORT_ID"},
                errors="ignore",
            )
            for f in all_csv_files
        )
        df.to_json(self.output().path, orient="records")
        with open(self.output().path, "w") as f:
            for row in df.iterrows():
                row[1].to_json(f)
                f.write("\n")


class TobaccoProblem2JSONMapper(parallel.Mapper):
    rename_map = {
        "REPORT_ID": "report_id",
        "DATE_SUBMITTED": "date_submitted",
        "NUMBER_TOBACCO_PRODUCTS": "number_tobacco_products",
        "TOBACCO_PRODUCTS": "tobacco_products",
        "NUMBER_HEALTH_PROBLEMS": "number_health_problems",
        "REPORTED_HEALTH_PROBLEMS": "reported_health_problems",
        "NONUSER_AFFECTED": "nonuser_affected",
        "NUMBER_PRODUCT_PROBLEMS": "number_product_problems",
        "REPORTED_PRODUCT_PROBLEMS": "reported_product_problems",
    }

    def map(self, key, value, output):
        def _cleaner(k, v):
            """Helper function to rename keys and purge any keys that are not in
            the map.
            """
            if k in self.rename_map and v is not None:
                if "DATE" in k:
                    return self.rename_map[k], str(
                        arrow.get(v, ["M/D/YYYY", "D-MMM-YYYY", "D-MMM-YY"]).format(
                            "MM/DD/YYYY"
                        )
                    )
                # Make arrays of these three fields.
                if k in [
                    "TOBACCO_PRODUCTS",
                    "REPORTED_HEALTH_PROBLEMS",
                    "REPORTED_PRODUCT_PROBLEMS",
                ]:
                    return self.rename_map[k], list(set(v.split(" / ")))
                else:
                    return self.rename_map[k], v

        new_value = common.transform_dict(value, _cleaner)
        output.add(key, new_value)


class TobaccoProblem2JSON(luigi.Task):
    def requires(self):
        return DownloadTobaccoProblem()

    def output(self):
        return luigi.LocalTarget(config.data_dir(TOBACCO_PROBLEM_EXTRACT_DB))

    def run(self):
        parallel.mapreduce(
            parallel.Collection.from_glob(self.input().path, parallel.JSONLineInput()),
            mapper=TobaccoProblem2JSONMapper(),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
            num_shards=1,
        )


class LoadJSON(index_util.LoadJSONBase):
    index_name = "tobaccoproblem"
    mapping_file = "./schemas/tobaccoproblem_mapping.json"
    data_source = TobaccoProblem2JSON()
    use_checksum = False
    optimize_index = True
    last_update_date = lambda _: newest_file_timestamp(TOBACCO_RAW_DIR)


if __name__ == "__main__":
    luigi.run()
