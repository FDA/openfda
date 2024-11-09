#!/usr/local/bin/python

"""
Pipeline for converting CSV nsde data to JSON and importing into Elasticsearch.
"""

import glob
import os
from os.path import join, dirname

import luigi

from openfda import common, config, parallel, index_util
from openfda.common import oldest_file_timestamp

NSDE_DOWNLOAD = (
    "https://download.open.fda.gov/Comprehensive_NDC_SPL_Data_Elements_File.zip"
)
NSDE_EXTRACT_DB = "nsde/nsde.db"
NSDE_RAW_DIR = config.data_dir("nsde/raw")


class DownloadNSDE(luigi.Task):

    def output(self):
        return luigi.LocalTarget(join(NSDE_RAW_DIR, "nsde.csv"))

    def run(self):
        output_dir = dirname(self.output().path)
        zip_filename = join(output_dir, "nsde.zip")
        common.download(NSDE_DOWNLOAD, zip_filename)
        os.system("unzip -o %(zip_filename)s -d %(output_dir)s" % locals())
        os.rename(glob.glob(join(output_dir, "*.csv"))[0], self.output().path)


class NSDE2JSONMapper(parallel.Mapper):
    rename_map = {
        "Item Code": "package_ndc",
        "NDC11": "package_ndc11",
        "Marketing Category": "marketing_category",
        "Marketing Start Date": "marketing_start_date",
        "Marketing End Date": "marketing_end_date",
        "Billing Unit": "billing_unit",
        "Proprietary Name": "proprietary_name",
        "Dosage Form": "dosage_form",
        "Application Number or Citation": "application_number_or_citation",
        "Product Type": "product_type",
        "Inactivation Date": "inactivation_date",
        "Reactivation Date": "reactivation_date",
    }

    def map(self, key, value, output):
        def _cleaner(k, v):
            """Helper function to rename keys and purge any keys that are not in
            the map.
            """

            if k in self.rename_map and v is not None and v != "":
                if "Date" in k:
                    return (self.rename_map[k], str(int(v)))
                if "Proprietary Name" in k:
                    return (self.rename_map[k], str(v).title())
                else:
                    return (self.rename_map[k], v)

        new_value = common.transform_dict(value, _cleaner)
        output.add(key, new_value)


class NSDE2JSON(luigi.Task):
    def requires(self):
        return DownloadNSDE()

    def output(self):
        return luigi.LocalTarget(config.data_dir(NSDE_EXTRACT_DB))

    def run(self):
        parallel.mapreduce(
            parallel.Collection.from_glob(
                self.input().path, parallel.CSVDictLineInput()
            ),
            mapper=NSDE2JSONMapper(),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
        )


class LoadJSON(index_util.LoadJSONBase):
    index_name = "othernsde"
    mapping_file = "./schemas/othernsde_mapping.json"
    data_source = NSDE2JSON()
    use_checksum = False
    optimize_index = True
    last_update_date = lambda _: oldest_file_timestamp(NSDE_RAW_DIR)


if __name__ == "__main__":
    luigi.run()
