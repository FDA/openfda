#!/usr/bin/python

""" Device classification pipeline for downloading, converting to JSON and
    loading into elasticsearch.
"""

import csv
import glob
import os
from os.path import dirname, join

import luigi

from openfda import common, config, index_util, parallel
from openfda import device_common, download_util
from openfda.common import first_file_timestamp
from openfda.device_harmonization.pipeline import (
    Harmonized2OpenFDA,
    DeviceAnnotateMapper,
)

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
# A directory for holding files that track Task state
RAW_DIR = config.data_dir("classification/raw")
META_DIR = config.data_dir("classification/meta")
common.shell_cmd("mkdir -p %s", META_DIR)


DEVICE_CLASS_ZIP = "https://www.accessdata.fda.gov/premarket/" "ftparea/foiclass.zip"


class DownloadFoiClass(luigi.Task):
    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(RAW_DIR)

    def run(self):
        output_filename = join(self.output().path, DEVICE_CLASS_ZIP.split("/")[-1])
        common.download(DEVICE_CLASS_ZIP, output_filename)


class ExtractAndCleanDownloadsClassification(luigi.Task):
    """Unzip each of the download files and remove all the non-UTF8 characters.
    Unzip -p streams the data directly to iconv which then writes to disk.
    """

    def requires(self):
        return DownloadFoiClass()

    def output(self):
        return luigi.LocalTarget(config.data_dir("classification/extracted"))

    def run(self):
        output_dir = self.output().path
        common.shell_cmd("mkdir -p %s", output_dir)
        input_dir = self.input().path
        download_util.extract_and_clean(input_dir, "ISO-8859-1", "UTF-8", "txt")


class ClassificationMapper(parallel.Mapper):
    def map(self, key, value, output):
        IGNORE = ["physicalstate", "technicalmethod", "targetarea"]

        # Changing names to match the openFDA naming standard
        # key = source name, value = replacement name
        RENAME_MAP = {
            "productcode": "product_code",
            "reviewcode": "review_code",
            "regulationnumber": "regulation_number",
            "devicename": "device_name",
            "medicalspecialty": "medical_specialty",
            "thirdpartyflag": "third_party_flag",
            "gmpexemptflag": "gmp_exempt_flag",
            "deviceclass": "device_class",
            "summarymalfunctionreporting": "summary_malfunction_reporting",
        }

        MEDICAL_SPECIALTY = device_common.MED_SPECIALTY_ADVISORY_COMMITTEE

        def _cleaner(k, v):
            """A helper function used for removing and renaming dictionary keys."""
            k = k.lower()
            if k in IGNORE:
                return None
            if k in RENAME_MAP:
                k = RENAME_MAP[k]
            if k == "medical_specialty":
                tk, tv = "medical_specialty_description", MEDICAL_SPECIALTY[v]
                return [(k, v), (tk, tv)]
            return (k, v)

        new_value = common.transform_dict(value, _cleaner)
        output.add(key, new_value)


class Classification2JSON(luigi.Task):
    def requires(self):
        return ExtractAndCleanDownloadsClassification()

    def output(self):
        return luigi.LocalTarget(config.data_dir("classification/json.db"))

    def run(self):
        common.shell_cmd("mkdir -p %s", dirname(self.output().path))
        input_files = glob.glob(self.input().path + "/*.txt")
        parallel.mapreduce(
            parallel.Collection.from_glob(
                input_files,
                parallel.CSVDictLineInput(
                    delimiter="|", quoting=csv.QUOTE_NONE, escapechar="\\"
                ),
            ),
            mapper=ClassificationMapper(),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
        )


class AnnotateDevice(luigi.Task):
    def requires(self):
        return [Harmonized2OpenFDA(), Classification2JSON()]

    def output(self):
        return luigi.LocalTarget(config.data_dir("classification/annotate.db"))

    def run(self):
        harmonized_db = parallel.ShardedDB.open(self.input()[0].path).as_dict()

        parallel.mapreduce(
            parallel.Collection.from_sharded(self.input()[1].path),
            mapper=DeviceAnnotateMapper(harmonized_db=harmonized_db),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
            num_shards=10,
        )


class LoadJSON(index_util.LoadJSONBase):
    index_name = "deviceclass"
    mapping_file = "./schemas/classification_mapping.json"
    data_source = AnnotateDevice()
    use_checksum = False
    optimize_index = True
    last_update_date = lambda _: first_file_timestamp(RAW_DIR)


if __name__ == "__main__":
    luigi.run()
