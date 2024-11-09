#!/usr/bin/python

""" Device pipeline for downloading, transforming to JSON and loading COVID-19 Serological Testing Validation Project
 data into Elasticsearch.
"""

import glob
from os.path import dirname

import luigi
import arrow

from openfda import common, config, index_util, parallel
from openfda.tasks import AlwaysRunTask
from openfda.common import first_file_timestamp

SEROLOGY_TEST_BUCKET = "s3://openfda-covid19serology/"
SEROLOGY_TEST_SYNC_DIR = config.data_dir("covid19serology/s3_sync")
SEROLOGY_TEST_JSON_DB_DIR = config.data_dir("covid19serology/json.db")


class SyncS3SerologyTest(AlwaysRunTask):

    def _run(self):
        common.cmd(["mkdir", "-p", SEROLOGY_TEST_SYNC_DIR])
        common.cmd(
            [
                "aws",
                "--profile=" + config.aws_profile(),
                "s3",
                "sync",
                SEROLOGY_TEST_BUCKET,
                SEROLOGY_TEST_SYNC_DIR,
            ]
        )

    def output(self):
        return luigi.LocalTarget(SEROLOGY_TEST_SYNC_DIR)


class SerologyCSV2JSONMapper(parallel.Mapper):

    def map(self, key, value, output):

        # Date fields.
        DATE_KEYS = ["date_performed"]

        def _cleaner(k, v):
            """A helper function that is used formatting dates."""
            if v != None:
                if k in DATE_KEYS:
                    # Elasticsearch cannot handle null dates, emit None so the wrapper
                    # function `transform_dict` will omit it from its transformation
                    v = arrow.get(v).format("M/D/YYYY")
            return (k, v)

        new_value = common.transform_dict(value, _cleaner)
        output.add(key, new_value)


class SerologyCSV2JSON(luigi.Task):

    def requires(self):
        return SyncS3SerologyTest()

    def output(self):
        return luigi.LocalTarget(SEROLOGY_TEST_JSON_DB_DIR)

    def run(self):
        input_files = glob.glob(self.requires().output().path + "/*.csv")
        parallel.mapreduce(
            parallel.Collection.from_glob(input_files, parallel.CSVDictLineInput()),
            mapper=SerologyCSV2JSONMapper(),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
            num_shards=1,
        )

    def mapreduce_inputs(self):
        input_files = glob.glob(dirname(self.requires().output().path) + "/*.csv")
        return parallel.Collection.from_glob(input_files, parallel.CSVDictLineInput())


class LoadJSON(index_util.LoadJSONBase):
    index_name = "covid19serology"
    mapping_file = "./schemas/covid19serology_mapping.json"
    data_source = SerologyCSV2JSON()
    use_checksum = False
    optimize_index = True
    last_update_date = lambda _: first_file_timestamp(SEROLOGY_TEST_SYNC_DIR)


if __name__ == "__main__":
    luigi.run()
